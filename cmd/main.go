package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cevian/postbench/pkg/ewma"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/peterbourgon/ff/v3"
)

type config struct {
	metrics      int
	series       int
	batches      int
	pgURI        string
	reportPeriod time.Duration
	ewma         *ewma.Rate
}

func main() {
	var config config

	fs := flag.NewFlagSet("postbench", flag.ExitOnError)
	fs.IntVar(&config.metrics, "metrics", 10, "Number of metrics")
	fs.IntVar(&config.series, "series", 100000, "Number of series")
	fs.IntVar(&config.batches, "batches", 10000, "Number of items in a batch")
	fs.DurationVar(&config.reportPeriod, "reporting period", time.Second*10, "report period")
	fs.StringVar(&config.pgURI, "pg-uri", "postgres://localhost/test", "Postgres URI")

	ff.Parse(fs, os.Args[1:])
	config.ewma = ewma.NewEWMARate(1, config.reportPeriod)
	fmt.Printf("config %#v\n", config)
	run(config)
}

func execSql(pool *pgxpool.Pool, sql string, params ...interface{}) {
	_, err := pool.Exec(context.Background(), sql, params...)
	if err != nil {
		panic(err)
	}
}

func run(config config) {
	pgxConn, err := pgxpool.ParseConfig(config.pgURI)
	if err != nil {
		panic(err)
	}
	fmt.Printf("pgc config %#v\n", pgxConn)
	fmt.Printf("pgc config %#v\n", pgxConn.ConnConfig)

	pool, err := pgxpool.ConnectConfig(context.Background(), pgxConn)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		runWatcher(config, pool)
		wg.Done()
	}()

	execSql(pool, "CREATE EXTENSION IF NOT EXISTS timescaledb")
	execSql(pool, `
	CREATE OR REPLACE FUNCTION insert_metric_row(
		metric_table name,
		time_array timestamptz[],
		value_array DOUBLE PRECISION[],
		series_id_array bigint[]
	) RETURNS BIGINT AS
	$$
	DECLARE
	  num_rows BIGINT;
	BEGIN    
		EXECUTE FORMAT(
		 'INSERT INTO  %1$I (time, value, series_id)
			  SELECT * FROM unnest($1, $2, $3) a(t,v,s) ORDER BY s,t',
			metric_table
		) USING time_array, value_array, series_id_array;
		GET DIAGNOSTICS num_rows = ROW_COUNT;
		RETURN num_rows;
	EXCEPTION WHEN unique_violation THEN 
		EXECUTE FORMAT(
		'INSERT INTO  %1$I (time, value, series_id)
			 SELECT * FROM unnest($1, $2, $3) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING',
		   metric_table
	    ) USING time_array, value_array, series_id_array;
	    GET DIAGNOSTICS num_rows = ROW_COUNT;
		RETURN num_rows;
	END;
	$$
	LANGUAGE PLPGSQL`)

	for i := 0; i < config.metrics; i++ {
		metric := i
		execSql(pool, fmt.Sprintf("DROP TABLE IF EXISTS metric_%d", i))
		execSql(pool, fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_%d(time timestamptz NOT NULL, value double precision not null, series_id bigint not null) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)", i))
		execSql(pool, fmt.Sprintf("TRUNCATE metric_%d", i))
		execSql(pool, fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS metric_%d_idx ON metric_%d (series_id, time) INCLUDE (value)", i, i))
		//execSql(pool, fmt.Sprintf("CREATE INDEX IF NOT EXISTS metric_%d_idx ON metric_%d (series_id) INCLUDE (time, value)", i, i))
		execSql(pool, fmt.Sprintf("SELECT create_hypertable('metric_%d', 'time', chunk_time_interval=> (interval '1 minute' * (1.0+((random()*0.01)-0.005))), create_default_indexes=>false);", i))

		wg.Add(1)
		go func() {
			runInserter(config, pool, metric)
			wg.Done()
		}()
	}

	wg.Wait()
}

type buffers struct {
	checkpoint int64
	clean      int64
	backend    int64
}

func (t *buffers) setZero(zero *buffers) {
	t.checkpoint -= zero.checkpoint
	t.clean -= zero.clean
	t.backend -= zero.backend
}

func (t *buffers) scan(pool *pgxpool.Pool) {
	err := pool.QueryRow(context.Background(), "SELECT  buffers_checkpoint, buffers_clean, buffers_backend FROM pg_stat_bgwriter bg").
		Scan(&t.checkpoint, &t.clean, &t.backend)
	if err != nil {
		panic(err)
	}
}

func (t *buffers) bytes() int64 {
	return (t.checkpoint + t.clean + t.backend) * 8192
}

type wal struct {
	records int64
	bytes   int64
}

func (t *wal) setZero(zero *wal) {
	t.records -= zero.records
	t.bytes -= zero.bytes
}

func (t *wal) scan(pool *pgxpool.Pool) {
	err := pool.QueryRow(context.Background(), "SELECT wal_records, wal_bytes FROM pg_stat_wal bg").
		Scan(&t.records, &t.bytes)
	if err != nil {
		panic(err)
	}
}

type chunkSize struct {
	index int64
	total int64
}

func (t *chunkSize) scan(pool *pgxpool.Pool) {
	err := pool.QueryRow(context.Background(),
		`SELECT sum(chunk_index_size), sum(chunk_total_size) 
		 FROM (
		   select distinct ON (hypertable_name) 
		     hypertable_name, 
		     chunk_name, 
		     pg_indexes_size(('_timescaledb_internal.'||chunk_name)::regclass) chunk_index_size, 
		     pg_total_relation_size(('_timescaledb_internal.'||chunk_name)::regclass) chunk_total_size 
		   from timescaledb_information.chunks 
		   ORDER BY hypertable_name, range_end desc
		) as info`).
		Scan(&t.index, &t.total)
	if err != nil {
		panic(err)
	}
}

func runWatcher(config config, pool *pgxpool.Pool) {
	t := time.NewTicker(config.reportPeriod)
	defer t.Stop()

	zeroBuffers := &buffers{}
	zeroBuffers.scan(pool)
	zeroWal := &wal{}
	zeroWal.scan(pool)
	start := time.Now()

	for {
		select {
		case <-t.C:
		}
		config.ewma.Tick()
		rate := config.ewma.Rate()
		avg := config.ewma.AvgRate()
		samples := config.ewma.TotalEvents()

		currentBuffers := &buffers{}
		currentBuffers.scan(pool)
		currentBuffers.setZero(zeroBuffers)
		//.fmt.Printf("%#v %#v %v\n", zeroBuffers, currentBuffers, currentBuffers.bytes())

		currentWal := &wal{}
		currentWal.scan(pool)
		currentWal.setZero(zeroWal)

		durS := time.Since(start).Seconds()
		chunkSize := &chunkSize{}
		chunkSize.scan(pool)

		fmt.Printf("Rate is %.2e, %.2e, %.2e, buffers=%4.0f %4.0f %4.0f %4.0f/sample, wal=%4.0f/sample, total=%.2e(MB) %4.0f(MB/s) %4.0f/sample, chunk size(MB): %8.2f %8.2f \n",
			rate,
			avg,
			float64(samples),
			float64(currentBuffers.checkpoint*8192)/float64(samples),
			float64(currentBuffers.clean*8192)/float64(samples),
			float64(currentBuffers.backend*8192)/float64(samples),
			float64(currentBuffers.bytes())/float64(samples),
			float64(currentWal.bytes)/float64(samples),
			float64((currentBuffers.bytes()+currentWal.bytes)/(1024*1024)),
			float64((currentBuffers.bytes()+currentWal.bytes)/(1024*1024))/durS,
			float64((currentBuffers.bytes()+currentWal.bytes))/float64(samples),
			float64(chunkSize.index/(1024*1024)),
			float64(chunkSize.total/(1024*1024)),
		)
	}

}

func runInserter(config config, pool *pgxpool.Pool, metric int) {
	ts := time.Now().Add(-time.Hour)
	conn := pool
	/*conn, err := pool.Acquire(context.Background())
	if err != nil {
		panic(err)
	}*/
	for {
		ts = ts.Add(time.Second * 10)
		seriesID := 1
		for seriesID < config.series {
			timeSamples := make([]time.Time, 0, config.batches)
			valSamples := make([]float64, 0, config.batches)
			seriesIdSamples := make([]int64, 0, config.batches)
			for item := 0; item < config.batches; item++ {
				timeSamples = append(timeSamples, ts)
				valSamples = append(valSamples, rand.Float64())
				seriesIdSamples = append(seriesIdSamples, int64(seriesID))
				seriesID++
			}
			rand.Shuffle(len(seriesIdSamples), func(i, j int) {
				seriesIdSamples[i], seriesIdSamples[j] = seriesIdSamples[j], seriesIdSamples[i]
			})
			/*insertSQL := fmt.Sprintf(
				`INSERT INTO metric_%d(time, value, series_id)
			SELECT * FROM unnest($1::timestamptz[], $2::double precision[], $3::bigint[]) a(t,v,s) ORDER BY s,t`, /* ON CONFLICT DO NOTHING metric)
			_, err := pool.Exec(context.Background(), insertSQL, timeSamples, valSamples, seriesIdSamples)*/
			insertSQL := `SELECT insert_metric_row($1::name, $2::timestamptz[], $3::double precision[], $4::bigint[])`
			_, err := conn.Exec(context.Background(), insertSQL, fmt.Sprintf("metric_%d", metric), timeSamples, valSamples, seriesIdSamples)
			if err != nil {
				panic(err)
			}
			config.ewma.Incr(int64(len(timeSamples)))
		}
	}

}

func runInserterCopy(config config, pool *pgxpool.Pool, metric int) {
	ts := time.Now().Add(-time.Hour)
	for {
		ts = ts.Add(time.Second * 10)
		seriesID := 1
		for seriesID < config.series {
			data := make([][]interface{}, 0, config.batches)
			for item := 0; item < config.batches; item++ {
				row := []interface{}{ts, rand.Float64(), int64(seriesID)}
				data = append(data, row)
				seriesID++
			}
			//rand.Shuffle(len(seriesIdSamples), func(i, j int) {
			//	seriesIdSamples[i], seriesIdSamples[j] = seriesIdSamples[j], seriesIdSamples[i]
			//})

			_, err := pool.CopyFrom(
				context.Background(),
				pgx.Identifier{fmt.Sprintf("metric_%d", metric)},
				[]string{"time", "value", "series_id"},
				pgx.CopyFromRows(data),
			)
			if err != nil {
				panic(err)
			}
			config.ewma.Incr(int64(len(data)))
		}
	}

}
