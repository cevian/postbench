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
		runWatcher(config)
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
		execSql(pool, fmt.Sprintf("SELECT create_hypertable('metric_%d', 'time', chunk_time_interval=> interval '5 minutes', create_default_indexes=>false);", i))

		wg.Add(1)
		go func() {
			runInserter(config, pool, metric)
			wg.Done()
		}()
	}

	wg.Wait()
}

func runWatcher(config config) {
	t := time.NewTicker(config.reportPeriod)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		}
		config.ewma.Tick()
		rate := config.ewma.Rate()
		avg := config.ewma.AvgRate()

		fmt.Printf("Rate is %v, %v, %v\n", rate, int64(avg), float64(config.ewma.TotalEvents()))
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
