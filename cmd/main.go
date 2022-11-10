package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cevian/postbench/pkg/ewma"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/peterbourgon/ff/v3"
)

type config struct {
	metrics             int
	metricConcurrency   int
	series              int
	seriesZipfParameter float64
	batches             int
	pgURI               string
	reportPeriod        time.Duration
	scrapeDuration      time.Duration
	ewma                *ewma.Rate
}

func main() {
	var config config

	fs := flag.NewFlagSet("postbench", flag.ExitOnError)
	fs.IntVar(&config.metrics, "metrics", 10, "Number of metrics")
	fs.IntVar(&config.metricConcurrency, "metricConcurrency", 2, "Number of concurrent metrics")
	fs.IntVar(&config.series, "series", 100000, "Number of series")
	fs.Float64Var(&config.seriesZipfParameter, "series-zipf", 0, "Zipf parameter (e.g. 1.1)")
	fs.IntVar(&config.batches, "batches", 10000, "Number of items in a batch")
	fs.DurationVar(&config.reportPeriod, "reporting period", time.Second*10, "report period")
	fs.DurationVar(&config.scrapeDuration, "scrape-duration", time.Second*0, "scrapeDuration ")
	fs.StringVar(&config.pgURI, "pg-uri", "postgres://localhost/test", "Postgres URI")

	ff.Parse(fs, os.Args[1:])
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

	copiers := 5
	metricsSlice := make([][]int, copiers)
	numSeriesSlice := make([][]int, copiers)
	ts := time.Now().Add(-time.Hour)

	var seriesNumberGen *rand.Zipf
	if config.seriesZipfParameter > 0 {
		seriesNumberGen = rand.NewZipf(rand.New(rand.NewSource(99)), config.seriesZipfParameter, 1, uint64(config.series-1))
	}
	fmt.Println("Creating metrics")
	for i := 0; i < config.metrics; i++ {
		metric := i
		execSql(pool, fmt.Sprintf(`
		DO $DO$
		BEGIN
			EXECUTE $$DROP TABLE IF EXISTS metric_%[1]d$$;
			EXECUTE $$TRUNCATE metric_%[1]d$$;
			EXECUTE $$CREATE UNIQUE INDEX IF NOT EXISTS metric_%[1]d_idx ON metric_%[1]d (series_id, time) INCLUDE (value)$$;
			EXECUTE $$SELECT create_hypertable('metric_%[1]d', 'time', chunk_time_interval=> (interval '1 minute' * (1.0+((random()*0.01)-0.005))), create_default_indexes=>false);$$;
		END$DO$;
		`, i))
		//execSql(pool, fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_%d(time timestamptz NOT NULL, value double precision not null, series_id bigint not null) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)", i))
		//execSql(pool, fmt.Sprintf("TRUNCATE metric_%d", i))
		//execSql(pool, fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS metric_%d_idx ON metric_%d (series_id, time) INCLUDE (value)", i, i))
		//execSql(pool, fmt.Sprintf("CREATE INDEX IF NOT EXISTS metric_%d_idx ON metric_%d (series_id) INCLUDE (time, value)", i, i))
		//execSql(pool, fmt.Sprintf("CREATE INDEX IF NOT EXISTS metric_%d_idx ON metric_%d USING hash (series_id)", i, i))
		//execSql(pool, fmt.Sprintf("SELECT create_hypertable('metric_%d', 'time', chunk_time_interval=> (interval '1 minute' * (1.0+((random()*0.01)-0.005))), create_default_indexes=>false);", i))

		index := i % len(metricsSlice)
		metricsSlice[index] = append(metricsSlice[index], metric)
		numSeries := config.series
		if config.seriesZipfParameter > 0 && i > 10 {
			numSeries = int(seriesNumberGen.Uint64()) + 1
		}
		numSeriesSlice[index] = append(numSeriesSlice[index], numSeries)

		/*for j := 0; j < config.metricConcurrency; j++ {
			//set to 0 for conflicts
			secondsOffset := time.Second * time.Duration(j)
			wg.Add(1)
			go func() {
				numSeries := config.series
				if config.seriesZipfParameter > 0 && i > 10 {
					numSeries = int(seriesNumberGen.Uint64()) + 1
				}
				runInserterCopy(config, pool, metric, ts.Add(secondsOffset), numSeries)
				//runInserter(config, pool, metric)
				wg.Done()
			}()
		}*/
	}

	wg.Add(1)
	config.ewma = ewma.NewEWMARate(1, config.reportPeriod)
	go func() {
		runWatcher(config, pool)
		wg.Done()
	}()

	for i := range metricsSlice {
		index := i
		wg.Add(1)
		go func() {
			runMultiMetricInserterCopy(config, pool, metricsSlice[index], ts, numSeriesSlice[index])
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

type txn struct {
	txnid int64
	mxid  int64
}

func (t *txn) setZero(zero *txn) {
	t.txnid -= zero.txnid
	t.mxid -= zero.mxid
}

func (t *txn) scan(pool *pgxpool.Pool) {
	err := pool.QueryRow(context.Background(), "SELECT age('10'::xid), mxid_age('10'::xid)").
		Scan(&t.txnid, &t.mxid)
	if err != nil {
		panic(err)
	}
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
		t.records = 0
		t.bytes = 0
		//panic(err)
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
		//panic(err)
	}
}

func runWatcher(config config, pool *pgxpool.Pool) {
	t := time.NewTicker(config.reportPeriod)
	defer t.Stop()

	zeroBuffers := buffers{}
	zeroBuffers.scan(pool)
	lastBuffers := zeroBuffers
	zeroWal := wal{}
	zeroWal.scan(pool)
	lastWal := zeroWal

	zeroTxn := txn{}
	zeroTxn.scan(pool)

	start := time.Now()
	lastTime := start
	lastSamples := int64(0)

	for {
		select {
		case <-t.C:
		}
		config.ewma.Tick()
		rate := config.ewma.Rate()
		avg := config.ewma.AvgRate()
		samples := config.ewma.TotalEvents()
		iterSamples := samples - lastSamples

		currentBuffers := buffers{}
		currentBuffers.scan(pool)
		sumBuffers := currentBuffers
		sumBuffers.setZero(&zeroBuffers)
		iterBuffers := currentBuffers
		iterBuffers.setZero(&lastBuffers)

		currentWal := wal{}
		currentWal.scan(pool)
		sumWal := currentWal
		sumWal.setZero(&zeroWal)
		iterWal := currentWal
		iterWal.setZero(&lastWal)

		currentTxn := txn{}
		currentTxn.scan(pool)
		currentTxn.setZero(&zeroTxn)

		currentTime := time.Now()
		durS := currentTime.Sub(start).Seconds()
		iterS := currentTime.Sub(lastTime).Seconds()
		chunkSize := &chunkSize{}
		chunkSize.scan(pool)

		fmt.Printf("Rate is %.2e, %.2e, %.2e, txn=%4.0f %4.0f buffers=%4.0f %4.0f %4.0f %4.0f/sample, wal=%4.0f[%4.0f]/sample %4.0fMB/s, total=%4.0f[%4.0f]MB/s %4.0f[%4.0f]/sample, chunk size(MB): %4.0f %4.0f \n",
			rate,
			avg,
			float64(samples),
			float64(currentTxn.txnid)/durS,
			float64(currentTxn.mxid)/durS,
			/* checkpoints report all buffers only at end of checkpoint so have to use avgs here*/
			float64(sumBuffers.checkpoint*8192)/float64(samples),
			float64(sumBuffers.clean*8192)/float64(samples),
			float64(sumBuffers.backend*8192)/float64(samples),
			float64(sumBuffers.bytes())/float64(samples),
			float64(iterWal.bytes)/float64(iterSamples),
			float64(sumWal.bytes)/float64(samples),
			float64(iterWal.bytes)/(1024*1024*iterS),
			float64((iterBuffers.bytes()+iterWal.bytes)/(1024*1024))/iterS,
			float64((sumBuffers.bytes()+sumWal.bytes)/(1024*1024))/durS,
			float64((iterBuffers.bytes()+iterWal.bytes))/float64(iterSamples),
			float64((sumBuffers.bytes()+sumWal.bytes))/float64(samples),
			float64(chunkSize.index/(1024*1024)),
			float64(chunkSize.total/(1024*1024)),
		)
		lastWal = currentWal
		lastBuffers = currentBuffers
		lastTime = currentTime
		lastSamples = samples
	}

}

func assertNoErr(err error) {
	if err != nil {
		panic(err)
	}
}

func CheckNetworkSizes(pool *pgxpool.Pool, timeSamples []time.Time, valSamples []float64, seriesIdSamples []int64) {
	size := 0
	compressedSize := 0
	conn2, err := pool.Acquire(context.Background())
	assertNoErr(err)
	tsa := pgtype.TimestampArray{}
	err = tsa.Set(timeSamples)
	assertNoErr(err)
	res, err := tsa.EncodeBinary(conn2.Conn().ConnInfo(), nil)
	assertNoErr(err)
	size += len(res)
	buf := bytes.Buffer{}
	zw := gzip.NewWriter(&buf)
	_, err = zw.Write(res)
	assertNoErr(err)
	err = zw.Close()
	assertNoErr(err)
	compressedSize += buf.Len()

	vala := pgtype.Float8Array{}
	err = vala.Set(valSamples)
	assertNoErr(err)
	res, err = vala.EncodeBinary(conn2.Conn().ConnInfo(), nil)
	assertNoErr(err)
	size += len(res)
	buf = bytes.Buffer{}
	zw = gzip.NewWriter(&buf)
	_, err = zw.Write(res)
	assertNoErr(err)
	err = zw.Close()
	assertNoErr(err)
	compressedSize += buf.Len()

	sa := pgtype.Int8Array{}
	err = sa.Set(seriesIdSamples)
	assertNoErr(err)
	res, err = sa.EncodeBinary(conn2.Conn().ConnInfo(), nil)
	assertNoErr(err)
	size += len(res)
	buf = bytes.Buffer{}
	zw = gzip.NewWriter(&buf)
	_, err = zw.Write(res)
	assertNoErr(err)
	err = zw.Close()
	assertNoErr(err)
	compressedSize += buf.Len()

	fmt.Println("size", size, "size/sample", size/len(timeSamples), "compressed", compressedSize, "compressed/sample", compressedSize/len(timeSamples))
	conn2.Release()
}

func runInserter(config config, pool *pgxpool.Pool, metric int) {
	ts := time.Now().Add(-time.Hour)
	conn := pool
	var err error
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

			//CheckNetworkSizes(pool, timeSamples, valSamples, seriesIdSamples)
			/*insertSQL := fmt.Sprintf(
				`INSERT INTO metric_%d(time, value, series_id)
			SELECT * FROM unnest($1::timestamptz[], $2::double precision[], $3::bigint[]) a(t,v,s) ORDER BY s,t`, /* ON CONFLICT DO NOTHING metric)
			_, err := pool.Exec(context.Background(), insertSQL, timeSamples, valSamples, seriesIdSamples)*/
			insertSQL := `SELECT insert_metric_row($1::name, $2::timestamptz[], $3::double precision[], $4::bigint[])`
			_, err = conn.Exec(context.Background(), insertSQL, fmt.Sprintf("metric_%d", metric), timeSamples, valSamples, seriesIdSamples)
			if err != nil {
				panic(err)
			}
			config.ewma.Incr(int64(len(timeSamples)))
		}
	}

}

func runInserterCopy(config config, pool *pgxpool.Pool, metric int, ts time.Time, numSeries int) {
	startScrape := time.Time{}
	for {
		if !startScrape.IsZero() && config.scrapeDuration > 0 {
			sinceLastScape := time.Since(startScrape)
			if sinceLastScape < config.scrapeDuration {
				time.Sleep(config.scrapeDuration - sinceLastScape)
			}
		}
		startScrape = time.Now()
		ts = ts.Add(time.Second * 10)
		seriesID := 1
		for seriesID < numSeries {
			data := make([][]interface{}, 0, config.batches)
			for item := 0; item < config.batches && seriesID < numSeries; item++ {
				row := []interface{}{ts, rand.Float64(), int64(seriesID)}
				data = append(data, row)
				seriesID++
			}
			//rand.Shuffle(len(seriesIdSamples), func(i, j int) {
			//	seriesIdSamples[i], seriesIdSamples[j] = seriesIdSamples[j], seriesIdSamples[i]
			//})

			table := fmt.Sprintf("metric_%d", metric)
			ctx := context.Background()
			columns := []string{"time", "value", "series_id"}

			if false /*with onconflict*/ {
				tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
				if err != nil {
					panic(err)
				}
				defer func() {
					if err != nil && tx != nil {
						if err := tx.Rollback(ctx); err != nil {
							panic(err)
						}
					}
				}()

				if _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE temp ON COMMIT DROP AS TABLE %s WITH NO DATA", table)); err != nil {
					panic(err)
				}

				if _, err = tx.CopyFrom(ctx, pgx.Identifier{"temp"}, columns, pgx.CopyFromRows(data)); err != nil {
					panic(err)
				}

				if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(%[2]s) SELECT %[2]s FROM temp ON CONFLICT DO NOTHING", table, strings.Join(columns, ","))); err != nil {
					panic(err)
				}
				tx.Commit(ctx)
			} else {
				_, err := pool.CopyFrom(
					context.Background(),
					pgx.Identifier{table},
					columns,
					pgx.CopyFromRows(data),
				)
				if err != nil {
					panic(err)
				}
			}
			config.ewma.Incr(int64(len(data)))
		}
	}

}

func runMultiMetricInserterCopy(config config, pool *pgxpool.Pool, metrics []int, ts time.Time, numSeriesSlice []int) {
	startScrape := time.Time{}
	for {
		if !startScrape.IsZero() && config.scrapeDuration > 0 {
			sinceLastScape := time.Since(startScrape)
			if sinceLastScape < config.scrapeDuration {
				time.Sleep(config.scrapeDuration - sinceLastScape)
			}
		}
		startScrape = time.Now()
		ts = ts.Add(time.Second * 10)
		for metric_index, metric := range metrics {
			numSeries := numSeriesSlice[metric_index]
			seriesID := 1
			for seriesID < numSeries {
				data := make([][]interface{}, 0, config.batches)
				for item := 0; item < config.batches && seriesID < numSeries; item++ {
					row := []interface{}{ts, rand.Float64(), int64(seriesID)}
					data = append(data, row)
					seriesID++
				}
				//rand.Shuffle(len(seriesIdSamples), func(i, j int) {
				//	seriesIdSamples[i], seriesIdSamples[j] = seriesIdSamples[j], seriesIdSamples[i]
				//})

				table := fmt.Sprintf("metric_%d", metric)
				ctx := context.Background()
				columns := []string{"time", "value", "series_id"}

				if false /*with onconflict*/ {
					tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
					if err != nil {
						panic(err)
					}
					defer func() {
						if err != nil && tx != nil {
							if err := tx.Rollback(ctx); err != nil {
								panic(err)
							}
						}
					}()

					if _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE temp ON COMMIT DROP AS TABLE %s WITH NO DATA", table)); err != nil {
						panic(err)
					}

					if _, err = tx.CopyFrom(ctx, pgx.Identifier{"temp"}, columns, pgx.CopyFromRows(data)); err != nil {
						panic(err)
					}

					if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(%[2]s) SELECT %[2]s FROM temp ON CONFLICT DO NOTHING", table, strings.Join(columns, ","))); err != nil {
						panic(err)
					}
					tx.Commit(ctx)
				} else {
					_, err := pool.CopyFrom(
						context.Background(),
						pgx.Identifier{table},
						columns,
						pgx.CopyFromRows(data),
					)
					if err != nil {
						panic(err)
					}
				}
				config.ewma.Incr(int64(len(data)))
			}
		}
	}
}
