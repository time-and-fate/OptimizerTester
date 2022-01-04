package cost

import (
	"encoding/json"
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// CostEval ...
func CostEval() {
	opt := tidb.Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}
	opt.Addr = "127.0.0.1"

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	//genSyntheticData(ins, 100000, "synthetic")
	evalOnDataset(ins, "synthetic", genSyntheticQueries)
	//evalOnDataset(ins, "imdb", genIMDBQueries)
}

func evalOnDataset(ins tidb.Instance, db string, queryGenFunc func(ins tidb.Instance, db string) Queries) {
	fmt.Println("[cost-eval] start to eval on ", db)
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-queries.json", db))
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-records.json", db))

	qs, err := readQueriesFrom(queryFile)
	if err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = queryGenFunc(ins, db)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), db)
		saveQueriesTo(qs, queryFile)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	all, err := readRecordsFrom(recordFile)
	if err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)

		concurrency := 1
		instances := make([]tidb.Instance, concurrency)
		for i := 0; i < concurrency; i++ {
			tmp, err := tidb.ConnectTo(ins.Opt())
			if err != nil {
				panic(err)
			}
			instances[i] = tmp
		}

		var wg sync.WaitGroup
		queries := splitQueries(qs, concurrency)
		rs := make([]Records, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer fmt.Printf("[cost-eval] worker-%v finish\n", id)
				rs[id] = runCostEvalQueries(id, instances[id], db, queries[id])
			}(i)
		}
		wg.Wait()

		for _, r := range rs {
			all = append(all, r...)
		}
		saveRecordsTo(all, recordFile)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].TimeMS < all[j].TimeMS
	})

	tmp := make(Records, 0, len(all))
	for _, r := range all {
		if r.Label == "Point" {
			continue
		}
		//if r.Cost < 4e8 || r.Cost > 7e8 || r.TimeMS > 3500 {
		//	continue
		//}
		fmt.Println(">>>> ", r.SQL, r.Cost, r.TimeMS)
		//if r.Cost < 1000 { // the cost of PointGet is always zero
		//	continue
		//}
		tmp = append(tmp, r)
	}

	drawCostRecordsTo(tmp, fmt.Sprintf("%v-scatter.png", db))
}

type Query struct {
	SQL   string
	Label string
}

type Queries []Query

type Record struct {
	Cost   float64
	TimeMS float64
	Label  string
	SQL    string
}

type Records []Record

func runCostEvalQueries(id int, ins tidb.Instance, db string, qs Queries) Records {
	beginAt := time.Now()
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	ins.MustExec(`set @@tidb_cost_calibration_mode=2`)
	ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
	ins.MustExec(`set @@tidb_executor_concurrency=1`)
	ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
	
	ins.MustExec(`set @@tidb_opt_cpu_factor=230`)
	ins.MustExec(`set @@tidb_opt_copcpu_factor=0`)
	ins.MustExec(`set @@tidb_opt_network_factor=6`)
	ins.MustExec(`set @@tidb_opt_scan_factor=7`)
	ins.MustExec(`set @@tidb_opt_desc_factor=0`)
	ins.MustExec(`set @@tidb_opt_memory_factor=0`)
	records := make([]Record, 0, len(qs))

	//mysql> explain analyze select /*+ use_index(t, b) */ * from synthetic.t where b>=1 and b<=100000;
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| id                            | estRows   | estCost     | actRows | task      | access object       | execution info                                                                                                                                                                                                                                                 | operator info                      | memory  | disk |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| IndexLookUp_7                 | 100109.36 | 11149394.98 | 99986   | root      |                     | time:252.5ms, loops:99, index_task: {total_time: 134.2ms, fetch_handle: 98.2ms, build: 7.86µs, wait: 36ms}, table_task: {total_time: 666.2ms, num: 9, concurrency: 5}                                                                                          |                                    | 37.7 MB | N/A  |
	//	| ├─IndexRangeScan_5(Build)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t, index:b(b) | time:93.2ms, loops:102, cop_task: {num: 1, max: 89.6ms, proc_keys: 0, tot_proc: 89ms, rpc_num: 1, rpc_time: 89.6ms, copr_cache_hit_ratio: 0.00}, tikv_task:{time:59.4ms, loops:99986}                                                                          | range:[1,100000], keep order:false | N/A     | N/A  |
	//	| └─TableRowIDScan_6(Probe)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t             | time:592.1ms, loops:109, cop_task: {num: 9, max: 89.2ms, min: 10.4ms, avg: 54.1ms, p95: 89.2ms, tot_proc: 456ms, rpc_num: 9, rpc_time: 486.3ms, copr_cache_hit_ratio: 0.00}, tikv_task:{proc max:15ms, min:2.57ms, p80:10.9ms, p95:15ms, iters:99986, tasks:9} | keep order:false                   | N/A     | N/A  |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	for i, q := range qs {
		fmt.Printf("[cost-eval] worker-%v run query %v %v/%v %v\n", id, q, i, len(qs), time.Since(beginAt))

		rs := ins.MustQuery("explain analyze " + q.SQL)
		var id, task, access, execInfo, opInfo, mem, disk, rootExecInfo string
		var estRows, actRows, cost, rootCost float64
		planLabel := "Unmatched"

		for rs.Next() {
			if err := rs.Scan(&id, &estRows, &cost, &actRows, &task, &access, &execInfo, &opInfo, &mem, &disk); err != nil {
				panic(err)
			}
			if actRows != estRows {
				//fmt.Printf("[cost-eval] worker-%v not true-CE for query=%v, est=%v, act=%v\n", id, q, estRows, actRows)
				panic(fmt.Sprintf(`not true-CE for query=%v, est=%v, act=%v`, q, estRows, actRows))
			}
			if rootExecInfo == "" {
				rootExecInfo, rootCost = execInfo, cost
			}
			if planLabel == "Unmatched" {
				for _, operator := range []string{"Point", "Batch", "IndexReader", "IndexLookup", "TableReader", "Sort"} {
					if strings.Contains(strings.ToLower(id), strings.ToLower(operator)) {
						planLabel = operator
					}
				}
			}
		}
		if err := rs.Close(); err != nil {
			panic(err)
		}

		if q.Label != "" {
			planLabel = q.Label
		}
		records = append(records, Record{
			Cost:   rootCost,
			TimeMS: parseTimeFromExecInfo(rootExecInfo),
			Label:  planLabel,
			SQL:    q.SQL,
		})
	}

	return records
}

func parseTimeFromExecInfo(execInfo string) (timeMS float64) {
	// time:252.5ms, loops:99, index_task: {total_time: 13
	timeField := strings.Split(execInfo, ",")[0]
	timeField = strings.Split(timeField, ":")[1]
	dur, err := time.ParseDuration(timeField)
	if err != nil {
		panic(fmt.Sprintf("invalid time %v", timeField))
	}
	return float64(dur) / float64(time.Millisecond)
}

func splitQueries(r Queries, n int) []Queries {
	rs := make([]Queries, n)
	for i, record := range r {
		rs[i%n] = append(rs[i%n], record)
	}
	return rs
}

func saveQueriesTo(q Queries, f string) {
	data, err := json.Marshal(q)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(f, data, 0666); err != nil {
		panic(err)
	}
}
