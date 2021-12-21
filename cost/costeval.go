package cost

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"strings"
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

	//genSyntheticData(ins, 200000, "synthetic")

	//qs := genSyntheticQueries(ins, "synthetic")
	//for _, q := range qs {
	//	fmt.Println(q)
	//}

	records := runCostEvalQueries(ins, "synthetic", []string{"select /*+ use_index(t, b) */ * from synthetic.t where b>=1 and b<=100000"})
	
	fmt.Println(records)
}

type record struct {
	cost   float64
	timeMS float64
}

func runCostEvalQueries(ins tidb.Instance, db string, qs []string) []record {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	ins.MustExec(`set @@tidb_cost_calibration_mode=2`)
	ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
	ins.MustExec(`set @@tidb_executor_concurrency=1`)
	ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
	records := make([]record, 0, len(qs))
	
	//mysql> explain analyze select /*+ use_index(t, b) */ * from synthetic.t where b>=1 and b<=100000;
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| id                            | estRows   | estCost     | actRows | task      | access object       | execution info                                                                                                                                                                                                                                                 | operator info                      | memory  | disk |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| IndexLookUp_7                 | 100109.36 | 11149394.98 | 99986   | root      |                     | time:252.5ms, loops:99, index_task: {total_time: 134.2ms, fetch_handle: 98.2ms, build: 7.86µs, wait: 36ms}, table_task: {total_time: 666.2ms, num: 9, concurrency: 5}                                                                                          |                                    | 37.7 MB | N/A  |
	//	| ├─IndexRangeScan_5(Build)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t, index:b(b) | time:93.2ms, loops:102, cop_task: {num: 1, max: 89.6ms, proc_keys: 0, tot_proc: 89ms, rpc_num: 1, rpc_time: 89.6ms, copr_cache_hit_ratio: 0.00}, tikv_task:{time:59.4ms, loops:99986}                                                                          | range:[1,100000], keep order:false | N/A     | N/A  |
	//	| └─TableRowIDScan_6(Probe)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t             | time:592.1ms, loops:109, cop_task: {num: 9, max: 89.2ms, min: 10.4ms, avg: 54.1ms, p95: 89.2ms, tot_proc: 456ms, rpc_num: 9, rpc_time: 486.3ms, copr_cache_hit_ratio: 0.00}, tikv_task:{proc max:15ms, min:2.57ms, p80:10.9ms, p95:15ms, iters:99986, tasks:9} | keep order:false                   | N/A     | N/A  |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	for _, q := range qs {
		rs := ins.MustQuery("explain analyze " + q)
		var id, task, access, execInfo, opInfo, mem, disk, rootExecInfo string
		var estRows, actRows, cost, rootCost float64

		for rs.Next() {
			if err := rs.Scan(&id, &estRows, &cost, &actRows, &task, &access, &execInfo, &opInfo, &mem, &disk); err != nil {
				panic(err)
			}
			if actRows != estRows {
				panic(fmt.Sprintf(`not true-CE for query=%v, est=%v, act=%v`, q, estRows, actRows))
			}
			if rootExecInfo == "" {
				rootExecInfo, rootCost = execInfo, cost
			}
		}
		if err := rs.Close(); err != nil {
			panic(err)
		}

		records = append(records, record{
			cost:   rootCost,
			timeMS: parseTimeFromExecInfo(rootExecInfo),
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
