package cetest

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"strconv"
	"sync"
	"time"
)

type predicateFromTableQuerier struct {
	db        string
	CERecords []CERecord
	initOnce  sync.Once
}

type CERecord struct {
	tableName string
	expr      string
	rowCount  uint64
}

func newPredicateFromTableQuerier(db string) *predicateFromTableQuerier {
	querier := predicateFromTableQuerier{
		db:        db,
		CERecords: make([]CERecord, 0, 50),
	}
	return &querier
}

func (querier *predicateFromTableQuerier) init(ins tidb.Instance) (rerr error) {
	querier.initOnce.Do(func() {
		begin := time.Now()
		q := fmt.Sprintf("set sql_mode='';SELECT type, db_name, table_name, expr, value FROM mysql.optimizer_trace group by type, db_name, table_name, expr;")
		rows, err := ins.Query(q)
		if err != nil {
			rerr = err
			return
		}
		for rows.Next() {
			var tp, db, table, expr, value string
			if rerr = rows.Scan(&tp, &db, &table, &expr, &value); rerr != nil {
				rows.Close()
				return
			}
			rowCount, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				fmt.Printf("[predicateFromTableQuerier-Init] Error when converting string to uint: " + err.Error())
			}
			querier.CERecords = append(querier.CERecords, CERecord{
				tableName: table,
				expr:      expr,
				rowCount:  rowCount,
			})

		}
		if rerr = rows.Close(); rerr != nil {
			return
		}
		fmt.Printf("[predicateFromTableQuerier-Init] sql=%v, cost=%v\n", q, time.Since(begin))
	})
	return
}

func (querier *predicateFromTableQuerier) Collect(qt QueryType, ers []EstResult, ins tidb.Instance) ([]EstResult, error) {
	if err := querier.init(ins); err != nil {
		return nil, err
	}

	concurrency := 1
	var wg sync.WaitGroup
	var resultLock sync.Mutex
	end := len(querier.CERecords)

	begin := time.Now()
	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := id; i < end; i += concurrency {
				record := querier.CERecords[i]
				q := fmt.Sprintf("SELECT count(*) FROM %v.`%v` WHERE %v", querier.db, record.tableName, record.expr)
				rows, err := ins.Query(q)
				if err != nil {
					fmt.Printf("[predicateFromTableQuerier-Process] Error when querying: " + err.Error() + ". SQL: " + q)
				}
				var result string
				rows.Next()
				if err = rows.Scan(&result); err != nil {
					rows.Close()
					return
				}
				trueCard, err := strconv.ParseUint(result, 10, 64)
				if err != nil {
					fmt.Printf("[predicateFromTableQuerier-Process] Error when converting string to uint: " + err.Error())
				}

				resultLock.Lock()
				ers = append(ers, EstResult{q, float64(record.rowCount), float64(trueCard)})
				resultLock.Unlock()
			}
			fmt.Printf("[predicateFromTableQuerier-Process] ins=%v, qt=%v, concurrency=%v, time-cost=%v\n",
				ins.Opt().Label, qt, concurrency, time.Since(begin))
		}(workerID)
	}

	wg.Wait()
	return ers, nil
}
