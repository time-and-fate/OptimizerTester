package cost

import (
	"fmt"
	"math"

	"github.com/qw4990/OptimizerTester/tidb"
)

// Scan: scanFactor, netFactor																				(CPU, CopCPU, Net, Scan, DescScan, Mem)
//   select /*+ use_index(t, primary) */ a from t where a>=? and a<=?										(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b from t where b>=? and b<=?												(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b, d from t where b>=? and b<=?											(estRow*(1+ log2(Min(estRow, lookupBatchSize))), 0, estRow*log(tblRowSize)+estRow*log(idxRowSize), estRow*tblRowSize+estRow*idxRowSize, 0, 0)
// WideScan: scanFactor, netFactor
//   select /*+ use_index(t, primary) */ a, c from t where a>=? and a<=?									(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, bc) */ b, c from t where b>=? and b<=?											(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b, c from t where b>=? and b<=?											(estRow*(1+ log2(Min(estRow, lookupBatchSize))), 0, estRow*log(tblRowSize)+estRow*log(idxRowSize), estRow*tblRowSize+estRow*idxRowSize, 0, 0)
// DescScan: descScanFactor, netFactor
//   select /*+ use_index(t, primary), no_reorder() */ a from t where a>=? and a<=? order by a desc			(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
//   select /*+ use_index(t, b), no_reorder() */ b from t where b>=? and b<=? order by b desc				(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
// AGG: CPUFactor, copCPUFactor
//   select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=? and b<=?			(0, estRow, 0, estRow*log(rowSize), 0, 0)
//   select /*+ use_index(t, b), stream_agg(), agg_not_to_cop */ count(1) from t where b>=? and b<=?		(estRow, 0, estRow*rowSize, estRow*log(rowSize), 0, 0)
// Sort: CPUFactor, MemFactor
//   select /*+ use_index(t, b), must_reorder() */ b from t where b>=? and b<=? order by b					(estRow*log(estRow), 0, estRow*rowSize, estRow*log(rowSize), 0, estRow)

func genSyntheticCalibrationQueries(ins tidb.Instance, db string) CaliQueries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	n := 2
	var ret CaliQueries
	ret = append(ret, genSyntheticCaliScanQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliWideScanQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliDescScanQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliAGGQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliSortQueries(ins, n)...)
	return ret
}

var syntheticScanRowSize, syntheticNetRowSize map[string]float64

func init() {
	syntheticScanRowSize = map[string]float64{
		"tbl-scan(a)":          20,
		"idx-scan(b)":          29,
		"lookup-idx(b,d)":      38,
		"lookup-tbl(b,d)":      179,
		"wide-tbl-scan(a,c)":   172,
		"wide-idx-scan(b,c)":   161,
		"wide-lookup-idx(b,c)": 39,
		"wide-lookup-tbl(b,c)": 179,
		"desc-tbl-scan(a)":     20,
		"desc-idx-scan(b)":     29,
	}

	syntheticNetRowSize = map[string]float64{
		"tbl-scan(a)":          8.125,
		"idx-scan(b)":          8.125,
		"lookup-idx(b,d)":      16.25,
		"lookup-tbl(b,d)":      16.25,
		"wide-tbl-scan(a,c)":   140.22,
		"wide-idx-scan(b,c)":   140.22,
		"wide-lookup-idx(b,c)": 16.25,
		"wide-lookup-tbl(b,c)": 140.22,
		"desc-tbl-scan(a)":     8.125,
		"desc-idx-scan(b)":     8.125,
	}
}

func getSyntheticRowSize(key, forWhat string, modelVariant int) float64 {
	if forWhat == "net" {
		if v, ok := syntheticNetRowSize[key]; ok {
			return v
		}
		panic(key)
	}
	if forWhat == "scan" {
		if v, ok := syntheticScanRowSize[key]; ok {
			if modelVariant == 1 {
				v = math.Log2(v)
			}
			return v
		}
		panic(key)
	}
	panic(forWhat)
}

func genSyntheticCaliScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("tbl-scan(a)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("tbl-scan(a)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary) */ a from t where a>=%v and a<=%v", l, r),
			Label:   "TableScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b from t where b>=%v and b<=%v", l, r),
			Label:   "IndexScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * (getSyntheticRowSize("lookup-idx(b,d)", "scan", 1) + getSyntheticRowSize("lookup-tbl(b,d)", "scan", 1))
		netW := float64(rowCount) * (getSyntheticRowSize("lookup-idx(b,d)", "net", 1) + getSyntheticRowSize("lookup-tbl(b,d)", "net", 1))
		cpuW := float64(rowCount) * (1.0 + math.Log2(math.Min(float64(rowCount), float64(20000))))
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b, d from t where b>=%v and b<=%v", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliWideScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("wide-tbl-scan(a,c)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("wide-tbl-scan(a,c)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary) */ a, c from t where a>=%v and a<=%v", l, r),
			Label:   "Wide-TableScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("wide-idx-scan(b,c)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("wide-idx-scan(b,c)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, bc) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:   "Wide-IndexScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * (getSyntheticRowSize("wide-lookup-idx(b,c)", "scan", 1) + getSyntheticRowSize("wide-lookup-tbl(b,c)", "scan", 1))
		netW := float64(rowCount) * (getSyntheticRowSize("wide-lookup-idx(b,c)", "net", 1) + getSyntheticRowSize("wide-lookup-tbl(b,c)", "net", 1))
		cpuW := float64(rowCount) * (1.0 + math.Log2(math.Min(float64(rowCount), float64(20000))))
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:   "Wide-IndexLookup",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliDescScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// table scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and b<=%v", l, r))
		descScanW := float64(rowCount) * getSyntheticRowSize("desc-tbl-scan(a)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("desc-tbl-scan(a)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary), no_reorder() */ a from t where a>=%v and a<=%v order by a desc", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{0, 0, netW, 0, descScanW, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		descScanW := float64(rowCount) * getSyntheticRowSize("desc-idx-scan(b)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("desc-idx-scan(b)", "net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b), no_reorder() */ b from t where b>=%v and b<=%v order by b desc", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{0, 0, netW, 0, descScanW, 0},
		})
	}
	return qs
}

func genSyntheticCaliAGGQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minB, maxB int
	mustReadOneLine(ins, `select  min(b), max(b) from t`, &minB, &maxB)

	// pushed down: copCPU
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "scan", 1)
		copCPUW := float64(rowCount)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=%v and b<=%v", l, r),
			Label:   "Agg-PushedDown",
			Weights: CostWeights{0, copCPUW, 0, scanW, 0, 0},
		})
	}

	// not pushed down: CPU
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "net", 1)
		cpuW := float64(rowCount)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b), stream_agg(), agg_not_to_cop() */ count(1) from t where b>=%v and b<=%v", l, r),
			Label:   "Agg-NotPushedDown",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, 0},
		})
	}
	return qs
}

func genSyntheticCaliSortQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minB, maxB int
	mustReadOneLine(ins, `select  min(b), max(b) from t`, &minB, &maxB)

	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		cpuW := float64(rowCount) * math.Log2(float64(rowCount))
		scanW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("idx-scan(b)", "net", 1)
		memW := float64(rowCount)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b), must_reorder() */ b from t where b>=%v and b<=%v order by b", l, r),
			Label:   "Sort",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, memW},
		})
	}
	return qs
}
