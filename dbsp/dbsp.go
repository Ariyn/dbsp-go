package dbsp

import (
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/state"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// Engine 는 내부 state store 를 관리하며 SQL 기반 IVM 쿼리를 실행한다.
type Engine struct {
	store *state.Store
}

// NewEngine 는 새로운 엔진 인스턴스를 생성한다.
func NewEngine() *Engine {
	return &Engine{store: state.NewStore()}
}

// QueryHandle 은 하나의 SELECT 쿼리에 대응하는 증분 DBSP 그래프 핸들이다.
type QueryHandle struct {
	node *op.Node
}

// Prepare 는 SELECT SQL 쿼리를 받아 증분 DBSP 그래프를 준비한다.
func (e *Engine) Prepare(query string) (*QueryHandle, error) {
	node, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		return nil, err
	}
	return &QueryHandle{node: node}, nil
}

// ApplyDML 은 INSERT/UPDATE/DELETE SQL 을 적용하고, 결과 델타를 반환한다.
// UPDATE/DELETE 의 경우 내부 state.Store 를 사용해 이전 값을 복원한다.
func (e *Engine) ApplyDML(table string, dml string, h *QueryHandle) ([]map[string]any, error) {
	if h == nil || h.node == nil {
		return nil, nil
	}

	var (
		batch types.Batch
		out   types.Batch
		err   error
	)

	// UPDATE / DELETE 에서 필요하므로 store 기반 파서를 사용한다.
	if e.store == nil {
		e.store = state.NewStore()
	}

	batch, err = sqlconv.ParseDMLToBatchWithStore(dml, e.store)
	if err != nil {
		return nil, err
	}

	// 테이블별 state 를 갱신한다.
	e.store.ApplyBatch(table, batch)

	sources := op.SourceNames(h.node)
	if len(sources) > 0 {
		seen := false
		for _, s := range sources {
			if s == table {
				seen = true
				break
			}
		}
		if !seen {
			// This DML does not affect the query inputs, so no output delta.
			return nil, nil
		}
	}
	if len(sources) > 1 {
		out, err = op.ExecuteTick(h.node, map[string]types.Batch{table: batch})
	} else {
		out, err = op.Execute(h.node, batch)
	}
	if err != nil {
		return nil, err
	}

	// 외부에는 간단한 map 리스트로 전달한다.
	var results []map[string]any
	for _, td := range out {
		if td.Count == 0 {
			continue
		}
		results = append(results, map[string]any(td.Tuple))
	}
	return results, nil
}
