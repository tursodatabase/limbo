package turso

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
)

type tursoStmt struct {
	connCtx uintptr
	stmtPtr uintptr
	sql     string
	query   func(uintptr) uintptr
	execute func(uintptr, uintptr) uintptr
}

func (st *tursoStmt) Close() error {
	return nil
}

func (st *tursoStmt) NumInput() int {
	// TODO: parameter count
	return -1
}

func (st *tursoStmt) Exec(args []driver.Value) (driver.Result, error) {
	res := st.exec(args)
	switch ResultCode(res) {
	case Ok:
		return driver.RowsAffected(1), nil
	case Error:
		return nil, errors.New("error executing statement")
	case Busy:
		return nil, errors.New("busy")
	case Interrupt:
		return nil, errors.New("interrupted")
	case Invalid:
		return nil, errors.New("invalid statement")
	default:
		return nil, fmt.Errorf("unexpected status: %d", res)
	}
}

func (st *tursoStmt) Query(args []driver.Value) (driver.Rows, error) {
	if st.query == nil {
		getFfiFunc(&st.query, "stmt_query")
	}

	rowsPtr := st.query(st.stmtPtr)
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", st.sql)
	}

	return &tursoRows{
		connCtx: st.connCtx,
		rowsPtr: rowsPtr,
	}, nil
}

func (st *tursoStmt) exec(args []driver.Value) uintptr {
	if st.execute == nil {
		getFfiFunc(&st.execute, FfiStmtExec)
	}
	var argsPtr uintptr
	if len(args) > 0 {
		argsPtr = toCString(fmt.Sprintf("%v", args))
		defer freeCString(argsPtr)
	}
	return st.execute(st.stmtPtr, argsPtr)
}

type tursoRows struct {
	connCtx  uintptr
	rowsPtr  uintptr
	columns  []string
	closed   bool
	getCols  func(uintptr, *uint) uintptr
	next     func(uintptr) uintptr
	getValue func(uintptr, int32) uintptr
	close    func(uintptr) uintptr
	freeCols func(uintptr) uintptr
}

func (r *tursoRows) Columns() []string {
	if r.getCols == nil {
		getFfiFunc(&r.getCols, FfiRowsGetColumns)
	}
	if r.columns == nil {
		var columnCount uint
		colArrayPtr := r.getCols(r.rowsPtr, &columnCount)
		if colArrayPtr != 0 && columnCount > 0 {
			r.columns = cArrayToGoStrings(colArrayPtr, columnCount)
			if r.freeCols == nil {
				getFfiFunc(&r.freeCols, FfiFreeColumns)
			}
			defer r.freeCols(colArrayPtr)
		}
	}
	return r.columns
}

func (r *tursoRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.close == nil {
		getFfiFunc(&r.close, FfiRowsClose)
	}
	r.close(r.rowsPtr)
	r.rowsPtr = 0
	return nil
}

func (r *tursoRows) Next(dest []driver.Value) error {
	if r.next == nil {
		getFfiFunc(&r.next, FfiRowsNext)
	}
	if r.getValue == nil {
		getFfiFunc(&r.getValue, FfiRowsGetValue)
	}
	status := r.next(r.rowsPtr)
	switch ResultCode(status) {
	case Row:
		for i := range dest {
			valPtr := r.getValue(r.connCtx, int32(i))
			val := toGoValue(valPtr)
			dest[i] = val
		}
		return nil
	case Done:
		return io.EOF
	default:
		return fmt.Errorf("unexpected status: %d", status)
	}
}
