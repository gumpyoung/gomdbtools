package mdbtools

// #cgo pkg-config: libmdbsql
// #include <mdbtools.h>
// #include <mdbsql.h>
// MdbSQLColumn* _go_ptr_sqlcol(GPtrArray *array, guint index) {
// return (MdbSQLColumn *)g_ptr_array_index(array, index);
// }
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

type MDBSQL struct {
	dbfile string
	sql    *C.MdbSQL
	mu     sync.Mutex
}

func NewMDBSQL(filename string) (*MDBSQL, error) {
	dbfile := C.CString(filename)
	defer C.free(unsafe.Pointer(dbfile))
	var sql *C.MdbSQL = C.mdb_sql_init()
	C.mdb_sql_open(sql, dbfile)

	if sql == nil {
		return nil, fmt.Errorf("mdbsql: error opening %s", filename)
	}

	mdb := &MDBSQL{
		dbfile: filename,
		sql:    sql,
	}
	runtime.SetFinalizer(mdb, (*MDBSQL).Close)

	return mdb, nil
}

func (sql *MDBSQL) HasError() bool {
	if sql.sql.error_msg[0] > 0 {
		return true
	} else {
		return false
	}
}

func (sql *MDBSQL) GetError() string {
	return C.GoString(&sql.sql.error_msg[0])
}

func (sql *MDBSQL) RunQuery(query string) ([]map[string]string, error) {
	var col *C.MdbSQLColumn
	querybuf := C.CString(query)
	defer C.free(unsafe.Pointer(querybuf))

	C.mdb_sql_run_query(sql.sql, (*C.gchar)(querybuf))
	if sql.HasError() {
		return nil, fmt.Errorf("mdbsql: query error: %s", sql.GetError())
	}

	ret := make([]map[string]string, 0)

	rowCount := 0
	for C.mdb_fetch_row(sql.sql.cur_table) == 1 {
		rowCount++
		row := make(map[string]string)
		for i := 0; i < int(sql.sql.num_columns); i++ {
			col = C._go_ptr_sqlcol(sql.sql.columns, C.guint(i))
			row[C.GoString(col.name)] = C.GoString((*C.char)(sql.sql.bound_values[i]))
		}
		ret = append(ret, row)
	}

	C.mdb_sql_reset(sql.sql)

	return ret, nil
}

func (sql *MDBSQL) Close() error {
	var err error
	C.mdb_sql_exit(sql.sql)
	return err
}
