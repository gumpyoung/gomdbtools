package mdbtools

// #cgo pkg-config: libmdb
// #include <mdbtools.h>
// MdbCatalogEntry* _go_ptr_catalog(GPtrArray *array, guint index) {
// return (MdbCatalogEntry *)g_ptr_array_index(array, index);
// }
// MdbColumn* _go_ptr_col(GPtrArray *array, guint index) {
// return (MdbColumn *)g_ptr_array_index(array, index);
// }
// MdbIndex* _go_ptr_idx(GPtrArray *array, guint index) {
// return (MdbIndex *)g_ptr_array_index(array, index);
// }
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

type MDB struct {
	dbfile string
	mdb    *C.MdbHandle
	mu     sync.Mutex
}

type MDBTableDef struct {
	table         *C.MdbTableDef
	num_cols      int
	bound_values  [][]byte
	bound_lengths []int
	column_names  []string
}

type MDBColumn struct {
	name    string
	Type    int
	size    int
	prec    int
	scale   int
	isFixed bool
}

type MDBIndex struct {
	num         int
	name        string
	Type        int
	rows        int
	keys        int
	flags       int
	keyColNum   []int
	keyColOrder []int
}

func NewMDB(filename string) (*MDB, error) {
	dbfile := C.CString(filename)
	defer C.free(unsafe.Pointer(dbfile))
	var mdbh *C.MdbHandle = C.mdb_open(dbfile, C.MDB_NOFLAGS)

	if mdbh == nil {
		return nil, fmt.Errorf("mdb: error opening %s", filename)
	}

	mdb := &MDB{
		dbfile: filename,
		mdb:    mdbh,
	}
	runtime.SetFinalizer(mdb, (*MDB).Close)

	return mdb, nil
}

func (db *MDB) Close() error {
	var err error
	C.mdb_close(db.mdb)
	return err
}

func (db *MDB) Version() int {
	return int(db.mdb.f.jet_version)
}

func (db *MDB) Tables() ([]string, error) {
	var entry *C.MdbCatalogEntry
	var tables []string
	// tables := make([]string, int(db.mdb.num_catalog))
	// append(tables, table)

	rv := C.mdb_read_catalog(db.mdb, C.MDB_TABLE)
	if rv == nil {
		return nil, errors.New("mdb: error reading table catalog")
	}

	for i := 0; i < int(db.mdb.num_catalog); i++ {
		entry = C._go_ptr_catalog(db.mdb.catalog, C.guint(i))
		name := C.GoString(&entry.object_name[0])
		if strings.HasPrefix(name, "MSys") {
			continue
		}
		tables = append(tables, name)

	}

	return tables, nil
}

func (db *MDB) TableOpen(tableName string) (*MDBTableDef, error) {
	var entry *C.MdbCatalogEntry
	var col *C.MdbColumn
	tableDef := &MDBTableDef{}

	rv := C.mdb_read_catalog(db.mdb, C.MDB_TABLE)
	if rv == nil {
		return nil, errors.New("mdb: error reading table catalog")
	}

	for i := 0; i < int(db.mdb.num_catalog); i++ {
		entry = C._go_ptr_catalog(db.mdb.catalog, C.guint(i))
		name := C.GoString(&entry.object_name[0])

		if name == tableName {
			tableDef.table = C.mdb_read_table(entry)
			if tableDef.table == nil {
				return nil, errors.New("mdb: error reading table")
			}
			tableDef.num_cols = int(tableDef.table.num_cols)

			C.mdb_read_columns(tableDef.table)
			C.mdb_read_indices(tableDef.table)
			C.mdb_rewind_table(tableDef.table)

			tableDef.column_names = make([]string, tableDef.num_cols)
			tableDef.bound_values = make([][]byte, tableDef.num_cols)
			tableDef.bound_lengths = make([]int, tableDef.num_cols)

			for j := 0; j < tableDef.num_cols; j++ {
				col = C._go_ptr_col(tableDef.table.columns, C.guint(j))
				colName := C.GoString(&col.name[0])
				tableDef.column_names[j] = colName

				tableDef.bound_values[j] = make([]byte, C.MDB_BIND_SIZE)
				C.mdb_bind_column(tableDef.table, C.int(j+1), unsafe.Pointer(&tableDef.bound_values[j][0]), (*C.int)(unsafe.Pointer(&tableDef.bound_lengths[j])))

			}
		}
	}

	return tableDef, nil
}

func (db *MDB) FetchRow(tableDef *MDBTableDef) ([]string, error) {
	row := make([]string, tableDef.num_cols)

	rv := C.mdb_fetch_row(tableDef.table)
	if rv == 0 {
		return nil, nil
	}

	for i := 0; i < tableDef.num_cols; i++ {
		row[i] = string(tableDef.bound_values[i][0:tableDef.bound_lengths[i]])
	}

	return row, nil
}

func (db *MDB) FetchAssoc(tableDef *MDBTableDef) (map[string]string, error) {
	row := make(map[string]string)

	rv := C.mdb_fetch_row(tableDef.table)
	if rv == 0 {
		return nil, nil
	}

	for i := 0; i < tableDef.num_cols; i++ {
		row[tableDef.column_names[i]] = string(tableDef.bound_values[i][0:tableDef.bound_lengths[i]])
	}

	return row, nil
}

// mdb_fetch_row rewinds table on last row
func (db *MDB) Rewind(tableDef *MDBTableDef) int {
	return int(C.mdb_rewind_table(tableDef.table))
}

func (db *MDB) TableClose(tableDef *MDBTableDef) {
	// nothing to do here
	C.free(unsafe.Pointer(tableDef.table))
}

func (db *MDB) NumFields(tableDef *MDBTableDef) (int, error) {
	return int(tableDef.table.num_cols), nil
}

func (db *MDB) NumRows(tableDef *MDBTableDef) (int, error) {
	return int(tableDef.table.num_rows), nil
}

func (db *MDB) TableFields(tableDef *MDBTableDef) (map[string]MDBColumn, error) {
	var col *C.MdbColumn
	columns := make(map[string]MDBColumn)

	for j := 0; j < tableDef.num_cols; j++ {
		col = C._go_ptr_col(tableDef.table.columns, C.guint(j))

		columns[C.GoString(&col.name[0])] = MDBColumn{
			C.GoString(&col.name[0]),
			int(col.col_type),
			int(col.col_size),
			int(col.col_prec),
			int(col.col_scale),
			bool(int(col.is_fixed) == 1),
		}
	}

	return columns, nil
	// return int(tableDef.table.num_rows), nil
}

func (db *MDB) TableIndexes(tableDef *MDBTableDef) ([]MDBIndex, error) {
	var idx *C.MdbIndex
	indexes := make([]MDBIndex, tableDef.table.num_idxs)

	for i := 0; i < int(tableDef.table.num_idxs); i++ {
		idx = C._go_ptr_idx(tableDef.table.indices, C.guint(i))

		mdbindex := MDBIndex{
			int(idx.index_num),
			C.GoString(&idx.name[0]),
			int(idx.index_type),
			int(idx.num_rows),
			int(idx.num_keys),
			int(idx.flags),
			make([]int, int(idx.num_keys)),
			make([]int, int(idx.num_keys)),
		}

		for j := 0; j < mdbindex.keys; j++ {
			mdbindex.keyColNum[j] = int(idx.key_col_num[j])
			mdbindex.keyColOrder[j] = int(idx.key_col_order[j])
		}

		indexes[i] = mdbindex
	}

	return indexes, nil
}

func (db *MDB) TypeName(typeCode int) string {
	return ""
}
