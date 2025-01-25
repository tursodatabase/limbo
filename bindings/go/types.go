package turso

import (
	"unsafe"
)

type ResultCode int

const (
	Error     ResultCode = -1
	Ok        ResultCode = 0
	Row       ResultCode = 1
	Busy      ResultCode = 2
	Io        ResultCode = 3
	Interrupt ResultCode = 4
	Invalid   ResultCode = 5
	Null      ResultCode = 6
	NoMem     ResultCode = 7
	ReadOnly  ResultCode = 8
	NoData    ResultCode = 9
	Done      ResultCode = 10
)

const (
	FfiDbOpen         string = "db_open"
	FfiDbClose        string = "db_close"
	FfiDbPrepare      string = "db_prepare"
	FfiStmtExec       string = "stmt_execute"
	FfiStmtQuery      string = "stmt_query"
	FfiRowsClose      string = "rows_close"
	FfiRowsGetColumns string = "rows_get_columns"
	FfiRowsNext       string = "rows_next"
	FfiRowsGetValue   string = "rows_get_value"
	FfiFreeColumns    string = "free_columns"
	FfiFreeCString    string = "free_string"
)

type valueType int

const (
	intVal valueType = iota
	textVal
	blobVal
	realVal
	nullVal
)

type tursoValue struct {
	Type  valueType
	Value [16]byte
}

type Blob struct {
	Data uintptr
	Len  uint
}

func toGoValue(valPtr uintptr) interface{} {
	val := (*tursoValue)(unsafe.Pointer(valPtr))
	switch val.Type {
	case intVal:
		return *(*int64)(unsafe.Pointer(&val.Value))
	case realVal:
		return *(*float64)(unsafe.Pointer(&val.Value))
	case textVal:
		textPtr := *(*uintptr)(unsafe.Pointer(&val.Value))
		return toGoStr(textPtr)
	case blobVal:
		blobPtr := *(*uintptr)(unsafe.Pointer(&val.Value))
		return toGoBlob(blobPtr)
	case nullVal:
		return nil
	default:
		return nil
	}
}

func toGoBlob(blobPtr uintptr) []byte {
	if blobPtr == 0 {
		return nil
	}
	blob := (*Blob)(unsafe.Pointer(blobPtr))
	return unsafe.Slice((*byte)(unsafe.Pointer(blob.Data)), blob.Len)
}

func toGoStr(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}
	return goStrFromC((*byte)(unsafe.Pointer(ptr)))
}

func toGoBytes(ptr uintptr) []byte {
	if ptr == 0 {
		return nil
	}
	uptr := unsafe.Pointer(ptr)
	b := (*[]byte)(uptr)
	return *b
}

var freeString func(*byte)

func freeCString(cstr uintptr) {
	if cstr == 0 {
		return
	}
	if freeString == nil {
		getFfiFunc(&freeString, FfiFreeCString)
	}
	freeString((*byte)(unsafe.Pointer(cstr)))
}

func freeStringPtr(cstr *byte) {
	if cstr == nil {
		return
	}
	if freeString == nil {
		getFfiFunc(&freeString, FfiFreeCString)
	}
	freeString(cstr)
}

func cArrayToGoStrings(arrayPtr uintptr, length uint) []string {
	var result []string
	ptrs := (*[1 << 30]*byte)(unsafe.Pointer(arrayPtr))[:length:length]
	for i := 0; i < int(length); i++ {
		cstr := ptrs[i]
		if cstr != nil {
			result = append(result, goStrFromC(cstr))
		} else {
			result = append(result, "")
		}
	}
	return result
}

func goStrFromC(cstr *byte) string {
	if cstr == nil {
		return ""
	}
	start := unsafe.Pointer(cstr)
	var length int
	for {
		if *(*byte)(unsafe.Add(start, length)) == 0 {
			break
		}
		length++
	}
	data := (*[1 << 30]byte)(start)[:length:length]
	clone := make([]byte, length)
	copy(clone, data)
	freeStringPtr(cstr)
	return string(clone)
}
