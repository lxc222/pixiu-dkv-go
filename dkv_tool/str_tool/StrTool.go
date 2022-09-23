package str_tool

import (
	"strconv"
)

func Substring(source string, start int, end int) string {
	var r = []rune(source)
	length := len(r)

	if start < 0 || end > length || start > end {
		return ""
	}

	if start == 0 && end == length {
		return source
	}

	return string(r[start:end])
}

func Str2Bytes(str string) []byte {
	if StrIsEmpty(str) {
		return nil
	}

	return []byte(str)
}

func Str2Int(str string, defaultV int) int {
	if StrIsEmpty(str) {
		return defaultV
	}

	var intV, err = strconv.Atoi(str)
	if err != nil {
		return defaultV
	} else {
		return intV
	}
}

func Str2Int32(str string, defaultV int32) int32 {
	if StrIsEmpty(str) {
		return defaultV
	}

	var intV, err = strconv.ParseInt(str, 10, 32)
	if err != nil {
		return defaultV
	} else {
		return int32(intV)
	}
}

func Str2Int64(str string, defaultV int64) int64 {
	if StrIsEmpty(str) {
		return defaultV
	}

	var intV, err = strconv.ParseInt(str, 10, 64)
	if err != nil {
		return defaultV
	} else {
		return intV
	}
}

func StrIsEmpty(str string) bool {
	if len(str) <= 0 {
		return true
	} else {
		return false
	}
}

func Bytes2Str(bytes []byte) string {
	if ByteIsEmpty(bytes) {
		return ""
	}

	return string(bytes)
}

func ByteIsEmpty(value []byte) bool {
	if value == nil || len(value) <= 0 {
		return true
	} else {
		return false
	}
}

func Int2Str(num int) string {
	return strconv.Itoa(num)
}

func Int32Min(a int32, b int32) int32 {
	if a >= b {
		return b
	} else {
		return a
	}
}

func Int642Str(num int64) string {
	return strconv.FormatInt(num, 10)
}
