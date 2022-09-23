package redis_request

import (
	"fmt"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"strconv"
	"strings"
	"unicode/utf8"
)

func ParseParamSize(msg string) int {
	if len(msg) <= 0 {
		return -99
	}

	var paramSize = -99
	//fmt.Printf("parseParamSize line: %s, %d\n", msg, utf8.RuneCountInString(msg))
	if strings.HasPrefix(msg, "*") && strings.HasSuffix(msg, "\r\n") {
		paramSize, _ = strconv.Atoi(str_tool.Substring(msg, 1, len(msg)-2))
	} else {
		fmt.Printf("parseParamSize[err] line: %s, %d\n", msg, utf8.RuneCountInString(msg))
	}

	return paramSize
}

func ParseParamStrSize(msg string) int {
	var paramSize = -99
	//fmt.Printf("parseParamStrSize line: %s, %d\n", msg, utf8.RuneCountInString(msg))
	if strings.HasPrefix(msg, "$") && strings.HasSuffix(msg, "\r\n") {
		paramSize, _ = strconv.Atoi(str_tool.Substring(msg, 1, len(msg)-2))
	} else {
		fmt.Printf("parseParamStrSize[err] line: %s, %d\n", msg, utf8.RuneCountInString(msg))
	}

	return paramSize
}

func ParseParamStr(msg string) string {
	//fmt.Printf("parseParamStr line: %s, %d\n", msg, utf8.RuneCountInString(msg))
	if strings.HasSuffix(msg, "\r\n") {
		return msg[:len(msg)-2]
	} else {
		fmt.Printf("parseParamStr[err] line: %s, %d\n", msg, utf8.RuneCountInString(msg))
		return ""
	}
}
