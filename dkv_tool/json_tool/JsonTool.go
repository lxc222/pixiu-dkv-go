package json_tool

import (
	"encoding/json"
	"pixiu-dkv-go/dkv_tool/str_tool"
)

func ToJsonStr(a any) (string, error) {
	bytes, err := json.Marshal(a)
	if err != nil {
		return "", err
	} else {
		return str_tool.Bytes2Str(bytes), nil
	}
}

func ToJsonStr2(a any) string {
	str, err := ToJsonStr(a)
	if err != nil {
		return ""
	} else {
		return str
	}
}

func FromJsonStr(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
