package sqlite

import (
	"fmt"
	"testing"
)

func TestScheme(t *testing.T) {
	//数据表的映射，  key 字段名，  value 字段类型
	//1 string 2 int
	var scheme = map[string]int{
		"Username": 1,
		"Age":      2,
	}

	//给结构体 添加字段,根据字段类型
	strc := NewSchemeBuilder()

	for k, v := range scheme {
		if v == 1 {
			strc.AddString(k)
		} else if v == 2 {
			strc.AddInt64(k)
		}
	}

	//构建结构体
	build := strc.Build().New()

	fmt.Printf("%+v\n", &build.instance)
}
