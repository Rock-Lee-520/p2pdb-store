package sqlite

import (
	"errors"
	"reflect"
)

//
type Builder struct {
	//⽤于存储属性字段
	fileId []reflect.StructField
}

func NewSchemeBuilder() *Builder {
	return &Builder{}
}

//添加字段
func (b *Builder) AddField(field string, typ reflect.Type) *Builder {
	b.fileId = append(b.fileId, reflect.StructField{Name: field, Type: typ})
	return b
}

//根据预先添加的字段构建出结构体
func (b *Builder) Build() *Struct {
	stu := reflect.StructOf(b.fileId)
	index := make(map[string]int)
	for i := 0; i < stu.NumField(); i++ {
		index[stu.Field(i).Name] = i
	}
	return &Struct{stu, index}
}
func (b *Builder) AddString(name string) *Builder {
	return b.AddField(name, reflect.TypeOf(""))
}
func (b *Builder) AddBool(name string) *Builder {
	return b.AddField(name, reflect.TypeOf(true))
}
func (b *Builder) AddInt64(name string) *Builder {
	return b.AddField(name, reflect.TypeOf(int64(0)))
}
func (b *Builder) AddFloat64(name string) *Builder {
	return b.AddField(name, reflect.TypeOf(float64(1.2)))
}

//实际⽣成的结构体，基类

//结构体的类型
type Struct struct {
	typ   reflect.Type
	index map[string]int
}

func (s Struct) New() *Instance {
	return &Instance{reflect.New(s.typ).Elem(), s.index}
}

//结构体的值
type Instance struct {
	instance reflect.Value
	index    map[string]int
}

var (
	FieldNoExist error = errors.New("field no exist")
)

func (in Instance) Field(name string) (reflect.Value, error) {
	if i, ok := in.index[name]; ok {
		return in.instance.Field(i), nil
	} else {
		return reflect.Value{}, FieldNoExist
	}
}

func (i *Instance) Interface() interface{} {
	return i.instance.Interface()
}
func (i *Instance) Addr() interface{} {
	return i.instance.Addr().Interface()
}
