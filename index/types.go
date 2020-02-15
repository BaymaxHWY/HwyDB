package index

// 类型转换 interface{} => 定义好的类型
func typeToKey(input interface{}) Key {
	var out Key
	switch input.(type) {
	case int:
		out = myint(input.(int))
	case int8:
		out = myint8(input.(int8))
	case int16:
		out = myint16(input.(int16))
	case int32:
		out = myint16(input.(int32))
	case int64:
		out = myint64(input.(int64))
	case float32:
		out = myfloat32(input.(float32))
	case float64:
		out = myfloat64(input.(float64))
	case string:
		out = mystr(input.(string))
	default:
		panic("this key is not support!")
	}
	return out
}

// 整型
type myint int

func (m myint) Less(than Key) bool {
	than, ok := than.(myint)
	if ok {
		return m < than.(myint)
	}
	panic("this key need int")
}

type myint8 int8

func (m myint8) Less(than Key) bool {
	than, ok := than.(myint8)
	if ok {
		return m < than.(myint8)
	}
	panic("this key need int8")
}

type myint16 int16

func (m myint16) Less(than Key) bool {
	than, ok := than.(myint16)
	if ok {
		return m < than.(myint16)
	}
	panic("this key need int16")
}

type myint32 int32

func (m myint32) Less(than Key) bool {
	than, ok := than.(myint32)
	if ok {
		return m < than.(myint32)
	}
	panic("this key need int32")
}

type myint64 int64

func (m myint64) Less(than Key) bool {
	than, ok := than.(myint64)
	if ok {
		return m < than.(myint64)
	}
	panic("this key need int64")
}

// 浮点型
type myfloat32 float32

func (m myfloat32) Less(than Key) bool {
	than, ok := than.(myfloat32)
	if ok {
		return m < than.(myfloat32)
	}
	panic("this key need float32")
}

type myfloat64 float64

func (m myfloat64) Less(than Key) bool {
	than, ok := than.(myfloat64)
	if ok {
		return m < than.(myfloat64)
	}
	panic("this key need float64")
}

// 字符型
type mystr string

func (m mystr) Less(than Key) bool {
	than, ok := than.(mystr)
	if ok {
		return m < than.(mystr)
	}
	panic("this key need string")
}
