package restapi

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestUnmarshal_Path(t *testing.T) {
	v := struct {
		String string `path:"aString"`
		Bool   bool   `path:"aBool"`
		Number int    `path:"num"`
	}{}

	ok(t, unmarshal(map[string]string{
		"aString": "bar",
		"aBool":   "true",
		"num":     "42",
	}, nil, nil, &v))

	equals(t, v.String, "bar")
	equals(t, v.Bool, true)
	equals(t, v.Number, 42)
}

func TestUnmarshal_Query(t *testing.T) {
	v := struct {
		String string `query:"aString"`
		Bool   bool   `query:"aBool"`
		Number int    `query:"num"`
	}{}

	ok(t, unmarshal(nil, map[string][]string{
		"aString": {"bar"},
		"aBool":   {"true"},
		"num":     {"42"},
	}, nil, &v))

	equals(t, v.String, "bar")
	equals(t, v.Bool, true)
	equals(t, v.Number, 42)
}

func TestUnmarshal_Form(t *testing.T) {
	v := struct {
		String string `formData:"aString"`
		Bool   bool   `formData:"aBool"`
		Number int    `formData:"num"`
	}{}

	ok(t, unmarshal(nil, nil, map[string][]string{
		"aString": {"bar"},
		"aBool":   {"true"},
		"num":     {"42"},
	}, &v))

	equals(t, v.String, "bar")
	equals(t, v.Bool, true)
	equals(t, v.Number, 42)
}

func TestUnmarshal_InvalidNumber(t *testing.T) {
	v := struct {
		Number int `path:"num"`
	}{}

	err := unmarshal(map[string]string{
		"num": "asdf",
	}, nil, nil, &v)

	if err == nil {
		t.Fatal("expected an error for invalid number, but got nil")
	}
}

func TestUnmarshal_InvalidNumberPointer(t *testing.T) {
	v := struct {
		Number *int `path:"num"`
	}{}

	err := unmarshal(map[string]string{
		"num": "asdf",
	}, nil, nil, &v)

	if err == nil {
		t.Fatal("expected an error for invalid number, but got nil")
	}
}

func TestAssign_String(t *testing.T) {
	var v string
	ok(t, assign(reflect.ValueOf(&v).Elem(), "bar"))
	equals(t, v, "bar")
}

func TestAssign_StringPointer(t *testing.T) {
	var v *string
	ok(t, assign(reflect.ValueOf(&v).Elem(), "bar"))
	equals(t, *v, "bar")
}

func TestAssign_Bool_True(t *testing.T) {
	v := false
	ok(t, assign(reflect.ValueOf(&v).Elem(), "true"))
	equals(t, v, true)
}

func TestAssign_Bool_False(t *testing.T) {
	v := true
	ok(t, assign(reflect.ValueOf(&v).Elem(), "false"))
	equals(t, v, false)
}

func TestAssign_BoolPointer_True(t *testing.T) {
	v := new(bool)
	*v = false
	ok(t, assign(reflect.ValueOf(&v).Elem(), "true"))
	equals(t, true, *v)
}

func TestAssign_BoolPointer_False(t *testing.T) {
	v := new(bool)
	*v = true
	ok(t, assign(reflect.ValueOf(&v).Elem(), "false"))
	equals(t, false, *v)
}

func TestAssign_Int(t *testing.T) {
	var v int
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, 42, v)
}

func TestAssign_IntPointer(t *testing.T) {
	var v *int
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, 42, *v)
}

func TestAssign_Int8(t *testing.T) {
	var v int8
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, int8(42), v)
}

func TestAssign_Int16(t *testing.T) {
	var v int16
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, int16(42), v)
}

func TestAssign_Int32(t *testing.T) {
	var v int32
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, int32(42), v)
}

func TestAssign_Int64(t *testing.T) {
	var v int64
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, int64(42), v)
}

func TestAssign_Uint(t *testing.T) {
	var v uint
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, uint(42), v)
}

func TestAssign_Uint8(t *testing.T) {
	var v uint8
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, uint8(42), v)
}

func TestAssign_Uint16(t *testing.T) {
	var v uint16
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, uint16(42), v)
}

func TestAssign_Uint32(t *testing.T) {
	var v uint32
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, uint32(42), v)
}

func TestAssign_Uint64(t *testing.T) {
	var v uint64
	ok(t, assign(reflect.ValueOf(&v).Elem(), "42"))
	equals(t, uint64(42), v)
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
