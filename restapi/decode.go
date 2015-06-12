package restapi

import (
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/gorilla/mux"
)

// Unmarshal decodes values from the request into a tagged struct.
//
// Similar to json.Unmarshal, but reads the values from the request, based on
// Swagger's naming convention for the parameter locations.
//
// Fields with the following tags will be read from the respective sources:
//   `path:"key"` -- gorilla/mux route parameters
//   `query:"key"` -- URL query parameters
//   `formData:"key"` -- form POST data
func Unmarshal(r *http.Request, o interface{}) error {
	r.ParseForm()
	// using r.Form instead of r.PostForm, since etcd seems to allow
	// parameters set in either
	return unmarshal(mux.Vars(r), r.URL.Query(), r.Form, o)
}

func unmarshal(path map[string]string, query, form url.Values, o interface{}) error {
	v := reflect.ValueOf(o)
	typ := v.Type().Elem()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		var value string
		if key := field.Tag.Get("path"); key != "" {
			value = path[key]
		} else if key := field.Tag.Get("query"); key != "" {
			value = query.Get(key)
		} else if key := field.Tag.Get("formData"); key != "" {
			value = form.Get(key)
		} else {
			continue
		}

		err := assign(v.Elem().Field(i), value)
		if err != nil {
			return err
		}
	}

	return nil
}

func assign(v reflect.Value, value string) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(value)
	case reflect.Bool:
		// TODO error for values other than true / false
		v.SetBool(value != "" && value != "false")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, err := strconv.ParseInt(value, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(val)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val, err := strconv.ParseUint(value, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(val)
	case reflect.Ptr:
		if value != "" {
			newV := reflect.New(v.Type().Elem())
			err := assign(reflect.Indirect(newV), value)
			if err != nil {
				return err
			}
			v.Set(newV)
		}
	}
	return nil
}
