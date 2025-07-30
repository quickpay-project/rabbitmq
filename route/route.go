package route

import (
	"errors"
	"log"
	"net/http"
	"reflect"
	"strings"

	controller "github.com/celalsahinaltinisik/controllers"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func urls(s map[string]string, str string, method string) (bool, string) {
	for i, v := range s {
		if i == str && v == method {
			return true, cases.Title(language.English).String(strings.Replace(str, "/", "", -1))
		}
	}
	return false, ""
}

func Routeing() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		routings := map[string]string{"/home": "GET", "/publish": "POST", "/consume": "GET"}
		log.Println(r.URL.Path, r.Method)
		check, funcName := urls(routings, r.URL.Path, r.Method)
		log.Println(funcName)
		if !check {
			http.Error(w, "404 not found.", http.StatusNotFound)
			return
		}

		// controller.Publish(w, r)
		m := controller.Functions{}
		Call(m, funcName, w, r) ////todo
	}
}

func Call(m interface{}, name string, params ...interface{}) (result []reflect.Value, err error) {
	f := reflect.ValueOf(m).MethodByName(name)
	if len(params) != f.Type().NumIn() {
		err = errors.New("The number of params is not adapted.")
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)
	return
}
