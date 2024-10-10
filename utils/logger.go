package utils

import (
	"github.com/fatih/color"
	logger "log"
	"reflect"
)

func Log[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}
	logger.Printf(color.MagentaString(msg), values...)
}
