package utils

import (
	"github.com/fatih/color"
	logger "log"
	"reflect"
)

func Log(msg string) {
	logger.Println(color.MagentaString(msg))
}

func Logf[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}

	logger.Printf(color.MagentaString(msg), values...)
}

func LogGREEN[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}
	logger.Printf(color.GreenString(msg), values...)
}

func LogCYAN[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}
	logger.Printf(color.CyanString(msg), values...)
}

func LogRED[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}
	logger.Printf(color.RedString(msg), values...)
}

func LogYELLOW[T any](msg string, args ...T) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg).Interface()
	}
	logger.Printf(color.YellowString(msg), values...)
}
