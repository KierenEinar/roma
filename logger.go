package main

import (
	"fmt"
	"time"
)

func Log(msg string, a ...any) {
	msg = fmt.Sprintf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), msg)
	fmt.Printf(msg, a...)
	fmt.Printf("\n")
}
