package auccore

import "fmt"

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("%v: %v", e.Code, e.Message)
}
