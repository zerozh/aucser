package auccore

import "fmt"

const (
	CodeSuccess        = 0
	CodeServerNotReady = 2
	CodeServerEnd      = 3

	CodeRequestInvalid      = 4
	CodeRequestInvalidPrice = 5
	CodeRequestInvalidTime  = 6
	CodeRequestNotAttend    = 7

	CodeRequestGTWarningPrice   = 12
	CodeRequestAttendFirstRound = 13
	CodeRequestEnd1             = 14

	CodeRequestOutOfRange          = 21
	CodeRequestNotAttendFirstRound = 22
	CodeRequestAllIn               = 23
	CodeRequestSamePrice           = 24
	CodeRequestEnd2                = 25

	CodeServerSaveError0 = 30
	CodeServerSaveError1 = 31
	CodeServerSaveError2 = 32
	CodeServerSaveError3 = 33
	CodeServerSaveError4 = 34
	CodeServerSaveError5 = 35

	CodeSuccessfulBid = 41
	CodeFailBid       = 42
)

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("%v: %v", e.Code, e.Message)
}
