package aucserver

import "github.com/zerozh/aucser/core"

type BidRequest struct {
	Client int `json:"client"`
	Pass   int `json:"pass"`
	Price  int `json:"price"`
}

type BidResponse struct {
	Code    int          `json:"code"`
	Message string       `json:"message"`
	Bid     *auccore.Bid `json:"bid,omitempty"`
}

type StatusResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Info    struct {
		StartTime    string `json:"startTime"`
		HalfTime     string `json:"halfTime"`
		EndTime      string `json:"endTime"`
		Capacity     int    `json:"capacity"`
		WarningPrice int    `json:"warningPrice"`

		Time        string `json:"time"`
		Session     int    `json:"session"`
		Bidders     int    `json:"bidders"`
		LowestPrice int    `json:"lowestPrice"`
		LowestTime  string `json:"lowestTime"`
	} `json:"info"`
}
