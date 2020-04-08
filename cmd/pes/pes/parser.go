package pes

import (
	"encoding/json"
	"io"
)

type Script struct {
	Statistic struct {
		Email string `json:"email"`
		Tool  string `json:"tool"`
		Key   string `json:"key"`
	} `json:"statistic"`
	PMIDS []int `json:"pmids"`
}

func Parse(f io.Reader) (*Script, error) {
	var script *Script
	err := json.NewDecoder(f).Decode(&script)
	return script, err
}
