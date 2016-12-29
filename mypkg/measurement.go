package mypkg

import (
	//"time"
	//"fmt"
	"strconv"
	"errors"
	"bytes"
	"fmt"
	"net/http"
)

type NGMeasurement struct {
	Name string
	Device map[string]string
	STime int64
	ETime int64
	Step  int64
	Val   []float64
	Err	  error
}


func (m *NGMeasurement) Save() (*NGMeasurement, error){
	if m.Err != nil {
		return m,m.Err
	}
	buf := new(bytes.Buffer)
	for i := len(m.Val) - 1; i>=0; i-- {
		tsSecond := m.STime + int64(i)*m.Step
		if m.ETime - tsSecond > 3600 {
			break
		}
		ts := tsSecond * 1000000000
		s := fmt.Sprintf("%s,device=%s value=%f %d\n", m.Name, m.Device["name"], m.Val[i], ts)
		buf.WriteString(s)
	}

	resp, err := http.Post("http://localhost:8086/write?db=mydb","", buf)
	if err != nil || resp.StatusCode != 204 {
		m.Err = errors.New("save failed")
	}
	return m,m.Err

}

func NGM(dv map[string]string, mname string, v map[string]interface{}) (*NGMeasurement) {
	m := &NGMeasurement{Device:dv,Name:mname,Err:nil}
	for key, val := range(v) {
		if val == nil {
			m.Err = errors.New("val is nil")
			break
		}
		switch key {
		case "step":
			m.Step = int64(val.(float64))
		case "etime":
			m.ETime = int64(val.(float64))
		case "stime":
			m.STime = int64(val.(float64))
		case "val":
			valTmp := val.([]interface{})
			m.Val = make([]float64, len(valTmp))
			for i, v := range valTmp {
				if v.(string) == "" {
					v = "0"
				}
				if v, err := strconv.ParseFloat(v.(string), 64); err == nil {
					m.Val[i] = v
				} else {
					m.Err = err
					break
				}
			}
		}
		if m.Err != nil {
			break
		}
	}
	return m
}