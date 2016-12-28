package mypkg

import (
	//"time"
	//"fmt"
	"strconv"
	"errors"
	"bytes"
	"fmt"
)

type NGMeasurement struct {
	Name string
	Device map[string]string
	STime int64
	ETime int64
	Step  int64
	Val   []float64
}

func (m *NGMeasurement) Buffer() *bytes.Buffer {
	buf := new(bytes.Buffer)
	for i,v := range m.Val {
		ts := (m.STime +int64(i)*m.Step) * 1000000000
		s := fmt.Sprintf("%s,device=%s value=%f %d\n", m.Name, m.Device["name"], v, ts)
		buf.WriteString(s)
	}
	return buf
}


func NGM(dv map[string]string, mname string, v map[string]interface{}) (*NGMeasurement, error) {
	m := &NGMeasurement{Device:dv,Name:mname}

	var t interface{}
	t = v["step"]
	if t == nil {
		return nil, errors.New("step")
	}
	m.Step = int64(t.(float64))


	t = v["etime"]
	if t == nil {
		return nil, errors.New("etime")
	}
	m.ETime = int64(t.(float64))

	t = v["stime"]
	if t == nil {
		return nil, errors.New("stime")
	}
	m.STime = int64(t.(float64))

	t = v["val"]
	if t == nil {
		return nil, errors.New("val")
	}

	valTmp := t.([]interface{})
	m.Val = make([]float64, len(valTmp))

	for i, v := range valTmp {
		if v.(string) == "" {
			v = "0"
		}
		if v,err := strconv.ParseFloat(v.(string),64); err == nil {
			m.Val[i] = v
		} else {
			return nil, errors.New("val")
		}
	}

	return m,nil

}