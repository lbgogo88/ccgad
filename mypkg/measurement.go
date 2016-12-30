package mypkg

import (
	"strconv"
	"errors"
	"bytes"
	"fmt"
	"net/http"
)

type NGMeasurement struct {
	Name string
	Device map[string]string
	Group []string
	STime int64
	ETime int64
	Step  int64
	Val   []float64
	Err	  error
	Cfg   *Config
}


func (self *NGMeasurement) Save() (*NGMeasurement, error){
	if self.Err != nil {
		return self,self.Err
	}
	buf := new(bytes.Buffer)
	if self.Group == nil {
		self.Group = make([]string, 1)
		self.Group[0] = "NULL"
	}
	for i := len(self.Val) - 1; i>=0; i-- {
		tsSecond := self.STime + int64(i)*self.Step
		if self.ETime - tsSecond > 3600 {
			break
		}
		ts := tsSecond * 1000000000
		for _, g := range self.Group {
			s := fmt.Sprintf(
				"%s,device=%s,bgroup=%s,isp=%s,region=%s,p12=%s value=%f %d\n",
				self.Name,self.Device["name"],g,self.Device["isp"],self.Device["region"],self.Device["p12"],self.Val[i],ts)
			buf.WriteString(s)
		}
	}

	resp, err := http.Post(self.Cfg.InfluxDB,"", buf)
	if err != nil || resp.StatusCode != 204 {
		self.Err = errors.New("save failed")
	}
	return self,self.Err

}

func NGM(cfg *Config, dv map[string]string, group []string, mname string, v map[string]interface{}) (*NGMeasurement) {
	m := &NGMeasurement{Device:dv, Group:group, Name:mname,Cfg: cfg,Err:nil}
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