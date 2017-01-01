package mypkg

import (
	"strconv"
	"bytes"
	"fmt"
)

type NGMeasurement struct {
	Name string
	Device map[string]string
	Group []string
	STime int64
	ETime int64
	Step  int64
	Val   []float64
}


func (self *NGMeasurement) Buffer() (*bytes.Buffer){
	if self == nil {
		return nil
	}
	buf := new(bytes.Buffer)
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
	return buf
}

func NGM(device *Device, mname string, metric map[string]interface{}) (*NGMeasurement, error) {
	m := &NGMeasurement{Device:device.dv, Group:device.group, Name:mname}
	var gerr error = nil
	for key, val := range metric {
		if val == nil {
			gerr = fmt.Errorf("val is nil")
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
					gerr = err
					break
				}
			}
		}
		if gerr != nil {
			break
		}
	}

	if m.Group == nil || len(m.Group) == 0 {
		m.Group = make([]string, 1)
		m.Group[0] = "NULL"
	}

	if gerr != nil {
		return nil, gerr
	}
	return m,nil
}