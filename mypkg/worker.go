package mypkg

import (
	"fmt"
	"time"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"github.com/axgle/mahonia"
	"net/http"
	"io/ioutil"
	"encoding/json"
    "errors"
)
type Worker struct {
    Context     *NGCrawler
    Cfg         *Config
    Http        *http.Client
    Redis       redis.Conn
}

func (self *Worker) Run() {
    fmt.Println("read start")
    self.Context.waitgroup.Add(1)
    defer self.Context.waitgroup.Done()

    var err error = nil
    var cntFails int = 0
    var cntSleep int = 0

    for {
        if self.Context.quit {
            break
        }
        if self.Redis == nil || self.Redis.Err() != nil {
            cntFails++
            self.Redis, err = GetRedis(self.Cfg)
            if err != nil {
                fmt.Println(err)
                time.Sleep(5 * time.Second)
                continue
            }
        }
        cntFails = 0
        dvname, _:= redis.String(self.Redis.Do("rpop", "request"))
        if dvname == "" {
            cntSleep++
            if cntSleep % 60 == 0 {
                fmt.Println("nothing to do, have a sleep")
            }
            time.Sleep(time.Second)
            continue
        }
        cntSleep = 0
        self.Do(dvname)
    }
    fmt.Println("read out")
    return
}

func (self *Worker) Do(dvname string) {
    dv, _ := redis.StringMap(self.Redis.Do("hgetall", dvname))
    if dv == nil {
        return
    }

    if v,err := strconv.Atoi(dv["failed"]); err == nil && v > 5 {
        fmt.Println("# dev failed before  ", dvname)
        return
    }

    if bytes, err := self.ReadNG(dv); err != nil {
        self.Redis.Send("hincrby", dv["name"], "failed", 1)
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
    } else {
        self.SaveJson(bytes,dv)
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL", "failed", 0)
    }

    return
}

func (self *Worker) ReadNG(dv map[string]string) ([]byte, error) {
    dec := mahonia.NewDecoder("gbk")
    var err error = nil
    var request *http.Request
    var response *http.Response
    var body []byte

    url := fmt.Sprintf("http://%s:21900/Rrd/%s/%s/daily", dv["ip"], dv["name"], self.Cfg.Metric)

    request, err = http.NewRequest("GET", url, nil)
    if err != nil {
        return nil,err
    }
    response, err = self.Http.Do(request)
    if err != nil {
        return nil,err
    }

    body, err = ioutil.ReadAll(response.Body)
    if err != nil {
        return nil,err
    }
    _, body, _ = dec.Translate(body, true)
    return body, nil
}

func (self *Worker) SaveJson(bytes []byte, dv map[string]string) (error) {
    var j map[string]interface{}
    if err := json.Unmarshal(bytes, &j); err != nil {
        return err
    }
    if _, ok := j["msg"]; ok {
        return errors.New(fmt.Sprintf("Err Data: %s %s %s\n", dv["name"], j["msg"]))
    }
    for app, metric := range j {
        if _, ok := metric.(string); ok {
            continue
        }
        for measurementName, v := range metric.(map[string]interface{}) {
            if msg, ok := v.(string); ok {
                fmt.Printf("Invalid metric %s %s %s %s\n", dv["name"], app, measurementName, msg)
                break
            }
            NGM(self.Cfg, dv, measurementName, v.(map[string]interface{})).Save()
        }
    }
    return nil
}


