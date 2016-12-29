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
    if dv["status"] != "OPEN" {
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL", "failed", 0)
        return
    }

    if v,err := strconv.Atoi(dv["failed"]); err == nil && v > 5 {
        fmt.Println("# dev failed before  ", dvname)
        return
    }

    if err := self.readNG(dv); err != nil {
        fmt.Printf("Failed %s %s\n",dv["name"],dv["ip"])
        self.Redis.Do("hincrby", dv["name"], "failed", 1)
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
    } else {
        fmt.Printf("Success %s %s\n",dv["name"],dv["ip"])
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL", "failed", 0)
    }

    return
}

func (self *Worker) readNG(dv map[string]string) error{
    dec := mahonia.NewDecoder("gbk")
    var err error = nil
    var request *http.Request
    var response *http.Response
    var body []byte

    url := fmt.Sprintf("http://%s:21900/Rrd/%s/%s/daily", dv["ip"], dv["name"], self.Cfg.Metric)

    request, err = http.NewRequest("GET", url, nil)
    if err != nil {
        return err
    }
    response, err = self.Http.Do(request)
    if err != nil {
        return err
    }

    body, err = ioutil.ReadAll(response.Body)
    if err != nil {
        return err
    }
    _, body, _ = dec.Translate(body, true)
    var j map[string]interface{}
    if err = json.Unmarshal(body, &j); err != nil {
        panic(err)
        return err
    }
    return self.handleJson(j,dv)
}

func (self *Worker) handleJson(j map[string]interface{}, dv map[string]string) (error) {
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
            NGM(dv, measurementName, v.(map[string]interface{})).Save()
        }
    }
    return nil
}


