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

    for {
        if self.Context.quit {
            break
        }
        if self.Redis == nil || self.Redis.Err() != nil {
            self.Redis, err = GetRedis(self.Cfg)
            if err != nil {
                fmt.Println(err)
                time.Sleep(5 * time.Second)
                continue
            }
        }

        dvname, _:= redis.String(self.Redis.Do("rpop", "request"))
        if dvname == "" {
            fmt.Println("# have a sleep")
            time.Sleep(time.Second)
            continue
        }
        self.Do(dvname)
    }
    fmt.Println("read out")
    return
}

func (self *Worker) Do(dvname string) {
    dv, _ := redis.StringMap(self.Redis.Do("hgetall", dvname))
    if dv == nil || dv["status"] != "OPEN" {
        return
    }

    if v,err := strconv.Atoi(dv["failed"]); err == nil && v > 1 {
        return
        fmt.Println("# dev failed before  ", dvname)
    }

    if err := self.readNG(dv); err != nil {
        self.Redis.Do("hincrby", dv["name"], "failed", 1)
        self.Redis.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
    } else {
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

    for app, metric := range self.Cfg.Metric {
        url := fmt.Sprintf("http://%s:21900/Rrd/%s/%s/daily", dv["ip"], dv["name"], metric)

        request, err = http.NewRequest("GET", url, nil)
        if err != nil {
            break
        }
        response, err = self.Http.Do(request)
        if err != nil {
            fmt.Println("Err Connection: ",err)
            break
        }

        body, err = ioutil.ReadAll(response.Body)
        if err != nil {
            fmt.Println("Err Read: ",err)
            break
        }
        _, body, _ = dec.Translate(body, true)
        var j map[string]interface{}
        if err = json.Unmarshal(body, &j); err != nil {
            panic(err)
            fmt.Println("Err JSON: ", err)
            break
        }

        if _, ok := j["msg"]; ok {
            fmt.Printf("Err Data: %s %s %s\n", dv["name"], app, j["msg"])
            continue
        }
        self.handleJson(j,app,dv)
    }
    return err
}

func (self *Worker) handleJson(j map[string]interface{}, app string, dv map[string]string) {
    for metric, v := range j[app].(map[string]interface{}) {
        if m, err := NGM(dv, metric, v.(map[string]interface{})) ; err == nil {
            resp, err := http.Post("http://localhost:8086/write?db=mydb","", m.Buffer())
            fmt.Println(resp,err)
        }
    }
}


