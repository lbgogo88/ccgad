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
    Context             *NGCrawler
    Cfg                 *Config
    Http                *http.Client
    HttpInfluxDB        *http.Client
    redis               redis.Conn
    redisRelDevGroup    redis.Conn
}
type Device struct {
    dv map[string]string
    group []string
}

func (self *Worker) Close() {
    if self.redis != nil && self.redis.Err() == nil {
        self.redis.Close()
    }
    if self.redisRelDevGroup != nil && self.redisRelDevGroup.Err() == nil {
        self.redisRelDevGroup.Close()
    }
    self.Context.waitgroup.Done()
}

func (self *Worker) popJob() (*Device, error) {
    redisJobQueue := self.Context.pool.Get()
    defer redisJobQueue.Close()

    redisRelDevGroup := self.Context.relpool.Get()
    defer redisRelDevGroup.Close()

    if redisJobQueue.Err() != nil || redisRelDevGroup.Err() != nil {
        return nil, fmt.Errorf("bad redis connection")
    }

    dvname, err := redis.String(redisJobQueue.Do("rpop", "request"))
    if err != nil {
        return nil, err
    }
    if dvname == "" {
        return nil, nil
    }
    group, err := redis.Strings(redisRelDevGroup.Do("smembers", dvname))
    if err != nil {
        return nil, err
    }
    dvmap, err := redis.StringMap(redisJobQueue.Do("hgetall", dvname))
    if err != nil {
        return nil, err
    }
    dv := &Device{dv:dvmap, group:group}
    return dv, nil
}

func (self *Worker) failedJob(device *Device) (*Device, error) {
    redisJobQueue := self.Context.pool.Get()
    defer redisJobQueue.Close()

    redisJobQueue.Send("hincrby", device.dv["name"], "failed", 1)
    redisJobQueue.Do("hmset", device.dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
    return device,nil
}

func (self *Worker) succeedJob(device *Device) (*Device, error) {
    redisJobQueue := self.Context.pool.Get()
    defer redisJobQueue.Close()

    redisJobQueue.Do("hmset", device.dv["name"], "last", int(time.Now().Unix()), "state", "NULL", "failed", 0)
    return device,nil
}


func (self *Worker) Run() {
    fmt.Println("worker start")
    self.Context.waitgroup.Add(1)
    defer self.Close()

    for {
        if self.Context.quit {
            break
        }
        dv, err := self.popJob()
        if err != nil || dv == nil {
            if err != nil {
                fmt.Println(err)
            }
            time.Sleep(time.Second)
            continue
        }
        self.Do(dv)
    }
    fmt.Println("worker end")
    return
}


func (self *Worker) Do(device *Device) {
    if v,err := strconv.Atoi(device.dv["last"]); err == nil && int(time.Now().Unix()) - v < 300 {
        return
    }

    if bytes, err := self.ReadNG(device.dv); err != nil {
        self.failedJob(device)
    } else {
        obj := self.toJsonObj(bytes)
        self.Save(obj,device)
        self.succeedJob(device)
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

func (self *Worker) toJsonObj(bytes []byte) (map[string]interface{}) {
    var obj map[string]interface{}
    if err := json.Unmarshal(bytes, &obj); err != nil {
        return nil
    }
    return obj
}

func (self *Worker) Save(j map[string]interface{}, device *Device) (error) {
    if _, ok := j["msg"]; ok {
        return fmt.Errorf("Err Data: %s %s %s\n", device.dv["name"], j["msg"])
    }
    for app, metric := range j {
        if _, ok := metric.(string); ok {
            continue
        }
        for measurementName, v := range metric.(map[string]interface{}) {
            if msg, ok := v.(string); ok {
                fmt.Printf("Invalid metric %s %s %s %s\n", device.dv["name"], app, measurementName, msg)
                break
            }
            ngm, _ := NGM(device, measurementName, v.(map[string]interface{}))
            if buf := ngm.Buffer(); buf != nil {
                _, err := self.HttpInfluxDB.Post(self.Cfg.InfluxDB,"", buf)
                if err != nil {
                    fmt.Println(err)
                }
            }
        }
    }
    return nil
}


