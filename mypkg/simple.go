package mypkg

import "github.com/axgle/mahonia"
import "github.com/garyburd/redigo/redis"

import (
    "net/http"
    "fmt"
    "sync"
    "time"
    "io/ioutil"
    "net"
    "encoding/json"
    "strconv"
    "errors"
)

type NGCrawler struct {
    Cfg         *Config
    quit        bool
    waitgroup   *sync.WaitGroup
}

func (self *NGCrawler) Close() {
    self.quit = true
    self.waitgroup.Wait()
}

func (self *NGCrawler) getReids() redis.Conn {
    r, error := redis.Dial("tcp", self.Cfg.Redis)
    if error != nil {
        fmt.Println(error)
    }
    return r
}


func (self *NGCrawler) Run() {
    self.quit = false
    self.waitgroup = &sync.WaitGroup{}

    maxProcess := self.Cfg.MaxProcess

    fmt.Println(maxProcess)
    for i := 0; i < maxProcess; i++ {
        go self.readWorker()
    }

}

func (self *NGCrawler) readWorker() {
    fmt.Println("read start")
    self.waitgroup.Add(1)
    defer self.waitgroup.Done()

    redisClient := self.getReids()
    defer redisClient.Close()
    httpMaxTime := self.Cfg.HttpMaxTime
    httpClient := &http.Client{
        Transport: &http.Transport{
            Dial: func(netw, addr string) (net.Conn, error) {
                c, err := net.DialTimeout(netw, addr, 500*time.Millisecond)
                if err != nil {
                    //fmt.Println("dail timeout", err)
                    return nil, err
                }
                return c, nil
            },
            ResponseHeaderTimeout: time.Second,
        },
        Timeout: time.Duration(httpMaxTime) * time.Second,
    }

    for {
        if self.quit {
            break
        }
        var dv map[string]string
        dvname, _:= redis.String(redisClient.Do("rpop", "request"))
        if dvname == "" {
            fmt.Println("# have a sleep")

            time.Sleep(time.Second)
            continue
        }
        //fmt.Println("#Receive Device ", dvname)
        if dv, _ = redis.StringMap(redisClient.Do("hgetall", dvname)); dv != nil {
            if dv["status"] != "OPEN" {
                continue
            }
            v,err := strconv.Atoi(dv["failed"])
            if err == nil && v > 1 {
                fmt.Println("# dev failed before  ", dvname)
                continue
            }

            if err := self.readNG(httpClient, dv); err != nil {
                redisClient.Do("hincrby", dv["name"], "failed", 1)
                redisClient.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
            } else {
                redisClient.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL", "failed", 0)
            }
        }
    }
    fmt.Println("read out")
    return
}


func (self *NGCrawler) readNG(client* http.Client, dv map[string]string) error{
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
        response, err = client.Do(request)
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

func (self *NGCrawler) handleJson(j map[string]interface{}, app string, dv map[string]string) error{
    var err error = errors.New("failed")
    for metric, v := range j[app].(map[string]interface{}) {
        var t interface{}
        t = v.(map[string]interface{})["step"]
        if t == nil {
            break
        }
        step := int64(t.(float64))

        t = v.(map[string]interface{})["etime"]
        if t == nil {
            break
        }
        etime := int64(t.(float64))

        t = v.(map[string]interface{})["stime"]
        if t == nil {
            break
        }
        stime := int64(t.(float64))
        t = v.(map[string]interface{})["val"]
        if t == nil {
            break
        }
        valTmp := t.([]interface{})
        val := make([]float64, len(valTmp))
        for i, v := range valTmp {
            val[i],_ = strconv.ParseFloat(v.(string),64)
        }

        fmt.Sprint("%s %s %d %s %d\n",app,metric,step,time.Unix(etime,0).String(),stime)
        fmt.Sprintf("%f\n",val)
        //fmt.Println(dv)
        err = nil
    }
    return err
}


