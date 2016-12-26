package mypkg

import "github.com/axgle/mahonia"
import "github.com/garyburd/redigo/redis"

import (
    "net/http"
    "fmt"
    "sync"
    "strconv"
    "time"
    "io/ioutil"
    "os"
    "strings"
)

type NGCrawler struct {
    Cfg         *Config
    quit        bool
    ch          chan map[string]string
    waitgroup   *sync.WaitGroup
}

func (self *NGCrawler) Quit() {
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
    self.ch = make(chan map[string]string, 1)
    self.waitgroup = &sync.WaitGroup{}

    maxProcess := self.Cfg.MaxProcess

    fmt.Println(maxProcess)
    go self.readWorker()
    //go self.testCommit()
    for i := 0; i < maxProcess; i++ {
        go self.writeWorker()
    }

}
func (self *NGCrawler) testCommit() {
    self.waitgroup.Add(1)
    dvname := "CHN-XA-f-3SG"
    redisClient := self.getReids()
    defer redisClient.Close()
    for {
        if self.quit {
            fmt.Println("testCommit quit")
            break
        }
        dv, err := redis.StringMap(redisClient.Do("hgetall", dvname))
        fmt.Println("# test ", dv)
        if err == nil {
            now := int(time.Now().Unix())
            last,_ := strconv.Atoi(dv["last"])
            if dv["state"] == "NULL" || now - last > 10 {
                redisClient.Do("hset", dvname, "state", "WORKING")
                redisClient.Do("lpush", "request", dvname)
            }
        } else {
            fmt.Println(err)
        }
        time.Sleep(2*time.Second)
    }
    defer self.waitgroup.Done()

}

func (self *NGCrawler) readWorker() {
    fmt.Println("read start")
    self.waitgroup.Add(1)
    redisClient := self.getReids()
    defer redisClient.Close()

    for {
        if self.quit {
            break
        }

        dvname, _:= redis.String(redisClient.Do("rpop", "request"))
        if dvname == "" {
            time.Sleep(time.Second)
            continue
        }
        fmt.Println("#Receive Device ", dvname)
        dv, _ := redis.StringMap(redisClient.Do("hgetall", dvname))
        if dv == nil || len(dv) < 2 {
            continue
        }
        select {
        case self.ch<-dv:
        default:
            fmt.Println("channel full")
            time.Sleep(10*time.Second)
        }


    }
    fmt.Println("read out")
    defer self.waitgroup.Done()
    return
}

func (self *NGCrawler) writeWorker() {
    self.waitgroup.Add(1)
    redisClient := self.getReids()
    defer redisClient.Close()
    client := &http.Client{}

    for {
        if self.quit {
            break
        }
        select {
        case dv := <-self.ch:
            fmt.Println("#Receive Map", dv)
            self.readNG(client,dv)
            redisClient.Do("hmset", dv["name"], "last", int(time.Now().Unix()), "state", "NULL")
        default:
            time.Sleep(time.Second)
        }

    }
    fmt.Println("write out")
    defer self.waitgroup.Done()
    return
}

func (self *NGCrawler) readNG(client* http.Client, dv map[string]string) {
    for app, metric := range self.Cfg.Metric {
        url := fmt.Sprintf("http://%s:21900/Rrd/%s/%s/daily", dv["ip"], dv["name"], metric)
        fmt.Println("#URL ", url)
        request, err := http.NewRequest("GET", url, nil)
        if err == nil {
            response, err := client.Do(request)
            if err != nil {
                fmt.Println(err)
            } else {
                self.saveResponse(response, app, dv["name"])
            }
        }
    }
}

func (self *NGCrawler) saveResponse(response *http.Response,app string,dvname string) {
    if response == nil {
        return
    }
    dir := strings.Join([]string{self.Cfg.DataPath,app}, string(os.PathSeparator))
    if ! DirExists(dir) {
        fmt.Printf("Err: %s does not exists.\n", dir)
        return
    }

    dec := mahonia.NewDecoder("gbk")
    body, _ := ioutil.ReadAll(response.Body)
    _,bodyUtf,_ := dec.Translate(body,true)

    path := strings.Join([]string{dir,fmt.Sprintf("%s.json",dvname)}, string(os.PathSeparator))
    fout,err := os.Create(path)
    defer fout.Close()
    if err == nil {
        fout.Write(bodyUtf)
    }else {
        fmt.Printf("Err: %s can not be written. %s\n", path, err)
    }
}


