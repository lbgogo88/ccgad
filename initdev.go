package main

import "github.com/garyburd/redigo/redis"
import "os"
import "io/ioutil"
import "encoding/json"
import "fmt"

//type dev struct {
//    name string `json:"devName"`
//    code string `json:"devCode"`
//    status string `json:"status"`
//    ip string `json:"ip"`
//}
type dev struct {
    Name string `json:"devName"`
    Code string `json:"devCode"`
    Status string `json:"status"`
    IP string `json:"ip"`
}

func (self *dev) ToMap() map[string]string {
    var m map[string]string
    m["mm"] = "cc"
    return m
}

func main() {
    fout, _:= os.Open("/Users/bo/CC/inspection_rcms/data/devices.json")
    body, _:= ioutil.ReadAll(fout)

    var alldev []map[string]interface{}

    _ = json.Unmarshal(body, &alldev)
    r, _ := redis.Dial("tcp", "192.168.8.198:6379")
    fmt.Println(len(alldev))

    for _, d := range alldev {
        d["name"] = d["devName"]
        if dv, _ := redis.StringMap(r.Do("hgetall", d["name"])); dv != nil {
            if _, ok := dv["name"]; !ok {
                _, err := r.Do("hmset", redis.Args{}.Add(d["devName"]).AddFlat(d)...)
                if err != nil {
                    fmt.Println(err)
                }
                _, _ = r.Do("lpush", "request", d["name"])
            } else {
                fmt.Println("exists ", d["name"])
            }
        }

    }
}

