package mypkg

import (
    "os"

    "github.com/garyburd/redigo/redis"
    "time"
    "net/http"
    "net"
    "errors"
    "fmt"
)


func DirExists(path string) bool{
    finfo, err := os.Stat(path)
    if err != nil {
        return false
    }
    return finfo.IsDir()
}

func GetRedis(connstr string, db int) (redis.Conn,error) {
    conn, err := redis.Dial("tcp", connstr,redis.DialConnectTimeout(500*time.Millisecond))
    if err != nil {
        return nil, err
    }
    rv, err := redis.String(conn.Do("select",db))
    if err != nil {
        conn.Close()
        return nil, err
    }
    if rv != "OK" {
        conn.Close()
        return nil, errors.New(fmt.Sprintf("select db[%d] faild", db))

    }
    return conn, err

}
func GetHTTP() (*http.Client) {
    return &http.Client{
        Transport: &http.Transport{
            Dial: func(netw, addr string) (net.Conn, error) {
                c, err := net.DialTimeout(netw, addr, 500 * time.Millisecond)
                if err != nil {
                    //fmt.Println("dail timeout", err)
                    return nil, err
                }
                return c, nil
            },
            ResponseHeaderTimeout: time.Second,
        },
        Timeout: 5 * time.Second,
    }
}