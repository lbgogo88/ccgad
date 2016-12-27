package mypkg

import (
    "encoding/json"
    "os"
    "io/ioutil"
    "fmt"
    "strings"
)


type Config struct {
    DataPath string `json:"DataPath"`
    MaxProcess int `json:"MaxProcess"`
    Redis string `json:"Redis"`
    Metric map[string]string `json:"Metric"`
}

func ToConfig(cfgPath string) (cfg *Config, err error){
    fout,err := os.Open(cfgPath)
    defer fout.Close()
    if err != nil {
        return nil, err
    }

    body, err := ioutil.ReadAll(fout)
    if err != nil {
        return nil,err
    }
    err = json.Unmarshal(body, &cfg)
    if err != nil {
        return nil,err
    }
    check(cfg)
    return cfg,nil
}

func check(cfg *Config) {
    for k, _ := range cfg.Metric {
        path := strings.Join([]string{cfg.DataPath,k}, string(os.PathSeparator))
        if ! DirExists(path) {
            if err := os.Mkdir(path,0774); err != nil {
                fmt.Printf("Err: can not init data dir, %s\n", err)
            }

        }
    }
}