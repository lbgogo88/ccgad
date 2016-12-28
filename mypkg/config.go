package mypkg

import (
    "gopkg.in/yaml.v2"
    "os"
    "io/ioutil"
    "fmt"
    "strings"
    "errors"
)


type Config struct {
    DataPath string `yaml:"DataPath"`
    MaxProcess int64 `yaml:"MaxProcess"`
    HttpMaxTime int64 `yaml:"HttpMaxTime"`
    Redis string `yaml:"Redis"`
    Metric map[string]string `yaml:"Metric"`
}

func ToConfig(cfgPath string) (cfg *Config, err error){
    cfg = &Config{}
    fout,err := os.Open(cfgPath)
    defer fout.Close()
    if err != nil {
        return nil, err
    }

    body, err := ioutil.ReadAll(fout)
    if err != nil {
        return nil,err
    }
    err = yaml.Unmarshal(body, &cfg)
    if err != nil {
        return nil,err
    }
    return cfg,check(cfg)
}

func check(cfg *Config) error {
    for k, _ := range cfg.Metric {
        path := strings.Join([]string{cfg.DataPath,k}, string(os.PathSeparator))
        if ! DirExists(path) {
            if err := os.Mkdir(path,0774); err != nil {
				return errors.New(fmt.Sprintf("can not init data dir, %s\n", err))
			}

        }
    }
	return nil
}