package mypkg


import (
    "fmt"
    "sync"
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

func (self *NGCrawler) Run() {
    self.quit = false
    self.waitgroup = &sync.WaitGroup{}

    maxProcess := self.Cfg.MaxProcess

    fmt.Println(maxProcess)
    for i := int64(0); i < maxProcess; i++ {
        worker := &Worker{}
        worker.Redis, _ = GetRedis(self.Cfg)
        worker.Http = GetHTTP(self.Cfg)
        worker.Cfg = self.Cfg
        worker.Context = self
        go worker.Run()
    }

}


