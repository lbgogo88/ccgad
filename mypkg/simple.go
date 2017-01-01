package mypkg


import (
    "sync"
    "github.com/garyburd/redigo/redis"
    "time"
)

type NGCrawler struct {
    Cfg         *Config
    quit        bool
    waitgroup   *sync.WaitGroup
    pool *redis.Pool
    relpool *redis.Pool

}


func (self *NGCrawler) Close() {
    self.quit = true
    self.waitgroup.Wait()
}

func (self *NGCrawler) Run() {
    self.quit = false
    self.waitgroup = &sync.WaitGroup{}

    maxProcess := self.Cfg.MaxProcess
    self.pool = &redis.Pool{
        MaxIdle: 3,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", self.Cfg.Redis, redis.DialConnectTimeout(500*time.Millisecond))
            if err != nil {
                return nil, err
            }
            if _, err := c.Do("SELECT", 0); err != nil {
                c.Close()
                return nil, err
            }
            return c, nil
        },
    }
    self.relpool = &redis.Pool{
        MaxIdle: 3,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", self.Cfg.Redis, redis.DialConnectTimeout(500*time.Millisecond))
            if err != nil {
                return nil, err
            }
            if _, err := c.Do("SELECT", 2); err != nil {
                c.Close()
                return nil, err
            }
            return c, nil
        },
    }


    for i := int64(0); i < maxProcess; i++ {
        worker := &Worker{}

        worker.redis, _ = GetRedis(self.Cfg.Redis,0)
        worker.redisRelDevGroup, _ = GetRedis(self.Cfg.Redis, 2)

        worker.Http = GetHTTP()
        worker.HttpInfluxDB = GetHTTP()

        worker.Cfg = self.Cfg
        worker.Context = self

        go worker.Run()
    }

}


