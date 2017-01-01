package main

import (
	"os"
	"os/signal"
	"fmt"
	"./mypkg"
)

func main() {
    cSignal := make(chan os.Signal,1)
    done := make(chan bool, 1)
    signal.Notify(cSignal)
    cfg,err := mypkg.ToConfig("config.yaml")
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    fmt.Printf("Start %d\n", os.Getpid())

    crawler := &mypkg.NGCrawler{Cfg:cfg}
    crawler.Run()
    defer crawler.Close()


    go func() {
        sig := <-cSignal
        fmt.Println("#####", sig)
        done <- true
    }()
    <-done
    fmt.Printf("Wating Worker Quit %d\n",os.Getpid())
}

