package main

//import "mypkg"
import "os"
import (
    "os/signal"
    "fmt"
    "mypkg"
)

func main() {
    cSignal := make(chan os.Signal,1)
    done := make(chan bool, 1)
    signal.Notify(cSignal)
    cfg,err := mypkg.ToConfig("config.json")
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    crawler := &mypkg.NGCrawler{Cfg:cfg}
    crawler.Run()

    fmt.Printf("Start %d\n", os.Getpid())

    go func() {
        sig := <-cSignal
        fmt.Println("#####", sig)
        done <- true
    }()
    <-done
    crawler.Quit()
    fmt.Printf("Quit %d\n",os.Getpid())
}

