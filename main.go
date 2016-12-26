package main

import (
    "regexp"
    "strings"
    "fmt"
    "bufio"
    "io/ioutil"
    "compress/gzip"
    "log"
    "os"
)
func scan_log(filename string) {
    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    gz, err := gzip.NewReader(file)
    if err != nil {
        log.Fatal(err)
    }

    scanner := bufio.NewScanner(gz)
    for scanner.Scan(){
        s := strings.Split(scanner.Text()," ")
        fmt.Println(s)
      //fmt.Fprintf(os.Stdout,"%s\n",scanner.Text())
    }
    hostname, _ := os.Hostname()
    fmt.Println(hostname)

    defer file.Close()
    defer gz.Close()

}
func main() {
    dir, err := ioutil.ReadDir("/var/log")
    if err != nil {
        log.Fatal(err)
    }
    r := regexp.MustCompile(`.*z$`)
    for _, fi := range dir {
        fn := fi.Name()
        if ! fi.IsDir() && r.MatchString(fn) {
            scan_log(fmt.Sprintf("%s/%s", "/var/log", fn))
        }
    }
}

