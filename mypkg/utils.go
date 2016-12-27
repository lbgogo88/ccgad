package mypkg

import (
    "os"

)


func DirExists(path string) bool{
    finfo, err := os.Stat(path)
    if err != nil {
        return false
    }
    return finfo.IsDir()
}