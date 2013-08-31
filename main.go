package main

import (
    //"strings"
    "flag"
    "log"
)

var process = flag.String("process", "streaming", "The process to start")

func main() {
    flag.Parse()
    switch *process {
    case "streaming":
        log.Println("Starting Log Stream Processing")
        ProcessLogParts()
    case "aggregate":
        panic("aggregate not supported yet")
    default:
        panic("Invalid process option selected")
    }
}
