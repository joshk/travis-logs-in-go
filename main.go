package main

import (
    //"strings"
    "log"
    "flag"
)

var process = flag.String("process", "streaming", "The process to start")

func main() {
    flag.Parse()
    switch *process {
    case "streaming":
        log.Println("Starting Log Stream Processing")
        ProcessLogParts()
    case "aggregate":
        log.Println("Starting Log Aggregation")
        log.Println("WE NEED AGGREGATION")
    default: 
        panic("Invalid process option selected")
    }
}
