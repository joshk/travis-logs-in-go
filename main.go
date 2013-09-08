package main

import (
    "flag"
    "log"
)

var process = flag.String("process", "streaming", "The process to start")

func init() {
    log.SetFlags(0)
}

func main() {
    flag.Parse()
    switch *process {
    case "streaming":
        startLogPartsProcessing()
    case "aggregate":
        panic("aggregate not supported yet")
    default:
        panic("Invalid process option selected")
    }
}
