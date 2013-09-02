package main

import (
    "flag"
)

var process = flag.String("process", "streaming", "The process to start")

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
