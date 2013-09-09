package main

import (
    "flag"
    "github.com/davecheney/profile"
    "log"
    "os"
    "os/signal"
    "syscall"
)

var process = flag.String("process", "streaming", "The process to start")

func profileHandler() {
    p := profile.Start(&profile.Config{
        CPUProfile:   true,
        MemProfile:   true,
        BlockProfile: true,
    })

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

    <-sig

    p.Stop()

    os.Exit(0)
}

func init() {
    log.SetFlags(0)
}

func main() {
    go profileHandler()

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
