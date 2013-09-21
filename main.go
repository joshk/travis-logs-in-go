package main

import (
	_ "expvar"
	"flag"
	"github.com/davecheney/profile"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var process = flag.String("process", "streaming", "The process to start")

func profileHandler() {
	p := profile.Start(&profile.Config{
		CPUProfile:     true,
		NoShutdownHook: true,
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

	go func() {
		http.ListenAndServe(":8080", nil)
	}()

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
