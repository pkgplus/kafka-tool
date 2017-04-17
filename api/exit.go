package api

import (
	"os"
	"os/signal"
)

func getSignalChan(l int) chan bool {
	// Trap SIGINT to trigger a shutdown.
	exitchan := make(chan bool, l)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		for i := 0; i < l; i++ {
			exitchan <- true
		}
	}()

	return exitchan
}
