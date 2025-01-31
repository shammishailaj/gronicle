package main

import (
	"log"
	"time"

	"github.com/shammishailaj/gronicle/pkg/scheduler"
)

func main() {
	log.Println("Starting Gronicle Server...")

	// Initialize the task scheduler
	s := scheduler.NewScheduler()

	// Example task: prints "Hello, Gronicle!" every 5 seconds
	task1 := &scheduler.Task{
		ID:   1,
		Name: "HelloTask",
		Execute: func() error {
			log.Println("Hello, Gronicle! Task is running.")
			return nil
		},
		Interval: 5 * time.Second,
	}

	// Add the task to the scheduler
	s.AddTask(task1)

	// Start the scheduler
	go s.Start()

	// Keep the main process running for testing
	select {}
}
