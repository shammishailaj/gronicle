package main

import (
	"log"
	"time"

	"github.com/shammishailaj/gronicle/pkg/scheduler"
	"github.com/shammishailaj/gronicle/pkg/storage"
)

func main() {
	log.Println("Starting Gronicle Server...")

	// Connect to MySQL
	db := storage.ConnectMySQL("scalland", "scallandpass", "localhost:3306", "gronicle")
	defer db.Close()

	// Initialize the scheduler with 5 workers, 3 retry attempts, and a 10-second polling interval
	s := scheduler.NewSchedulerWithDB(db, 5, 3, 10*time.Second)

	// Start polling for new tasks
	s.LoadTasksFromDB()

	// Start the scheduler
	s.Start()

	// Keep the main process running
	select {}
}
