package main

import (
	"github.com/shammishailaj/gronicle/pkg/scheduler"
	"github.com/shammishailaj/gronicle/pkg/storage"
	"log"
)

func main() {
	log.Println("Starting Gronicle Server...")

	// Connect to MySQL
	db := storage.ConnectMySQL("scalland", "scallandpass", "localhost:3306", "gronicle")
	defer db.Close()

	// Initialize the scheduler with DB
	s := scheduler.NewSchedulerWithDB(db)

	// Load tasks from the database and start the scheduler
	s.LoadTasksFromDB()
	go s.Start()

	// Keep the main process running
	select {}
}
