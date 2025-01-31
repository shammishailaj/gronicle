package main

import (
	"github.com/shammishailaj/gronicle/api"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/shammishailaj/gronicle/pkg/scheduler"
	"github.com/shammishailaj/gronicle/pkg/storage"
)

func main() {
	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		serverPort = "9999"
	}
	log.Println("Starting Gronicle Server...")

	// Connect to MySQL
	db := storage.ConnectMySQL("scalland", "scallandpass", "localhost:3306", "gronicle")
	defer db.Close()

	// Initialize the S3 logger
	s3Logger := storage.NewS3Logger("gronicle-logs", "us-east-1")

	// Initialize the scheduler with 5 workers, 3 retry attempts, and a 10-second polling interval
	s := scheduler.NewSchedulerWithDB(db, 5, 3, 10*time.Second)

	// Initialize the worker pool with the S3 logger
	s.WorkerPool = scheduler.NewWorkerPool(5, 3, s3Logger)

	// Start polling for new tasks
	s.LoadTasksFromDB()

	// Start the scheduler
	s.Start()

	// Set up the API server
	router := api.InitializeRouter(db, s3Logger)

	log.Println("Starting API server on port %s...", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, router))

	// Keep the main process running
	select {}
}
