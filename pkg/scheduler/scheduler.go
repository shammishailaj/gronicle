package scheduler

import (
	"database/sql"
	"log"

	"github.com/shammishailaj/gronicle/pkg/storage"
)

type Scheduler struct {
	db         *sql.DB
	workerPool *WorkerPool
}

// NewSchedulerWithDB initializes a scheduler with a database connection and worker pool.
func NewSchedulerWithDB(db *sql.DB, workerCount int, retryLimit int) *Scheduler {
	return &Scheduler{
		db:         db,
		workerPool: NewWorkerPool(workerCount, retryLimit),
	}
}

// LoadTasksFromDB fetches tasks from MySQL and adds them to the worker pool.
func (s *Scheduler) LoadTasksFromDB() {
	tasks, err := storage.FetchPendingTasks(s.db)
	if err != nil {
		log.Fatalf("Error fetching tasks from database: %v", err)
	}

	for _, task := range tasks {
		s.workerPool.AddTask(&task)
	}
}

// Start begins task processing using the worker pool.
func (s *Scheduler) Start() {
	log.Println("Starting Scheduler...")
	go s.workerPool.Start() // Start the worker pool
}

// Stop stops the worker pool.
func (s *Scheduler) Stop() {
	s.workerPool.Stop()
	log.Println("Scheduler stopped.")
}
