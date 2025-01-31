package scheduler

import (
	"database/sql"
	"log"
	"time"

	"github.com/shammishailaj/gronicle/pkg/storage"
)

type Scheduler struct {
	workerPool   *WorkerPool
	db           *sql.DB
	pollInterval time.Duration
}

// NewSchedulerWithDB initializes a scheduler with a database connection and worker pool.
func NewSchedulerWithDB(db *sql.DB, workerCount int, retryLimit int, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		db:           db,
		workerPool:   NewWorkerPool(workerCount, retryLimit),
		pollInterval: pollInterval,
	}
}

// LoadTasksFromDB continuously polls the database and adds pending tasks to the worker pool.
func (s *Scheduler) LoadTasksFromDB() {
	go func() {
		for {
			log.Println("Polling database for new tasks...")

			tasks, err := storage.FetchPendingTasks(s.db)
			if err != nil {
				log.Printf("Error fetching tasks: %v", err)
			} else {
				for _, task := range tasks {
					s.workerPool.AddTask(&task)
				}
			}

			time.Sleep(s.pollInterval) // Wait before polling again
		}
	}()
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
