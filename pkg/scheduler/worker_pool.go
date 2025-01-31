package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/shammishailaj/gronicle/pkg/storage"
)

// WorkerPool manages a set of workers to execute tasks concurrently.
type WorkerPool struct {
	taskQueue   chan *storage.Task
	workerCount int
	wg          sync.WaitGroup
	retryLimit  int
}

// NewWorkerPool initializes a new worker pool.
func NewWorkerPool(workerCount int, retryLimit int) *WorkerPool {
	return &WorkerPool{
		taskQueue:   make(chan *storage.Task, 100),
		workerCount: workerCount,
		retryLimit:  retryLimit,
	}
}

// AddTask adds a task to the queue.
func (wp *WorkerPool) AddTask(task *storage.Task) {
	wp.taskQueue <- task
	log.Printf("Task added to queue: %s", task.JobName)
}

// Start initializes the workers and begins processing tasks.
func (wp *WorkerPool) Start() {
	log.Printf("Starting %d workers...", wp.workerCount)

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)

		go func(workerID int) {
			defer wp.wg.Done()

			for task := range wp.taskQueue {
				log.Printf("Worker %d executing task: %s", workerID, task.JobName)
				success := wp.executeTaskWithRetry(task)

				if success {
					log.Printf("Task completed successfully: %s", task.JobName)
				} else {
					log.Printf("Task failed after retries: %s", task.JobName)
				}
			}
		}(i)
	}
}

// executeTaskWithRetry tries to execute a task and retries if it fails.
func (wp *WorkerPool) executeTaskWithRetry(task *storage.Task) bool {
	for attempt := 1; attempt <= wp.retryLimit; attempt++ {
		log.Printf("Attempt %d to execute task: %s", attempt, task.JobName)

		if err := executeCommand(task); err == nil {
			return true // Task succeeded
		}

		log.Printf("Task failed on attempt %d: %s", attempt, task.JobName)
		time.Sleep(2 * time.Second) // Backoff before retry
	}

	return false // Task failed after retries
}

// Stop closes the task queue and waits for all workers to complete.
func (wp *WorkerPool) Stop() {
	close(wp.taskQueue)
	wp.wg.Wait()
	log.Println("All workers have completed their tasks.")
}
