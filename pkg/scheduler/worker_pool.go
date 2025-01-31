package scheduler

import (
	"fmt"
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
	s3Logger    *storage.S3Logger
}

// NewWorkerPool initializes a new worker pool.
func NewWorkerPool(workerCount int, retryLimit int, s3Logger *storage.S3Logger) *WorkerPool {
	return &WorkerPool{
		taskQueue:   make(chan *storage.Task, 100),
		workerCount: workerCount,
		retryLimit:  retryLimit,
		s3Logger:    s3Logger,
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
				success, output := wp.executeTaskWithRetry(task)

				if success {
					log.Printf("Task completed successfully: %s", task.JobName)
					wp.uploadLogToS3(task.JobName, output)
				} else {
					log.Printf("Task failed after retries: %s", task.JobName)
					wp.uploadLogToS3(task.JobName, fmt.Sprintf("Task failed: %s", output))
				}
			}
		}(i)
	}
}

// executeTaskWithRetry tries to execute a task and retries if it fails.
func (wp *WorkerPool) executeTaskWithRetry(task *storage.Task) (bool, string) {
	var output string
	for attempt := 1; attempt <= wp.retryLimit; attempt++ {
		log.Printf("Attempt %d to execute task: %s", attempt, task.JobName)

		out, err := executeCommand(task)
		output = out
		if err == nil {
			return true, output // Task succeeded
		}

		log.Printf("Task failed on attempt %d: %s", attempt, task.JobName)
		wp.logTaskFailure(task.JobName, attempt, err.Error())
		time.Sleep(2 * time.Second) // Backoff before retry
	}

	return false, output // Task failed after retries
}

// logTaskFailure logs task failures with retry details and error messages.
func (wp *WorkerPool) logTaskFailure(taskName string, attempt int, errorMsg string) {
	logContent := fmt.Sprintf("Task: %s\nAttempt: %d\nError: %s\nTimestamp: %s\n\n",
		taskName, attempt, errorMsg, time.Now().Format(time.RFC3339))

	filename := fmt.Sprintf("failed_tasks/%s_%d.log", taskName, attempt)
	wp.s3Logger.UploadLog(filename, logContent)
}

// uploadLogToS3 uploads the task output to S3.
func (wp *WorkerPool) uploadLogToS3(taskName string, output string) {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("logs/%s/%s.log", taskName, timestamp)

	wp.s3Logger.UploadLog(filename, output)
}

// Stop closes the task queue and waits for all workers to complete.
func (wp *WorkerPool) Stop() {
	close(wp.taskQueue)
	wp.wg.Wait()
	log.Println("All workers have completed their tasks.")
}
