package scheduler

import (
	"database/sql"
	"fmt"
	"github.com/shammishailaj/gronicle/pkg/monitor"
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
func (wp *WorkerPool) Start(db *sql.DB) {
	log.Printf("Starting %d workers...", wp.workerCount)

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)

		go func(workerID int) {
			defer wp.wg.Done()

			for task := range wp.taskQueue {
				log.Printf("Worker %d executing task: %s", workerID, task.JobName)
				success, output := wp.executeTaskWithRetry(db, task)

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

// executeTaskWithRetry tries to execute a task and retries if it fails and logs its execution duration.
func (wp *WorkerPool) executeTaskWithRetry(db *sql.DB, task *storage.Task) (bool, string) {
	startTime := time.Now() // Track start time

	// Collect pre-execution system metrics
	preMetrics := monitor.CollectMetrics()
	log.Printf("Pre-execution metrics for task %d: %+v", task.ID, preMetrics)
	insertTaskMetricsErr := storage.InsertTaskMetrics(db, task.ID, preMetrics)
	if insertTaskMetricsErr != nil {
		log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert task metrics: %s", insertTaskMetricsErr.Error())
	}

	// Attempt to execute the task and track its process
	var (
		output             string
		inExecutionMetrics []monitor.ProcessMetrics
		outputErr          error
	)

	for attempt := 1; attempt <= wp.retryLimit; attempt++ {
		log.Printf("Attempt %d to execute task: %s", attempt, task.JobName)

		// Start the task and get its PID
		output, inExecutionMetrics, outputErr = executeCommand(task)

		// Collect system metrics after execution
		metrics := monitor.CollectMetrics()
		insertTaskMetricsErr = storage.InsertTaskMetrics(db, task.ID, metrics)

		if insertTaskMetricsErr != nil {
			log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert task metrics: %s", insertTaskMetricsErr.Error())
		}

		wp.logTaskDuration(db, task.ID, startTime, time.Now(), "completed")

		if outputErr == nil {
			// Collect post-execution system metrics
			postMetrics := monitor.CollectMetrics()
			log.Printf("Post-execution metrics for task %d: %+v", task.ID, postMetrics)

			// Store in-execution and post-execution metrics
			for _, metric := range inExecutionMetrics {
				insertProcessMetricsErr := storage.InsertProcessTaskMetrics(db, task.ID, metric)
				if insertProcessMetricsErr != nil {
					log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert process metrics: %s", insertProcessMetricsErr.Error())
				}
			}

			insertTaskMetricsErr = storage.InsertTaskMetrics(db, task.ID, postMetrics)
			if insertTaskMetricsErr != nil {
				log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert task metrics: %s", insertTaskMetricsErr.Error())
			}

			wp.logTaskDuration(db, task.ID, startTime, time.Now(), "completed")
			return true, string(output) // Task succeeded
		}

		log.Printf("Task failed on attempt %d: %s, error: %s", attempt, task.JobName, outputErr.Error())

		// Collect post-execution metrics after failure
		postFailureMetrics := monitor.CollectMetrics()
		log.Printf("Post-failure metrics for task %d: %+v", task.ID, postFailureMetrics)

		insertTaskMetricsErr = storage.InsertTaskMetrics(db, task.ID, postFailureMetrics)
		if insertTaskMetricsErr != nil {
			log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert task metrics: %s", insertTaskMetricsErr.Error())
		}

		wp.logTaskDuration(db, task.ID, startTime, time.Now(), "failed")
		wp.logTaskFailure(task.JobName, attempt, outputErr.Error())
		time.Sleep(2 * time.Second) // Backoff before retry
	}

	// Collect post-execution metrics after failure
	postFailureMetrics := monitor.CollectMetrics()
	log.Printf("Post-failure metrics for task %d: %+v", task.ID, postFailureMetrics)
	insertTaskMetricsErr = storage.InsertTaskMetrics(db, task.ID, postFailureMetrics)
	if insertTaskMetricsErr != nil {
		log.Printf("scheduler.WorkerPool.executeTaskWithRetry: failed to insert task metrics: %s", insertTaskMetricsErr.Error())
	}

	wp.logTaskDuration(db, task.ID, startTime, time.Now(), "failed")

	return false, output // Task failed after retries
}

// logTaskFailure logs task failures with retry details and error messages.
func (wp *WorkerPool) logTaskFailure(taskName string, attempt int, errorMsg string) {
	logContent := fmt.Sprintf("Task: %s\nAttempt: %d\nError: %s\nTimestamp: %s\n\n",
		taskName, attempt, errorMsg, time.Now().Format(time.RFC3339))

	filename := fmt.Sprintf("failed_tasks/%s_%d.log", taskName, attempt)
	wp.s3Logger.UploadLog(filename, logContent)
}

// logTaskDuration updates the task's execution time and status in the database.
func (wp *WorkerPool) logTaskDuration(db *sql.DB, taskID int, startTime, endTime time.Time, status string) {
	err := storage.UpdateTaskExecution(db, taskID, startTime, endTime, status)
	if err != nil {
		log.Printf("Failed to log task execution for task %d: %v", taskID, err)
	}
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
