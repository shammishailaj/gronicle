package scheduler

import (
	"database/sql"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/shammishailaj/gronicle/pkg/storage"
)

type Scheduler struct {
	tasks    []*storage.Task
	taskLock sync.Mutex
	stopChan chan struct{}
	taskWG   sync.WaitGroup
	db       *sql.DB
}

// NewSchedulerWithDB initializes a new scheduler and connects to the database.
func NewSchedulerWithDB(db *sql.DB) *Scheduler {
	return &Scheduler{
		tasks:    make([]*storage.Task, 0),
		stopChan: make(chan struct{}),
		db:       db,
	}
}

// LoadTasksFromDB fetches tasks from MySQL and schedules them.
func (s *Scheduler) LoadTasksFromDB() {
	tasks, err := storage.FetchPendingTasks(s.db)
	if err != nil {
		log.Fatalf("Error fetching tasks from database: %v", err)
	}

	for _, task := range tasks {
		s.AddTask(&task)
	}
}

// AddTask adds a new task to the scheduler.
func (s *Scheduler) AddTask(task *storage.Task) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	s.tasks = append(s.tasks, task)
	log.Printf("Task added: %s, Interval: %v", task.JobName, task.Interval)
}

// Start begins executing tasks.
func (s *Scheduler) Start() {
	log.Println("Starting Scheduler...")

	for _, task := range s.tasks {
		s.taskWG.Add(1)

		go func(task *storage.Task) {
			defer s.taskWG.Done()
			ticker := time.NewTicker(task.Interval)

			for {
				select {
				case <-ticker.C:
					log.Printf("Executing Task: %s", task.JobName)
					if err := executeCommand(task); err != nil {
						log.Printf("Task failed: %s, error: %v", task.JobName, err)
						storage.UpdateTaskStatus(s.db, task.ID, "failed")
					} else {
						storage.UpdateTaskStatus(s.db, task.ID, "completed")
					}
				case <-s.stopChan:
					ticker.Stop()
					log.Printf("Stopping task: %s", task.JobName)
					return
				}
			}
		}(task)
	}
}

// Stop stops all scheduled tasks.
func (s *Scheduler) Stop() {
	log.Println("Stopping Scheduler...")
	close(s.stopChan)
	s.taskWG.Wait()
	log.Println("Scheduler stopped.")
}

// executeCommand runs a system command for the task.
func executeCommand(task *storage.Task) error {
	cmd := exec.Command("/bin/sh", "-c", task.Command)
	output, err := cmd.CombinedOutput()
	log.Printf("Task output [%s]: %s", task.JobName, string(output))
	return err
}
