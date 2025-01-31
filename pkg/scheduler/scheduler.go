package scheduler

import (
	"log"
	"sync"
	"time"
)

// Task represents a single task with an ID, name, and the function to execute.
type Task struct {
	ID       int
	Name     string
	Execute  func() error  // Function to execute the task
	Interval time.Duration // How often the task should run
}

// Scheduler manages a set of tasks and executes them at defined intervals.
type Scheduler struct {
	tasks    []*Task
	taskLock sync.Mutex
	stopChan chan struct{} // Channel to stop the scheduler
	taskWG   sync.WaitGroup
}

// NewScheduler initializes a new scheduler.
func NewScheduler() *Scheduler {
	return &Scheduler{
		tasks:    make([]*Task, 0),
		stopChan: make(chan struct{}),
	}
}

// AddTask adds a new task to the scheduler.
func (s *Scheduler) AddTask(task *Task) {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks = append(s.tasks, task)
	log.Printf("Task added: %s, Interval: %v", task.Name, task.Interval)
}

// Start begins the task execution.
func (s *Scheduler) Start() {
	log.Println("Starting Scheduler...")

	for _, task := range s.tasks {
		s.taskWG.Add(1)

		// Launch a goroutine for each task
		go func(task *Task) {
			defer s.taskWG.Done()
			ticker := time.NewTicker(task.Interval)

			for {
				select {
				case <-ticker.C:
					log.Printf("Executing Task: %s", task.Name)
					err := task.Execute()
					if err != nil {
						log.Printf("Error executing task %s: %v", task.Name, err)
					}
				case <-s.stopChan:
					ticker.Stop()
					log.Printf("Stopping task: %s", task.Name)
					return
				}
			}
		}(task)
	}
}

// Stop signals the scheduler to stop running tasks.
func (s *Scheduler) Stop() {
	log.Println("Stopping Scheduler...")
	close(s.stopChan)
	s.taskWG.Wait() // Wait for all tasks to finish
	log.Println("Scheduler stopped.")
}
