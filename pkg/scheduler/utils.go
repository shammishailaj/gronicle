package scheduler

import (
	"github.com/shammishailaj/gronicle/pkg/monitor"
	"github.com/shammishailaj/gronicle/pkg/storage"
	"log"
	"os/exec"
	"time"
)

// executeCommand runs a system command for the task.
func executeCommand(task *storage.Task) (string, []monitor.ProcessMetrics, error) {
	var (
		output             []byte
		inExecutionMetrics []monitor.ProcessMetrics
		outputErr          error
	)
	cmd := exec.Command("/bin/sh", "-c", task.Command)

	cmdStartErr := cmd.Start()
	if cmdStartErr != nil {
		log.Printf("Failed to start task: %s, error: %s", task.JobName, cmdStartErr.Error())
		return string(output), inExecutionMetrics, cmdStartErr
	}
	taskPID := cmd.Process.Pid
	log.Printf("scheduler.utils.executeCommand: Task %s started with PID: %d", task.JobName, taskPID)

	// Periodically collect per-process metrics while the task is running
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				metric := monitor.CollectProcessMetrics(int32(taskPID))
				inExecutionMetrics = append(inExecutionMetrics, metric)
				time.Sleep(1 * time.Second) // Collect metrics every second
			}
		}
	}()

	// Wait for the task to complete and stop collecting metrics
	err := cmd.Wait()
	if err != nil {
		log.Printf("scheduler.utils.executeCommand: Task %s exited with error: %s", task.JobName, err.Error())
	}
	close(done) // Stop the goroutine collecting metrics
	// Capture the output
	output, outputErr = cmd.CombinedOutput()
	log.Printf("scheduler.utils.executeCommand: Task output [PID:%d][%s]:\n\n\n%s", taskPID, task.JobName, string(output))

	return string(output), inExecutionMetrics, outputErr
}
