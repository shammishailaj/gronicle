package scheduler

import (
	"log"
	"os/exec"

	"github.com/shammishailaj/gronicle/pkg/storage"
)

// executeCommand runs a system command for the task.
func executeCommand(task *storage.Task) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", task.Command)

	// Capture the output
	output, err := cmd.CombinedOutput()
	log.Printf("Task output [%s]: %s", task.JobName, string(output))

	return string(output), err
}
