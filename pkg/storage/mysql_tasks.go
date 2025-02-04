package storage

import (
	"database/sql"
	"github.com/shammishailaj/gronicle/pkg/monitor"
	"log"
	"time"
)

// InsertTask inserts a new task into the database.
func InsertTask(db *sql.DB, jobName, command string, intervalSeconds int) (int64, error) {
	query := "INSERT INTO tasks (job_name, command, interval_seconds) VALUES (?, ?, ?)"
	result, err := db.Exec(query, jobName, command, intervalSeconds)
	if err != nil {
		log.Printf("Failed to insert task: %v", err)
		return 0, err
	}
	return result.LastInsertId()
}

// FetchAllTasks retrieves all tasks from the database.
func FetchAllTasks(db *sql.DB) ([]Task, error) {
	query := "SELECT id, job_name, command, interval_seconds, status, created_at FROM tasks"
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Failed to fetch tasks: %v", err)
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		if err := rows.Scan(&task.ID, &task.JobName, &task.Command, &task.Interval, &task.Status, &task.CreatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// FetchTaskByID retrieves a specific task by ID.
func FetchTaskByID(db *sql.DB, id int) (*Task, error) {
	query := "SELECT id, job_name, command, interval_seconds, status, created_at FROM tasks WHERE id = ?"
	var task Task
	err := db.QueryRow(query, id).Scan(&task.ID, &task.JobName, &task.Command, &task.Interval, &task.Status, &task.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, err
	}
	return &task, err
}

// DeleteTask deletes a task by ID.
func DeleteTask(db *sql.DB, id int) error {
	query := "DELETE FROM tasks WHERE id = ?"
	_, err := db.Exec(query, id)
	return err
}

type TaskMetrics struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// FetchTaskMetrics retrieves the count of tasks grouped by their status.
func FetchTaskMetrics(db *sql.DB) ([]TaskMetrics, error) {
	query := `
        SELECT status, COUNT(*) AS count 
        FROM tasks 
        GROUP BY status`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Failed to fetch task metrics: %v", err)
		return nil, err
	}
	defer rows.Close()

	var metrics []TaskMetrics
	for rows.Next() {
		var metric TaskMetrics
		if err := rows.Scan(&metric.Status, &metric.Count); err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

// FetchTaskMetrics retrieves metrics for a specific task from the database.
func FetchTaskMetricsV2(db *sql.DB, taskID int) ([]monitor.TaskMetrics, error) {
	query := `SELECT cpu_usage, ram_usage, disk_usage, load_average, gpu_usage, recorded_at 
        FROM task_metrics 
        WHERE task_id = ?`

	rows, err := db.Query(query, taskID)
	if err != nil {
		log.Printf("Failed to fetch metrics for task %d: %v", taskID, err)
		return nil, err
	}
	defer rows.Close()

	var metrics []monitor.TaskMetrics
	for rows.Next() {
		var metric monitor.TaskMetrics
		if err := rows.Scan(&metric.CPUUsage, &metric.RAMUsage, &metric.DiskUsage, &metric.LoadAverage, &metric.GPUUsage, &metric.RecordedAt); err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

// UpdateTaskExecution updates the task's execution timestamps and status.
func UpdateTaskExecution(db *sql.DB, taskID int, startTime, endTime time.Time, status string) error {
	query := `UPDATE tasks 
        SET start_time = ?, end_time = ?, status = ? 
        WHERE id = ?`

	_, err := db.Exec(query, startTime, endTime, status, taskID)
	if err != nil {
		log.Printf("Failed to update task execution: %v", err)
	}
	return err
}

// FetchEnhancedMetrics retrieves average task duration and failures in the last 24 hours.
func FetchEnhancedMetrics(db *sql.DB) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// Average task duration for completed tasks
	queryAvgDuration := `
        SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) 
        FROM tasks 
        WHERE status = 'completed' AND start_time IS NOT NULL AND end_time IS NOT NULL`

	var avgDuration float64
	if err := db.QueryRow(queryAvgDuration).Scan(&avgDuration); err != nil {
		log.Printf("Failed to fetch average task duration: %v", err)
		return nil, err
	}
	metrics["average_task_duration"] = avgDuration

	// Count of failed tasks in the last 24 hours
	queryFailures := `
        SELECT COUNT(*) 
        FROM tasks 
        WHERE status = 'failed' 
        AND start_time >= NOW() - INTERVAL 24 HOUR`

	var failureCount int
	if err := db.QueryRow(queryFailures).Scan(&failureCount); err != nil {
		log.Printf("Failed to fetch task failures: %v", err)
		return nil, err
	}
	metrics["failures_last_24_hours"] = failureCount

	return metrics, nil
}

// InsertTaskMetrics stores the collected metrics for a task execution.
func InsertTaskMetrics(db *sql.DB, taskID int, metrics monitor.TaskMetrics) error {
	query := `
        INSERT INTO task_metrics (task_id, cpu_usage, ram_usage, disk_usage, load_average, gpu_usage, recorded_at) 
        VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err := db.Exec(query, taskID, metrics.CPUUsage, metrics.RAMUsage, metrics.DiskUsage, metrics.LoadAverage, metrics.GPUUsage, metrics.RecordedAt)
	if err != nil {
		log.Printf("Failed to insert task metrics for task %d: %v", taskID, err)
	}
	return err
}

// InsertProcessTaskMetrics stores per-process metrics for a task execution.
func InsertProcessTaskMetrics(db *sql.DB, taskID int, metrics monitor.ProcessMetrics) error {
	query := `INSERT INTO task_metrics (task_id, cpu_usage, ram_usage, disk_usage, recorded_at) 
        VALUES (?, ?, ?, ?, ?)`

	_, err := db.Exec(query, taskID, metrics.CPUUsage, metrics.RAMUsage, metrics.DiskUsage, metrics.RecordedAt)
	if err != nil {
		log.Printf("Failed to insert process metrics for task %d: %v", taskID, err)
	}
	return err
}
