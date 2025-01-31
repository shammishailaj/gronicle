package storage

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Task represents a task from the database.
type Task struct {
	ID        int           `json:"id"`
	JobName   string        `json:"job_name"`
	Command   string        `json:"command"`
	Interval  time.Duration `json:"interval_seconds"`
	Status    string        `json:"status"`
	CreatedAt string        `json:"created_at"`
}

// ConnectMySQL connects to the MySQL database
func ConnectMySQL(user, password, host, dbName string) *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, password, host, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not connect to MySQL: %v", err)
	}
	return db
}

// FetchPendingTasks fetches tasks from the database with 'pending' or 'running' status.
func FetchPendingTasks(db *sql.DB) ([]Task, error) {
	query := `
        SELECT id, job_name, command, interval_seconds 
        FROM tasks 
        WHERE status IN ('pending', 'running')`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		var intervalSeconds int

		if err := rows.Scan(&task.ID, &task.JobName, &task.Command, &intervalSeconds); err != nil {
			return nil, err
		}

		task.Interval = time.Duration(intervalSeconds) * time.Second
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// UpdateTaskStatus updates the status of a task (e.g., after execution).
func UpdateTaskStatus(db *sql.DB, taskID int, status string) error {
	query := `UPDATE tasks SET status = ?, updated_at = NOW() WHERE id = ?`
	_, err := db.Exec(query, status, taskID)
	return err
}
