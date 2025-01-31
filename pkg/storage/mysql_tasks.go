package storage

import (
	"database/sql"
	"log"
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
