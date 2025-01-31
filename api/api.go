package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/shammishailaj/gronicle/pkg/storage"
)

// TaskRequest represents a task creation request.
type TaskRequest struct {
	JobName         string `json:"job_name"`
	Command         string `json:"command"`
	IntervalSeconds int    `json:"interval_seconds"`
}

// AddTaskHandler handles POST requests to add a new task.
func AddTaskHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var taskReq TaskRequest

		if err := json.NewDecoder(r.Body).Decode(&taskReq); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		taskID, err := storage.InsertTask(db, taskReq.JobName, taskReq.Command, taskReq.IntervalSeconds)
		if err != nil {
			http.Error(w, "Failed to add task", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"task_id": taskID,
			"message": "Task created successfully",
		})
	}
}

// GetTasksHandler handles GET requests to retrieve all tasks.
func GetTasksHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tasks, err := storage.FetchAllTasks(db)
		if err != nil {
			http.Error(w, "Failed to fetch tasks", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tasks)
	}
}

// GetTaskByIDHandler handles GET requests to fetch a specific task.
func GetTaskByIDHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		taskID, taskIDErr := strconv.Atoi(mux.Vars(r)["id"])
		if taskIDErr != nil {
			http.Error(w, "Invalid task ID", http.StatusBadRequest)
			return
		}

		task, err := storage.FetchTaskByID(db, taskID)
		if err != nil {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(task)
	}
}

// DeleteTaskHandler handles DELETE requests to remove a task.
func DeleteTaskHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		taskID, _ := strconv.Atoi(mux.Vars(r)["id"])

		err := storage.DeleteTask(db, taskID)
		if err != nil {
			http.Error(w, "Failed to delete task", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Task deleted successfully"})
	}
}

// GetTaskLogsHandler handles GET requests to fetch logs for a specific task from S3.
func GetTaskLogsHandler(s3Logger *storage.S3Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		taskID := mux.Vars(r)["task_id"]
		prefix := fmt.Sprintf("logs/%s/", taskID)

		// List objects in S3 with the task-specific prefix
		logFiles, err := s3Logger.ListLogFiles(prefix)
		if err != nil {
			http.Error(w, "Failed to retrieve logs", http.StatusInternalServerError)
			return
		}

		if len(logFiles) == 0 {
			http.Error(w, "No logs found for this task", http.StatusNotFound)
			return
		}

		// Combine log entries into a single response
		var allLogs []string
		for _, file := range logFiles {
			logContent, err := s3Logger.FetchLogContent(file)
			if err != nil {
				log.Printf("Failed to fetch log content from %s: %v", file, err)
				continue
			}
			allLogs = append(allLogs, fmt.Sprintf("Log from %s:\n%s", file, logContent))
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strings.Join(allLogs, "\n\n")))
	}
}

// GetFailedLogsHandler lists locally stored logs for failed tasks.
func GetFailedLogsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		files, err := os.ReadDir("local_logs")
		if err != nil {
			http.Error(w, "Failed to read local logs", http.StatusInternalServerError)
			return
		}

		var logFiles []string
		for _, file := range files {
			logFiles = append(logFiles, file.Name())
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(logFiles)
	}
}

// InitializeRouter sets up API routes.
func InitializeRouter(db *sql.DB, s3Logger *storage.S3Logger) *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/tasks", AddTaskHandler(db)).Methods("POST")
	router.HandleFunc("/tasks", GetTasksHandler(db)).Methods("GET")
	router.HandleFunc("/tasks/{id:[0-9]+}", GetTaskByIDHandler(db)).Methods("GET")
	router.HandleFunc("/tasks/{id:[0-9]+}", DeleteTaskHandler(db)).Methods("DELETE")
	router.HandleFunc("/logs/{task_id}", GetTaskLogsHandler(s3Logger)).Methods("GET")
	router.HandleFunc("/failed_logs", GetFailedLogsHandler()).Methods("GET")

	return router
}
