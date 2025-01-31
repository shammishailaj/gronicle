# POST /tasks: Add new tasks dynamically.

curl -X POST http://localhost:8080/tasks -d '{"job_name": "New Task", "command": "echo Hello", "interval_seconds": 10}' -H "Content-Type: application/json"

# GET /tasks: Fetch all tasks with details like status, job name, etc.

curl http://localhost:8080/tasks


# GET /tasks/{id}: Fetch details of a specific task.



# DELETE /tasks/{id}: Delete a task.

curl -X DELETE http://localhost:8080/tasks/1


# GET /logs/{task_id}: Fetch logs stored in S3 for specific tasks.