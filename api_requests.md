# POST /tasks: Add new tasks dynamically.

curl -X POST http://localhost:8080/tasks -d '{"job_name": "New Task", "command": "echo Hello", "interval_seconds": 10}' -H "Content-Type: application/json"



# GET /tasks: Fetch all tasks with details like status, job name, etc.

curl http://localhost:8080/tasks



# GET /tasks/{id}: Fetch details of a specific task.

curl http://localhost:9999/tasks/2



# DELETE /tasks/{id}: Delete a task.

curl -X DELETE http://localhost:8080/tasks/1



# GET /logs/{task_id}: Fetch logs stored in S3 for specific tasks.

curl http://localhost:9999/logs/2


# Retrieve locally stored logs (if S3 failed):

curl http://localhost:9999/failed_logs

# Request the tasks metrics:

curl http://localhost:9999/metrics

# Request the enhanced metrics:

curl http://localhost:9999/metrics/enhanced


# Retrieve the task metrics:

curl http://localhost:8080/tasks/15/metrics

