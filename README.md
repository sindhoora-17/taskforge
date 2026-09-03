# TaskForge

TaskForge is a distributed job execution platform written in Go. It accepts background jobs through an HTTP API and will eventually distribute them across worker processes using Redis while storing job state in PostgreSQL.

## Planned Features

- Redis Streams-backed job queue
- Concurrent worker pools
- Multiple distributed worker instances
- Automatic retries with exponential backoff
- Worker crash recovery
- Scheduled and priority jobs
- Job cancellation and timeouts
- Dead-letter queue
- Metrics and distributed tracing
- Load testing and performance benchmarks

## API Endpoints

### Check Service Health

```http
GET /health
```

### Submit a Job

```http
POST /jobs
Content-Type: application/json
```

Example request:

```json
{
  "type": "generate_report",
  "payload": {
    "report_name": "monthly-sales"
  },
  "max_attempts": 3
}
```

Example response:

```json
{
  "id": "f6cb76fe-7fcf-48fd-b2fe-7661a84503e0",
  "type": "generate_report",
  "payload": {
    "report_name": "monthly-sales"
  },
  "status": "queued",
  "attempts": 0,
  "max_attempts": 3,
  "created_at": "2026-09-03T03:54:39Z",
  "updated_at": "2026-09-03T03:54:39Z"
}
```

### Retrieve a Job

```http
GET /jobs/{id}
```

## Run Locally

Start the API:

```bash
go run ./cmd/api
```

The API runs at:

```text
http://localhost:8080
```

Test the health endpoint:

```bash
curl http://localhost:8080/health
```

## Run Tests

```bash
go test ./...
```

To display individual test results:

```bash
go test -v ./...
```

## Planned Features

- PostgreSQL persistence
- Redis-backed job queues
- Concurrent worker processes
- Automatic retries with exponential backoff
- Scheduled and priority jobs
- Worker heartbeats and failure recovery
- Job cancellation and timeouts
- Dead-letter queues
- Metrics and distributed tracing
- Load testing and performance benchmarks