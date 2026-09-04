# TaskForge

TaskForge is an in-progress distributed job execution platform written in Go. The API accepts background jobs, stores their state in PostgreSQL, and publishes them to Redis Streams for asynchronous processing.

> **Project status:** End-to-end distributed job execution is operational. Automatic retries, crash recovery, and production observability are currently under development.

## Current Features

- Go HTTP API
- Health-check endpoint
- Job submission with request validation
- UUID-based job identifiers
- PostgreSQL job persistence
- PostgreSQL connection pooling with pgx
- Job status retrieval
- Redis Streams job publishing
- Versioned SQL migrations
- Docker Compose development environment
- Automated HTTP handler tests
- Redis Streams consumer-group processing
- Separate API and worker services
- Asynchronous job execution
- Job lifecycle updates (`queued`, `running`, `completed`, and `failed`)
- Redis message acknowledgements
- JSON report generation
- Configurable concurrent worker pools
- Horizontal worker scaling with Docker Compose
- Redis consumer-group distribution across worker instances
- Multi-stage Docker application builds
- Executor and worker-processing unit tests

## Current Flow

```text
                         ┌──> Worker Process 1 ──> Executors
Client ──> API ──> Redis ┤
                         └──> Worker Process 2 ──> Executors
              │                       │
              └──────> PostgreSQL <───┘
```

## API Endpoints

### Check Service Health

```http
GET /health
```

Example response:

```json
{
  "service": "taskforge-api",
  "status": "healthy"
}
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

### 1. Create the environment file

```bash
cp .env.example .env
```

### 2. Start PostgreSQL and Redis

```bash
docker compose up -d
```

Check that both services are healthy:

```bash
docker compose ps
```

### 3. Run the database migration

This is required only when setting up a new database:

```bash
docker compose exec -T postgres psql \
  -U taskforge \
  -d taskforge \
  < migrations/001_create_jobs.up.sql
```

### 4. Start the API

```bash
go run ./cmd/api
```

The API runs at `http://localhost:8080`.

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

- Automatic retries with exponential backoff
- Worker crash recovery
- Scheduled and priority jobs
- Job cancellation and timeouts
- Dead-letter queue
- Metrics and distributed tracing
- Load testing and performance benchmarks