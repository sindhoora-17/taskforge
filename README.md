# TaskForge

TaskForge is a distributed background job execution platform written in Go. It accepts jobs through an HTTP API, persists job state in PostgreSQL, distributes work using Redis Streams, and executes jobs across concurrent, horizontally scalable worker processes with retries and crash recovery.

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
- Job lifecycle updates (`queued`, `running`, `retrying`, `completed`, and `failed`)
- Redis message acknowledgements
- JSON report generation
- Configurable concurrent worker pools
- Horizontal worker scaling with Docker Compose
- Redis consumer-group distribution across worker instances
- Multi-stage Docker application builds
- Executor and worker-processing unit tests
- Automatic retries with exponential backoff
- Redis sorted-set scheduling for delayed retries
- Persistent retry errors and next-attempt timestamps
- Worker heartbeats for long-running jobs
- Automatic stale-message recovery after worker crashes
- Redis Streams dead-letter queue for exhausted jobs
- Dead-letter failure metadata including attempts, errors, and original message IDs
- Transactional outbox for reliable PostgreSQL-to-Redis delivery
- Background outbox dispatcher using PostgreSQL row locking
- Persistent exponential backoff for Redis publishing failures
- Automatic delivery recovery after Redis outages
- Atomic PostgreSQL job claiming using compare-and-set updates
- Duplicate-execution protection for at-least-once queue delivery
- Exponential worker backoff during Redis outages
- Automatic worker recovery after Redis becomes available

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

- Scheduled and priority jobs
- Job cancellation and execution timeouts
- Metrics and distributed tracing
- Integration tests with PostgreSQL and Redis
- Load testing and performance benchmarks