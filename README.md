# Real-Time Group Chat with Dynamic Load Balancing and Persistent Storage

**Author:** Agastya Nath (Roll No. 12340140)  
**Load Balancer URL:** http://10.1.75.51:7265  
**API Endpoints:** `/message`, `/feed`  

A distributed, real-time group chat application built for Lab 6 (Dynamic Load Balancing and Persistent Chat). Clients communicate over WebSockets with any of six independent backend processes; a Go load balancer distributes HTTP traffic across the backends based on live CPU/memory/in-flight load, Redis fans messages out between backends, and PostgreSQL persists every message with duplicate-safe writes.

## Architecture

```
                    +----------------------+
                    |    Load Generator    |
                    |   (multiple users)   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Load Balancer (Go) |
                    |  :7265 (public port)  |
                    +----------+-----------+
                               |
        +---------+---------+-----+---------+---------+
        |         |         |     |         |         |
        v         v         v     v         v         v
    Backend1  Backend2  Backend3 ... Backend5   Backend6
        |         |         |     |         |         |
        +---------+---------+-----+---------+---------+
                               |
                        +------+------+
                        |    Redis    |  (pub/sub message fan-out)
                        +------+------+
                               |
                        +------+------+
                        | PostgreSQL  |  (persistent storage)
                        +-------------+
```

### Components

- **Load Balancer** (`main.go`) — Go reverse proxy exposing `/message` and `/feed` on a single public port. Selects backends dynamically using live CPU/memory/in-flight metrics, retries transient backend failures on a different backend, and marks backends unhealthy after consecutive failures.
- **Backend** (`server.py`) — Python process handling both a WebSocket chat endpoint and an HTTP API (`/message`, `/feed`, `/health`). Each backend independently verifies message signatures, encrypts message content, publishes to Redis, and persists to PostgreSQL.
- **Redis** — Pub/sub channel (`chat_messages`) so a user connected to Backend 1 still receives messages sent via Backend 5.
- **PostgreSQL** — Durable storage for users' public keys and messages, accessed through a pooled connection (`ThreadedConnectionPool`, 10–50 connections).
- **Load Generator** (`load_generator.py`) — Custom Python load-testing tool supporting variable user counts, message lengths, and send intervals, with per-backend CPU/memory/persistence monitoring.

## Backend Details

### Message flow (`POST /message`)

1. Receive `client-name` and `msg` from the request.
2. Validate the payload and look up the sender's public key (in-memory cache, falling back to PostgreSQL on a cache miss).
3. Verify the message signature.
4. Encrypt the message content (AES-256-GCM).
5. Publish the encrypted message to the Redis `chat_messages` channel.
6. Persist the message to PostgreSQL, tracking total/success/failed persistence counts.
7. Broadcast the message to connected WebSocket clients.

Each message is assigned a UUID (`uuid.uuid4()`) as its unique ID. The `messages` table uses this UUID as its primary key with `ON CONFLICT (id) DO NOTHING`, so a message retried or resubmitted with the same ID (due to client retries, reconnects, or load-balancer failover) is inserted at most once.

```sql
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY,
    username TEXT NOT NULL,
    public_key TEXT NOT NULL,
    ciphertext BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    signature BYTEA NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
)
```

### Public-key caching

A naive implementation would query PostgreSQL on every message to fetch the sender's public key. Instead, keys are cached in memory (`public_key_cache`) after first lookup, protected by a lock (`public_key_cache_lock`) since `/message` requests are handled concurrently. Locks are held only for the cache read/write itself — never across a database or Redis call — to avoid serializing unrelated requests.

### Health endpoint

Each backend exposes `/health`, reporting CPU%, memory%, and cumulative persistence counters (`persistence_total`, `persistence_success`, `persistence_failed`). The load balancer polls this every second per backend to drive routing decisions and failure detection.

## Load Balancer Details

### Dynamic backend selection

Rather than round-robin, each request is routed to the backend with the lowest weighted load score:

```
score = 0.7 * cpu_percent + 0.2 * memory_percent + 0.1 * normalized_in_flight
```

Backends at or above the configured CPU threshold are avoided unless every alive backend is over threshold, in which case the least-loaded backend is used anyway rather than rejecting the request outright.

### Health detection

A background goroutine per backend polls `GET <api_url>/health` every second. A backend is marked unhealthy after **3 consecutive** failed health checks or proxy errors, and marked healthy again once a health check succeeds — preventing a single transient blip from pulling a backend out of rotation while still reacting to sustained outages.

### Retry-on-failure

If a proxy attempt to a backend fails at the network level (dial failure, timeout, connection reset) before any response bytes have reached the client, the load balancer automatically retries the request against a different healthy backend (up to `maxAttempts`) rather than immediately returning an error. The request body is buffered once per incoming request so it can be safely replayed across attempts. If a response has already begun streaming to the client, the load balancer will not retry (to avoid sending a corrupted second response) — it simply reports the failure.

Genuine application-level errors returned by a backend (e.g. HTTP 500) are passed through to the client as-is and are **not** retried, since retrying an application error typically reproduces the same failure and does not represent a transport-layer problem.

### Endpoints

| Endpoint      | Method | Description                                                          |
|---------------|--------|------------------------------------------------------------------------|
| `/message`    | POST   | Submit a chat message (`client-name`, `msg`) — routed to a backend |
| `/feed`       | GET    | Retrieve all persisted messages                                     |
| `/lb/health`  | GET    | Load balancer's own liveness check                                  |
| `/lb/status`  | GET    | Per-backend alive/CPU/memory/in-flight status                       |
| `/lb/metrics` | GET    | Aggregate request totals, success/fail counts, and latency percentiles (p50/p95/p99) |

## Running It

### 1. Start the backends

```bash
python3 server.py --port 6000 --api_port 5000 --name backend1
```

Run one instance per backend, each with a distinct `--port` (WebSocket) and `--api_port` (HTTP API). Repeat for as many backends as desired (this deployment uses six).

Each backend expects:
- A running Redis instance (host/port configured at the top of `server.py`)
- A running PostgreSQL instance with a database/user matching the configured `DB_NAME`/`DB_USER`/`DB_PASSWORD`
- TLS certificate/key files for the WebSocket listener

### 2. Build and start the load balancer

```bash
go build -o loadbalancer main.go

./loadbalancer \
  -backends "<WS_URL_1>,<API_URL_1>;<WS_URL_2>,<API_URL_2>;..." \
  -threshold 0.60
```

Example:

```bash
./loadbalancer \
  -backends "https://10.1.75.51:4266,http://10.1.75.51:3266;https://10.1.75.51:6266,http://10.1.75.51:5266;https://10.1.75.51:4267,http://10.1.75.51:3267;https://10.1.75.51:6267,http://10.1.75.51:5267;https://10.1.75.51:4268,http://10.1.75.51:3268;https://10.1.75.51:6268,http://10.1.75.51:5268" \
  -threshold 0.60
```

`-threshold` is the CPU fraction (0.0–1.0) above which the load balancer prefers routing to a different backend. This deployment uses **0.60** (60%) as the tuned operating threshold.

The load balancer listens on `:7000` by default (deployed here behind port **7265**).

### 3. Run the load generator

```bash
python3 load_generator.py \
  --url http://<load-balancer-host>:<port>/message \
  --users 100 \
  --duration 60 \
  --min-length 50 \
  --max-length 500 \
  --min-interval 2 \
  --max-interval 3
```

This produces:
- `latency.csv` — per-request timestamp, user ID, response time, and status (HTTP code, timeout, or connection error)
- `utilization.csv` — per-second CPU%, memory%, and persistence total/success/failed for each backend

### 4. Generate graphs

```bash
python3 visualize.py
```

Produces plots (stored under `graphs/`) for CPU utilization over time, memory utilization over time, persistence operations over time, persistence success/failure, response time over time, and response time distribution — one set per load test run.

## Load Testing Results

All tests below used: `--duration 60 --min-length 50 --max-length 500 --min-interval 2 --max-interval 3`, CPU threshold = 60%.

| Users | Requests | Success Rate | Avg Latency | P50 | P95 | P99 |
|------:|---------:|-------------:|------------:|----:|----:|----:|
| 1     | 48       | 100%          | 30 ms       | 29 ms | 43 ms | 44 ms |
| 20    | 477      | 100%          | 48 ms       | 36 ms | 95 ms | 215 ms |
| 100   | 2,367    | 100%          | 81 ms       | 60 ms | 201 ms | 929 ms |
| 200   | 4,538    | 100%          | 201 ms      | 109 ms | 519 ms | 2,264 ms |
| 600   | 7,453    | 95.6%         | 2,435 ms    | 1,695 ms | 7,184 ms | 9,030 ms |
| 1,000 | 12,162   | 92.3%         | 2,462 ms    | 2,066 ms | 7,191 ms | 8,623 ms |

**Summary of operating regions:**
- **Low load (1–20 users):** 100% success, latency in the tens of milliseconds — negligible contention.
- **Moderate load (100–200 users):** 100% success from the client's perspective, but P99 latency climbs into the seconds — CPU utilization becomes significant even while requests still complete.
- **Extreme load (600–1,000 users):** Success rate drops to 92–96%, P95/P99 latency reaches 7–9 seconds — the system is saturated. CPU on multiple backends repeatedly hits 100%, while memory stays flat around 21–22% throughout, confirming CPU (not memory) as the binding resource.

Backend-side persistence metrics recorded `persistence_success ≈ persistence_total` with `persistence_failed = 0` across all runs, including under extreme load — demonstrating that **request-level failure and persistence failure are distinct phenomena**: a client-visible failure does not necessarily mean the corresponding write was lost, and a message that does reach a backend is written durably.

## Known Bottlenecks and Limitations

Diagnosed during load testing and worth documenting for anyone extending this project:

- **Redis connection pool exhaustion** — under high concurrency, backends can raise `PoolError('connection pool exhausted')` when demand for Redis connections exceeds the configured pool size. This is a resource-sizing issue, not a Redis outage.
- **Load-generator file descriptor limits** — at very high concurrency (600+ simulated users), the load generator itself can hit `[Errno 24] Too many open files`, meaning some "failures" originate from the *test client's* OS limits rather than the backend. Raise `ulimit -n` on the load-generator host before high-concurrency runs.
- **Health-check timeouts under load** — when a backend is heavily loaded, `/health` requests can time out even though the backend is technically reachable; this is recorded as a missing (not zero) value in monitoring data, since a timeout does not mean 0% utilization.
- **CPU is the dominant bottleneck** — memory remained stable (~21–22%) across all tested loads; CPU saturation on backend processes is what limits throughput at scale.
- **HTTP keep-alive matters** — the backend HTTP handler uses `protocol_version = "HTTP/1.1"` so the load balancer's connection pool can reuse TCP connections instead of paying a full handshake per request.

