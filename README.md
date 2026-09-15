# Real-Time Group Chat with Load Balancing and Multiple Persistence

A distributed real-time group chat application. Clients connect over WebSockets to one of six
backend processes spread across three machines. Redis pub/sub distributes messages between the
backends, and each machine persists every message into its own PostgreSQL database. A custom Go
load balancer distributes HTTP API traffic across the backends using live CPU, memory, and
in-flight-request measurements.

**Author:** Agastya Nath (Roll No. 12340140)

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Repository Layout](#2-repository-layout)
3. [Deployment Topology](#3-deployment-topology)
4. [Prerequisites](#4-prerequisites)
5. [Installing Go](#5-installing-go)
6. [PostgreSQL Setup](#6-postgresql-setup)
7. [Redis Setup](#7-redis-setup)
8. [TLS Certificates](#8-tls-certificates)
9. [Running the Backends](#9-running-the-backends)
10. [Building and Running the Load Balancer](#10-building-and-running-the-load-balancer)
11. [Running the Frontend](#11-running-the-frontend)
12. [API Reference](#12-api-reference)
13. [Load Testing](#13-load-testing)
14. [Generating Graphs](#14-generating-graphs)
15. [Configuration Reference](#15-configuration-reference)
16. [Troubleshooting](#16-troubleshooting)

---

## 1. Architecture

```
                 +----------------------+
                 |   Load Generator     |
                 |   (N simulated users)|
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 |    Load Balancer     |   Go, port 7265
                 |  CPU/memory scoring  |
                 +----------+-----------+
                            |
        +---------+---------+---------+---------+---------+
        v         v         v         v         v         v
    Backend1  Backend2  Backend3  Backend4  Backend5  Backend6
        |         |         |         |         |         |
     [ Machine 1 ]       [ Machine 2 ]       [ Machine 3 ]
     PostgreSQL #1        PostgreSQL #2       PostgreSQL #3
        |         |         |         |         |         |
        +---------+---------+----+----+---------+---------+
                                 |
                          +------+------+
                          |    Redis    |   channel: chat_messages
                          +-------------+
```

Message flow for a WebSocket chat message:

1. The browser signs the plaintext with an RSA-PSS private key generated in-page and sends
   `{type: "chat", content, signature}` to its backend.
2. The backend verifies the signature against the sender's public key, read from the in-process
   `public_keys` map that was populated during registration.
3. The backend encrypts the plaintext with AES-256-GCM and publishes the ciphertext, nonce,
   signature, and metadata to the Redis `chat_messages` channel.
4. **Every** backend receives the published payload, writes it into *its own* PostgreSQL
   database, decrypts it, and broadcasts the plaintext to its locally connected clients.

This is why a user on Backend 1 can talk to a user on Backend 5, and why all three databases hold
the same message set.

`POST /message` follows the same path but skips signature verification (the payload is tagged
`"source": "api"`), which is what makes it usable for load testing. It sends an empty
`public_key` and empty `signature`, so the API route does no asymmetric cryptography at all — the
CPU cost measured under load is AES-GCM encryption, JSON handling, the Redis publish, and the
PostgreSQL insert performed by all six subscribers.

Two lookup structures exist for public keys. `public_keys` is the one actually consulted when
verifying a WebSocket message; it is populated at registration and cleared on disconnect.
`public_key_cache` (guarded by `public_key_cache_lock`) is also written at registration and is read
by `load_public_key()`, which falls back to a `SELECT` on the `users` table and caches the result.
Neither message path currently calls `load_public_key()`, so the cache-with-database-fallback is in
place but not exercised on the hot path as the code stands.

---

## 2. Repository Layout

```
chat_app_load_balancing/
├── backend/
│   └── server.py                         # WebSocket + HTTP API backend
├── load-balancer/
│   └── main.go                           # Go load balancer
├── python-message-load-generator/
│   ├── load_generator.py                 # Multi-threaded load generator
│   ├── visualize.py                      # Graph generation from CSV output
│   └── graphs/                           # Generated PNGs (created on first run)
└── chat-frontend/
    ├── src/
    │   └── App.jsx                       # React chat client
    ├── package.json
    └── vite.config.js
```

| File | Role |
|---|---|
| `chat_app_load_balancing/backend/server.py` | One backend process: WSS server, HTTP API, Redis subscriber, PostgreSQL writer |
| `chat_app_load_balancing/load-balancer/main.go` | Load balancer with health polling, CPU-aware selection, and retries |
| `chat_app_load_balancing/python-message-load-generator/load_generator.py` | Simulates N concurrent users; writes `latency.csv` and `utilization.csv` |
| `chat_app_load_balancing/python-message-load-generator/visualize.py` | Turns those CSVs into the report graphs |
| `chat_app_load_balancing/chat-frontend/src/App.jsx` | React/Vite client: key generation, signing, chat UI |

---

## 3. Deployment Topology

Four machines are used.

| Machine | Runs | Components |
|---|---|---|
| 1 | Backend 1, Backend 2 | `server.py` ×2, local PostgreSQL |
| 2 | Backend 3, Backend 4 | `server.py` ×2, local PostgreSQL |
| 3 | Backend 5, Backend 6 | `server.py` ×2, local PostgreSQL |
| 4 | Load balancer, Redis | `main.go`, Redis server |

Each of the three backend machines runs its **own** PostgreSQL instance on `localhost:5432`. The
backends never talk to a remote database — they only write locally. The fourth machine hosts
Redis (port `4265`) and the Go load balancer (port `7265`).

Because both backends on a machine use `DB_HOST = "localhost"` and `DB_NAME = "chatdb"`, the two
processes share a single database. Every backend subscribes to Redis and writes every message, so
each message is inserted twice per machine. The `ON CONFLICT (id) DO NOTHING` clause in
`store_message()` makes the second insert a no-op, so the table holds exactly one row per message.

One consequence for the metrics: `persistence_total` is a per-process counter incremented on every
attempted insert, including the deduplicated ones. Summing it across all six backends gives roughly
six times the number of distinct messages, not the row count of any database.

Port assignment used in the reference deployment (all on `10.1.75.51` in the lab environment,
which multiplexes the machines onto one address; substitute per-machine IPs if your machines have
distinct addresses):

| Backend | Machine | WebSocket (WSS) | HTTP API |
|---|---|---|---|
| 1 | 1 | 4266 | 3266 |
| 2 | 1 | 6266 | 5266 |
| 3 | 2 | 4267 | 3267 |
| 4 | 2 | 6267 | 5267 |
| 5 | 3 | 4268 | 3268 |
| 6 | 3 | 6268 | 5268 |

---

## 4. Prerequisites

**On each backend machine (1–3):**

- Python 3.10 or newer
- PostgreSQL 13+
- A TLS certificate and key (see [§8](#8-tls-certificates))

Install the Python dependencies:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv postgresql

python3 -m venv ~/chatenv
source ~/chatenv/bin/activate

pip install \
    websockets \
    psycopg2-binary \
    redis \
    cryptography \
    psutil
```

**On the load-balancer machine (4):**

- Go 1.21 or newer (see [§5](#5-installing-go))
- Redis server

**On whichever machine runs the load tests:**

```bash
pip install requests urllib3 pandas matplotlib
```

**For the frontend:**

- Node.js 18+ and npm

---

## 5. Installing Go

The `apt` version of Go is often several releases behind. Install from the official tarball:

```bash
# Download (adjust version/architecture as needed)
wget https://go.dev/dl/go1.22.5.linux-amd64.tar.gz

# Remove any previous installation and extract
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz

# Add Go to PATH permanently
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# Verify
go version
```

Expected output resembles `go version go1.22.5 linux/amd64`.

If you prefer the distribution package:

```bash
sudo apt install -y golang-go
```

---

## 6. PostgreSQL Setup

Run this on **each** of the three backend machines — every machine needs its own database.

```bash
sudo systemctl enable --now postgresql

sudo -u postgres psql
```

Inside `psql`:

```sql
CREATE DATABASE chatdb;
CREATE USER chatuser WITH PASSWORD 'agastya';
GRANT ALL PRIVILEGES ON DATABASE chatdb TO chatuser;
\c chatdb
GRANT ALL ON SCHEMA public TO chatuser;
\q
```

The `users` and `messages` tables are created automatically by `init_db()` in
`chat_app_load_balancing/backend/server.py` the first time a backend starts, so no manual schema
work is needed.

Schema created:

```sql
CREATE TABLE users (
    username   TEXT PRIMARY KEY,
    public_key TEXT NOT NULL
);

CREATE TABLE messages (
    id         UUID PRIMARY KEY,
    username   TEXT NOT NULL,
    public_key TEXT NOT NULL,
    ciphertext BYTEA NOT NULL,
    nonce      BYTEA NOT NULL,
    signature  BYTEA NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL
);
```

Credentials are set at the top of `chat_app_load_balancing/backend/server.py`:

```python
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "chatdb"
DB_USER = "chatuser"
DB_PASSWORD = "agastya"
```

Change `DB_PASSWORD` to something private before making the repository public, and prefer reading
it from an environment variable rather than hard-coding it.

---

## 7. Redis Setup

On machine 4:

```bash
sudo apt install -y redis-server
```

Edit `/etc/redis/redis.conf` so the backends on the other machines can reach it:

```conf
port 4265
bind 0.0.0.0
protected-mode no
```

Then:

```bash
sudo systemctl restart redis-server
redis-cli -p 4265 ping     # expect: PONG
```

The backends connect using the constants in
`chat_app_load_balancing/backend/server.py`:

```python
REDIS_HOST = "10.1.75.51"
REDIS_PORT = 4265
```

Update `REDIS_HOST` to the address of machine 4 in your deployment.

Because every backend publishes and subscribes on the same connection pool, raise the Redis
client pool size if you plan to test above a few hundred concurrent users — pool exhaustion was
the first resource limit hit during testing.

---

## 8. TLS Certificates

The WebSocket server runs over WSS, so each backend machine needs a certificate and key. For a
lab deployment a self-signed pair is sufficient:

```bash
mkdir -p /home/student/chat-ssl
cd /home/student/chat-ssl

openssl req -x509 -newkey rsa:4096 -nodes \
    -keyout key.pem \
    -out cert.pem \
    -days 365 \
    -subj "/CN=10.1.75.51"
```

`chat_app_load_balancing/backend/server.py` loads them from hard-coded paths:

```python
ssl_context.load_cert_chain(
    "/home/student/chat-ssl/cert.pem",
    "/home/student/chat-ssl/key.pem",
)
```

Adjust those paths if you place the files elsewhere.

Browsers reject self-signed certificates on WSS connections by default. Visit
`https://<host>:<ws-port>` once and accept the warning before connecting from the frontend, or
the WebSocket handshake will fail silently.

---

## 9. Running the Backends

### The shared AES key

On first start, each backend generates `encryption.key` (AES-256) next to
`chat_app_load_balancing/backend/server.py` if the file does not already exist.

**All six backends must use the identical key file.** Messages are encrypted by whichever backend
receives them and decrypted by every other backend after the Redis fan-out. If the keys differ,
backends will fail to decrypt each other's messages and `/feed` will return incomplete results.

Generate the key once, then copy it to the other machines:

```bash
# On machine 1, after the first backend has started once:
scp chat_app_load_balancing/backend/encryption.key \
    student@<machine2>:~/chat_app_load_balancing/backend/
scp chat_app_load_balancing/backend/encryption.key \
    student@<machine3>:~/chat_app_load_balancing/backend/
```

`encryption.key` should be listed in `.gitignore` — it is a secret, not source.

### Starting the processes

Each machine runs two backends. Arguments:

| Flag | Default | Meaning |
|---|---|---|
| `--port` | 6000 | WebSocket (WSS) listen port |
| `--api_port` | 5000 | HTTP API port (`/message`, `/feed`, `/health`) |
| `--name` | `backend` | Label used in logs and `/health` responses |
| `--failure-rate` | 0.0 | Probability (0.0–1.0) of returning a synthetic 503 |
| `--delay-ms` | 0 | Synthetic delay injected before registration |

**Machine 1:**

```bash
source ~/chatenv/bin/activate

python3 chat_app_load_balancing/backend/server.py \
    --port 4266 --api_port 3266 --name backend1 &

python3 chat_app_load_balancing/backend/server.py \
    --port 6266 --api_port 5266 --name backend2 &
```

**Machine 2:**

```bash
python3 chat_app_load_balancing/backend/server.py \
    --port 4267 --api_port 3267 --name backend3 &

python3 chat_app_load_balancing/backend/server.py \
    --port 6267 --api_port 5267 --name backend4 &
```

**Machine 3:**

```bash
python3 chat_app_load_balancing/backend/server.py \
    --port 4268 --api_port 3268 --name backend5 &

python3 chat_app_load_balancing/backend/server.py \
    --port 6268 --api_port 5268 --name backend6 &
```

Confirm each one is up:

```bash
curl http://10.1.75.51:3266/health
```

```json
{
  "status": "ok",
  "backend": "backend1",
  "cpu_percent": 3.81,
  "memory_percent": 21.1,
  "persistence_total": 52175,
  "persistence_success": 52175,
  "persistence_failed": 0
}
```

`cpu_percent` is measured against the **cgroup CPU quota**, not the host's total CPU. 100% means
the process group has consumed its full allocation. If no cgroup limit is set, it falls back to
`psutil.cpu_percent()`.

### Simulating failures

```bash
# 10% of WebSocket handshakes return 503
python3 chat_app_load_balancing/backend/server.py \
    --port 4266 --api_port 3266 --name backend1 --failure-rate 0.1
```

A single connection can also be failed on demand by appending `?fail=true` to the WebSocket URL.

---

## 10. Building and Running the Load Balancer

On machine 4:

```bash
cd chat_app_load_balancing/load-balancer

go build -o loadbalancer main.go
```

The `-backends` flag takes semicolon-separated backend entries; each entry is
`<WEBSOCKET_URL>,<HTTP_API_URL>`.

```bash
./loadbalancer -backends \
"https://10.1.75.51:4266,http://10.1.75.51:3266;\
https://10.1.75.51:6266,http://10.1.75.51:5266;\
https://10.1.75.51:4267,http://10.1.75.51:3267;\
https://10.1.75.51:6267,http://10.1.75.51:5267;\
https://10.1.75.51:4268,http://10.1.75.51:3268;\
https://10.1.75.51:6268,http://10.1.75.51:5268" \
-threshold 0.60
```

| Flag | Default | Meaning |
|---|---|---|
| `-backends` | *(required)* | `chatURL,apiURL` pairs separated by `;` |
| `-threshold` | 0.70 | CPU fraction (0.0–1.0) above which a backend is deprioritised |

**Listen port.** `main.go` currently hard-codes the listen address:

```go
server := &http.Server{
    Addr:    ":7000",
    Handler: lb,
}
```

The documented deployment uses port **7265**. Change `":7000"` to `":7265"` and rebuild, or put a
port forward in front of it, so the URLs below match.

### How backend selection works

Every backend is polled at `GET <apiURL>/health` on a 2-second ticker by its own goroutine. Each
candidate is scored:

```
score = 0.7 × cpu_fraction
      + 0.2 × memory_fraction
      + 0.1 × min(in_flight / 50, 1.0)
```

Selection proceeds in two passes: first among alive backends below the CPU threshold, choosing the
lowest score; if every alive backend is over the threshold, the lowest-scoring alive backend is
used anyway rather than shedding the request.

Requests are retried up to **3 times**, excluding backends that have already failed for that
request. The request body is buffered up front so each retry can replay it. A backend is marked
unhealthy after **5 consecutive** connection-level failures; a successful response or a successful
health check resets the counter.

Retry is abandoned if response headers have already reached the client, since writing a second
response would corrupt the first.

Verify it's running:

```bash
curl http://10.1.75.51:7265/lb/health      # -> ok
curl http://10.1.75.51:7265/lb/status      # -> per-backend CPU/memory/in-flight
curl http://10.1.75.51:7265/lb/metrics     # -> totals and latency percentiles
```

---

## 11. Running the Frontend

```bash
cd chat_app_load_balancing/chat-frontend

npm install
npm run dev
```

Vite serves on `http://localhost:5173` by default.

The WebSocket target is set at the top of
`chat_app_load_balancing/chat-frontend/src/App.jsx`:

```javascript
const WS_URL = "wss://10.1.75.51:5266";
```

Point this at whichever backend's WebSocket port you want to connect to. Note that the load
balancer proxies `/message` and `/feed` only — WebSocket connections go directly to a backend.

For a production build:

```bash
npm run build
npm run preview
```

### What the client does

- Generates a 2048-bit RSA-PSS key pair per session using the Web Crypto API. The private key
  never leaves the browser.
- Sends `{type: "register", username, public_key}` as the first frame after connecting. The
  backend rejects the connection if the first message is anything else.
- Signs each outgoing message with SHA-256 / PSS (salt length 32) and sends the base64 signature
  alongside the plaintext.
- Receives `history` on join, then `chat` and `system` events live.

The commented-out `tamperedMessage` line in `sendMessage()` is a test hook: uncomment it to send a
message whose content no longer matches its signature and watch the backend reject it with
`[SIGNATURE REJECTED]`.

---

## 12. API Reference

### Load balancer

| Endpoint | Method | Purpose |
|---|---|---|
| `/lb/health` | GET | Liveness of the load balancer itself. Returns `ok`. |
| `/lb/status` | GET | Per-backend CPU, memory, in-flight count, alive flag. |
| `/lb/metrics` | GET | Totals, successes, failures, backend errors, p50/p95/p99. |
| `/message` | POST | Proxied to a selected backend. |
| `/feed` | GET | Proxied to a selected backend. |

`GET /lb/status` returns an array, one object per backend:

| Field | Type | Description |
|---|---|---|
| `url` | string | WebSocket URL of the backend |
| `api_url` | string | HTTP API URL of the backend |
| `alive` | bool | Whether health checks are currently passing |
| `in_flight` | int | Requests currently being proxied to this backend |
| `cpu_percent` | float | Most recent CPU reading |
| `memory_percent` | float | Most recent memory reading |

`GET /lb/metrics`:

| Field | Type | Description |
|---|---|---|
| `total` | int | Requests received |
| `success` | int | Responses with status < 400 |
| `failed` | int | Failed requests |
| `backend_errors` | int | Connection-level backend errors |
| `p50_ms`, `p95_ms`, `p99_ms` | float | Latency percentiles in milliseconds |

### Backend

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | CPU, memory, and persistence counters. |
| `/message` | POST | Submit a message for processing and persistence. |
| `/feed` | GET | Retrieve decrypted persisted messages. |

`POST /message` request body:

```json
{
  "client-name": "Alice",
  "msg": "hello world"
}
```

Response:

```json
{
  "status": "submitted",
  "id": "5f2c9a1e-0f3d-4c7b-9a1e-2b6d8c4f0a11"
}
```

`GET /feed` returns an array of `{username, message, timestamp}` objects. `load_history()` selects
the newest `HISTORY_LIMIT` (1000) rows with `ORDER BY timestamp DESC` and then reverses them, so
the array is the most recent 1000 messages in **chronological order — oldest first**. The result
is cached in-process for 200 ms
(`FEED_CACHE_TTL`) so that repeated polling under load doesn't hammer PostgreSQL.

---

## 13. Load Testing

Before running anything above ~200 users, raise the file-descriptor limit on the
load-generating machine. Exhausting descriptors there produces failures that look like backend
failures but are not:

```bash
ulimit -n 65535
```

Edit the `HEALTH_URLS` dictionary at the top of
`chat_app_load_balancing/python-message-load-generator/load_generator.py` so it points at your six
backends' `/health` endpoints:

```python
HEALTH_URLS = {
    "system1": "http://10.1.75.51:3266/health",
    "system2": "http://10.1.75.51:5266/health",
    "system3": "http://10.1.75.51:3267/health",
    "system4": "http://10.1.75.51:5267/health",
    "system5": "http://10.1.75.51:3268/health",
    "system6": "http://10.1.75.51:5268/health",
}
```

Run a test:

```bash
python3 chat_app_load_balancing/python-message-load-generator/load_generator.py \
    --url http://10.1.75.51:7265/message \
    --users 100 \
    --duration 60 \
    --min-length 50 \
    --max-length 500 \
    --min-interval 2 \
    --max-interval 3
```

| Flag | Default | Meaning |
|---|---|---|
| `--url` | *(required)* | Load balancer `/message` endpoint |
| `--users` | 10 | Number of simulated users (one thread each) |
| `--duration` | 60 | Test duration in seconds |
| `--min-length` / `--max-length` | 10 / 200 | Message length range in characters |
| `--min-interval` / `--max-interval` | 0.5 / 2.0 | Per-user delay between messages, in seconds |

Two files are written into the working directory, overwriting any previous run:

- **`latency.csv`** — one row per request: `timestamp, user_id, response_time_ms, status`
- **`utilization.csv`** — one row per second with CPU, memory, and persistence counters for all
  six backends

A missing value in `utilization.csv` means the health check failed or timed out. It does **not**
mean the metric was zero, and it should not be treated as zero when interpreting results.

Console output at the end reports total/successful/failed requests, timeouts, connection errors,
throughput, and latency percentiles.

---

## 14. Generating Graphs

Run this **immediately after** a load test, from the same directory, before the CSVs are
overwritten by another run:

```bash
python3 chat_app_load_balancing/python-message-load-generator/visualize.py
```

Eight PNGs are written to
`chat_app_load_balancing/python-message-load-generator/graphs/` at 300 DPI:

| File | Contents |
|---|---|
| `response_time_over_time.png` | Latency of every request over the run |
| `cpu_utilization_over_time.png` | CPU for all six backends |
| `memory_utilization_over_time.png` | Memory for all six backends |
| `persistence_over_time.png` | Cumulative persistence operations per backend |
| `persistence_success_failures.png` | Successes vs failures per backend |
| `response_time_distribution.png` | Latency histogram (50 bins) |
| `response_time_percentiles.png` | P50 / P95 / P99 bar chart |
| `average_cpu_utilization.png` | Mean CPU per backend |
| `average_memory_utilization.png` | Mean memory per backend |

Persistence counters are **cumulative**. Their slope is the rate; the raw values are not a
per-second figure and shouldn't be read as one.

To keep results from several runs, rename or move the CSVs and the `graphs/` directory between
runs:

```bash
mkdir -p results/users-100
mv latency.csv utilization.csv graphs results/users-100/
```

---

## 15. Configuration Reference

Values that are currently hard-coded and will need editing for a different deployment.

**`chat_app_load_balancing/backend/server.py`**

| Constant | Value | Notes |
|---|---|---|
| `REDIS_HOST` / `REDIS_PORT` | `10.1.75.51` / `4265` | Machine 4 |
| Redis timeouts | 2 s socket, 2 s connect | Set on the `redis.Redis` client |
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | Always the local database |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `chatdb` / `chatuser` / `agastya` | Move the password out of source |
| `KEY_PATH` | `encryption.key` (alongside the script) | Must be identical on all machines |
| `HISTORY_LIMIT` | 1000 | Messages sent to a joining client |
| `FEED_CACHE_TTL` | 0.2 s | `/feed` in-process cache lifetime |
| DB pool | min 20, max 80 | `ThreadedConnectionPool` |
| `db_semaphore` | 10 | Concurrent DB writes from the Redis path |
| TLS paths | `/home/student/chat-ssl/{cert,key}.pem` | |

**`chat_app_load_balancing/load-balancer/main.go`**

| Constant | Value | Notes |
|---|---|---|
| `Addr` | `:7000` | Change to `:7265` to match the documented URL |
| `consecutiveFailureThreshold` | 5 | Failures before a backend is marked unhealthy |
| `maxAttempts` | 3 | Retries per request |
| `maxExpectedInFlight` | 50 | Normalisation constant in the score |
| Health interval | 2 s | Per-backend ticker |
| `ResponseHeaderTimeout` | 5 s | Transport timeout |
| Dial timeout | 2 s | |

**`chat_app_load_balancing/chat-frontend/src/App.jsx`**

| Constant | Value |
|---|---|
| `WS_URL` | `wss://10.1.75.51:5266` |

---

## 16. Troubleshooting

**`PoolError('connection pool exhausted')` in backend logs.**
More concurrent operations wanted a Redis or PostgreSQL connection than the pool could supply.
This is a resource limit, not a crash — Redis itself is still up. Raise `maxconn` on the
PostgreSQL pool or configure a larger Redis connection pool.

**`[Errno 24] Too many open files` from the load generator.**
The load-generating machine ran out of file descriptors. Run `ulimit -n 65535` before the test.
Failures from this source are client-side and do not indicate backend failure.

**`ReadTimeout` on `/health` during a test.**
The backend was reachable but too busy to answer within the 2-second timeout. The corresponding
cells in `utilization.csv` are blank — read them as "unknown", not zero.

**Frontend connects then immediately disconnects.**
Usually the self-signed certificate. Open `https://<host>:<ws-port>` in the same browser and accept
the warning first. Check the browser console for the WebSocket error.

**`Username already taken.`**
Usernames are unique case-insensitively among clients connected to the *same* backend process.

**Messages appear in some clients but not others.**
The `encryption.key` files have diverged. Copy one key to every machine and restart the backends.

**Backend marked `alive: false` in `/lb/status`.**
Five consecutive proxy failures, or failing health checks. Curl the backend's `/health` directly
to see whether the process is up. Recovery is automatic once a health check succeeds.

**Load balancer reports "no healthy backends".**
No backend has passed a health check yet. All backends start as `Alive = false` and are marked
healthy only after the first successful poll — give it a few seconds after start-up.

---

