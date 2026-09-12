import asyncio
import psutil
from urllib.parse import urlparse, parse_qs
import uuid
import random
import ssl
import base64
import os
import time
import json
import psycopg2
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from pathlib import Path
import argparse
import redis
import time

import websockets
from websockets.exceptions import ConnectionClosed

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

parser = argparse.ArgumentParser()

parser.add_argument("--port", type=int, default=6000)
parser.add_argument("--api_port", type=int, default=5000)
parser.add_argument("--name", default="backend")
parser.add_argument(
    "--failure-rate",
    type=float,
    default=0.0,
    help="Probability of synthetic failure for each request"
)

parser.add_argument(
    "--delay-ms",
    type=int,
    default=0,
    help="Synthetic delay in milliseconds"
)

args = parser.parse_args()
if not 0.0 <= args.failure_rate <= 1.0:
    parser.error("--failure-rate must be between 0.0 and 1.0")

API_PORT = args.api_port
NAME = args.name
HOST = "0.0.0.0"
PORT = args.port
REDIS_HOST = "10.1.75.51"
REDIS_PORT = 4265

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "chatdb"
DB_USER = "chatuser"
DB_PASSWORD = "agastya"
KEY_PATH = Path(__file__).with_name("encryption.key")

# Number of most recent messages sent to a user when they join.
HISTORY_LIMIT = 50

# Maps each WebSocket connection to its username.
users = {}
# username -> public signing key
public_keys = {}

ssl_context = ssl.SSLContext(
    ssl.PROTOCOL_TLS_SERVER
)

ssl_context.load_cert_chain(
    "/home/student/chat-ssl/cert.pem",
    "/home/student/chat-ssl/key.pem",
)

public_key_cache = {}
public_key_cache_lock = threading.Lock()
# ---------------------------------------------------------
# Encryption key
# ---------------------------------------------------------

def load_encryption_key():
    """Load the persistent AES-256 key, creating it if necessary."""

    if KEY_PATH.exists():
        return KEY_PATH.read_bytes()

    key = AESGCM.generate_key(bit_length=256)
    KEY_PATH.write_bytes(key)

    return key


ENCRYPTION_KEY = load_encryption_key()
aes = AESGCM(ENCRYPTION_KEY)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def init_db():
    """Create PostgreSQL tables if they do not exist."""

    with get_db_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    public_key TEXT NOT NULL
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id UUID PRIMARY KEY,
                    username TEXT NOT NULL,
                    public_key TEXT NOT NULL,
                    ciphertext BYTEA NOT NULL,
                    nonce BYTEA NOT NULL,
                    signature BYTEA NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                )
                """
            )

        connection.commit()

def store_message(message_id, username, public_key, ciphertext, nonce, signature, timestamp):

    """Store a message without allowing duplicate IDs."""

    with get_db_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO messages (
                    id,
                    username,
                    public_key,
                    ciphertext,
                    nonce,
                    signature,
                    timestamp
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    message_id,
                    username,
                    public_key,
                    psycopg2.Binary(ciphertext),
                    psycopg2.Binary(nonce),
                    psycopg2.Binary(signature),
                    timestamp,
                ),
            )

        connection.commit()

def load_history(limit=HISTORY_LIMIT):
    """Load the most recent messages."""

    with get_db_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    username,
                    public_key,
                    ciphertext,
                    nonce,
                    signature,
                    timestamp
                FROM messages
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (limit,),
            )

            rows = cursor.fetchall()

    return [
        {
            "username": username,
            "public_key": public_key,
            "ciphertext": base64.b64encode(
                ciphertext
            ).decode(),
            "nonce": base64.b64encode(
                nonce
            ).decode(),
            "signature": base64.b64encode(
                signature
            ).decode(),
            "timestamp": timestamp.isoformat(),
        }
        for (
            username,
            public_key,
            ciphertext,
            nonce,
            signature,
            timestamp,
        ) in reversed(rows)
    ]

def save_public_key(username, public_key):
    """Save or update a user's public key."""

    with get_db_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO users (
                    username,
                    public_key
                )
                VALUES (%s, %s)
                ON CONFLICT (username)
                DO UPDATE SET
                    public_key = EXCLUDED.public_key
                """,
                (username, public_key),
            )

        connection.commit()

def load_public_key(username):
    """Load a user's public key."""
    start = time.perf_counter()

    # Check cache first
    with public_key_cache_lock:
        cached = public_key_cache.get(username)

    if cached is not None:
        elapsed = time.perf_counter() - start
        print(
            f"{NAME}: CACHE HIT {username} "
            f"({elapsed * 1000:.2f}ms)"
        )
        return cached

    print(f"{NAME}: CACHE MISS {username}")

    connection_start = time.perf_counter()
    with get_db_connection() as connection:
        connection_time = time.perf_counter() - connection_start
        query_start = time.perf_counter()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT public_key
                FROM users
                WHERE username = %s
                """,
                (username,),
            )

            row = cursor.fetchone()
        query_time = time.perf_counter() - query_start
    total_time = time.perf_counter() - start

    print(
        f"{NAME}: "
        f"connection={connection_time * 1000:.2f}ms "
        f"query={query_time * 1000:.2f}ms "
        f"total={total_time * 1000:.2f}ms"
    )

    if row is None:
        return None

    public_key = row[0]

    # Store in cache
    with public_key_cache_lock:
        public_key_cache[username] = public_key

    return public_key

# ---------------------------------------------------------
# AES-GCM
# ---------------------------------------------------------

def encrypt_message(message):
    """Encrypt plaintext using AES-GCM."""

    nonce = os.urandom(12)

    ciphertext = aes.encrypt(
        nonce,
        message.encode(),
        None,
    )

    return ciphertext, nonce

def decrypt_message(ciphertext, nonce):
    """Decrypt an AES-GCM encrypted message."""

    plaintext = aes.decrypt(
        nonce,
        ciphertext,
        None,
    )

    return plaintext.decode()

# ---------------------------------------------------------
# Digital signatures
# ---------------------------------------------------------

def verify_signature(public_key_b64, message, signature):
    """Verify a message using the sender's RSA-PSS public key."""

    if public_key_b64 is None:
        return False

    try:
        public_key_bytes = base64.b64decode(public_key_b64)

        public_key = serialization.load_der_public_key(
            public_key_bytes
        )

        public_key.verify(
            signature,
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=32,
            ),
            hashes.SHA256(),
        )

        return True

    except Exception as error:
        print("Signature verification failed:", repr(error))
        return False

# ---------------------------------------------------------
# Broadcasting
# ---------------------------------------------------------

async def broadcast(payload):
    """Send a JSON payload to all currently connected clients."""
    if not users:
        return

    message = json.dumps(payload)

    clients = list(users.keys())

    results = await asyncio.gather(
        *(client.send(message) for client in clients),
        return_exceptions=True,
    )

    # Remove clients whose connection failed while broadcasting.
    for client, result in zip(clients, results):
        if isinstance(result, Exception):
            users.pop(client, None)

# ---------------------------------------------------------
# User registration
# ---------------------------------------------------------

async def register_user(websocket):
    """Receive and validate a username from a new client."""
    try:
        raw_message = await websocket.recv()
    except ConnectionClosed:
        return None

    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError:
        await websocket.send(json.dumps({
            "type": "error",
            "message": "Invalid registration message.",
        }))
        return None

    if data.get("type") != "register":
        await websocket.send(json.dumps({
            "type": "error",
            "message": "First message must be registration.",
        }))
        return None

    username = data.get("username", "").strip()
    public_key = data.get("public_key")

    if not username:
        await websocket.send(json.dumps({
            "type": "error",
            "message": "Username cannot be empty.",
        }))
        return None

    if len(username) > 20:
        await websocket.send(json.dumps({
            "type": "error",
            "message": "Username must be 20 characters or fewer.",
        }))
        return None

    if not public_key:
        await websocket.send(json.dumps({
            "type": "error",
            "message": "Public key is required.",
        }))
        return None

    # Usernames are considered unique case-insensitively.
    existing_names = {
        name.lower()
        for name in users.values()
    }

    if username.lower() in existing_names:
        await websocket.send(json.dumps({
            "type": "error",
            "message": "Username already taken.",
        }))
        return None

    # Store public key in memory and database.
    public_keys[username] = public_key
    await asyncio.to_thread(
        save_public_key,
        username,
        public_key,
    )

    return username


async def unregister_user(websocket):
    """Remove a client from the connected users."""
    return users.pop(websocket, None)


# ---------------------------------------------------------
# Client handler
# ---------------------------------------------------------

async def handle_client(websocket):
    """Handle the complete session of one connected client."""

    # ---------------------------------------------------------
    # Synthetic failure simulation
    # ---------------------------------------------------------

    if args.delay_ms > 0:
        await asyncio.sleep(args.delay_ms / 1000)

    username = await register_user(websocket)

    if username is None:
        await websocket.close()
        return

    # Send recent chat history to the new user only.
    # This must happen BEFORE adding the client to `users`, otherwise a
    # broadcast could reach it before it has received the history.
    history = await asyncio.to_thread(load_history)

    decrypted_history = []

    for item in history:

        try:
            ciphertext = base64.b64decode(item["ciphertext"])
            nonce = base64.b64decode(item["nonce"])
            signature = base64.b64decode(item["signature"])

            message = decrypt_message(
                ciphertext,
                nonce,
            )

            valid_signature = verify_signature(
                item["public_key"],
                message,
                signature,
            )

            if not valid_signature:
                print(
                f"Invalid signature for "
                f"message from {item['username']}")
                continue

            decrypted_history.append({
                "username": item["username"],
                "content": message,
                "timestamp": item["timestamp"],
            })
        except Exception as error:
            # Ignore corrupted/tampered messages.
            print(
        f"SECURITY ALERT: Message from "
        f"{item['username']} failed "
        f"integrity/decryption check: {repr(error)}")
        continue

    await websocket.send(json.dumps({
        "type": "history",
        "messages": decrypted_history,
    }))

    users[websocket] = username

    print(f"{username} joined the chat.")

    await broadcast({
        "type": "system",
        "content": f"{username} joined the chat.",
    })

    try:
        async for raw_message in websocket:

            try:
                data = json.loads(raw_message)
            except json.JSONDecodeError:
                continue

            if data.get("type") != "chat":
                continue

            message = data.get("content", "").strip()
            signature_b64 = data.get("signature")

            if not message or not signature_b64:
                continue

            try:
                signature = base64.b64decode(signature_b64)
            except Exception:
                continue

            print(f"{username}: {message}")

            signature_valid = verify_signature(
                public_keys[username],
                message,
                signature,
                )

            if signature_valid:
                print(
                    f"[SIGNATURE VERIFIED] "
                    f"{username}: {message}"
                )
            else:
                print(
                    f"[SIGNATURE REJECTED] "
                    f"{username}: {message}"
                )

            if not signature_valid:
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "Invalid message signature.",
                }))
                continue

            ciphertext, nonce = encrypt_message(message)

            timestamp = datetime.now(
                timezone.utc
            ).isoformat()

            message_id = str(uuid.uuid4())

            payload = {
                "id": message_id,
                "username": username,
                "public_key": public_keys[username],
                "ciphertext": base64.b64encode(ciphertext).decode(),
                "nonce": base64.b64encode(nonce).decode(),
                "signature": base64.b64encode(signature).decode(),
                "timestamp": timestamp,
            }

            redis_client.publish(
                "chat_messages",
                json.dumps(payload),
            )

            #await asyncio.to_thread(
            #    store_message,
            #    username,
            #    public_keys[username],
            #    ciphertext,
            #    nonce,
            #    signature,
            #    timestamp
            #)

            #await broadcast({
            #    "type": "chat",
            #    "username": username,
            #    "content": message,
            #    "timestamp": timestamp,
            #})

    except ConnectionClosed:
        pass

    finally:
        removed_username = await unregister_user(websocket)

        if removed_username is not None:
            print(f"{removed_username} left the chat.")
            await broadcast({
                "type": "system",
                "content": f"{removed_username} left the chat.",
            })

# ---------------------------------------------------------
# Basic APIs for load balancing
# ---------------------------------------------------------
def get_cgroup_cpu_percent():
    """
    Return CPU usage relative to the cgroup CPU quota.

    100% means the entire CPU allocation is being consumed.
    """

    # Read CPU quota
    with open("/sys/fs/cgroup/cpu.max", "r") as f:
        quota, period = f.read().strip().split()

    if quota == "max":
        # No cgroup CPU limit.
        return psutil.cpu_percent(interval=0.1)

    quota = int(quota)
    period = int(period)

    # How many CPUs the cgroup is allowed to use.
    allocated_cpus = quota / period

    # Read total CPU time used by the entire cgroup.
    with open("/sys/fs/cgroup/cpu.stat", "r") as f:
        stats = {}

        for line in f:
            key, value = line.split()
            stats[key] = int(value)

    usage_start = stats["usage_usec"]
    time_start = time.monotonic()

    # Measure over 100 ms.
    time.sleep(0.1)

    with open("/sys/fs/cgroup/cpu.stat", "r") as f:
        stats = {}

        for line in f:
            key, value = line.split()
            stats[key] = int(value)

    usage_end = stats["usage_usec"]
    time_end = time.monotonic()

    cpu_time = (usage_end - usage_start) / 1_000_000
    elapsed = time_end - time_start

    # CPU capacity available during the measurement period.
    allocated_cpu_time = elapsed * allocated_cpus

    cpu_percent = (
        cpu_time / allocated_cpu_time
    ) * 100

    return min(cpu_percent, 100.0)

class APIHandler(BaseHTTPRequestHandler):

    def send_json(self, status_code, data):
        response = json.dumps(data).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            self.wfile.write(response)
        except BrokenPipeError:
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, OPTIONS"
        )
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type"
        )
        self.end_headers()

    def do_POST(self):

        if self.path != "/message":
            self.send_json(
                404,
                {"error": "Not found"}
            )
            return

        try:
            content_length = int(
                self.headers.get("Content-Length", 0)
            )

            body = self.rfile.read(content_length)

            data = json.loads(body)

            client_name = data.get("client-name")
            msg = data.get("msg")

            if not client_name or not msg:
                self.send_json(
                    400,
                    {
                        "error":
                        "client-name and msg are required"
                    }
                )
                return

            message_id = str(uuid.uuid4())

            timestamp = datetime.now(
                timezone.utc
            )

            # Encrypt the message.
            start = time.perf_counter()
            ciphertext, nonce = encrypt_message(msg)
            encrypt_time = time.perf_counter() - start

            # Get the user's public key if your application
            # requires it.
            start = time.perf_counter()
            public_key = load_public_key(client_name)
            key_load_time = time.perf_counter() - start


            if public_key is None:
                public_key = ""

            # Create payload for Redis.
            payload = {
                "id": message_id,
                "username": client_name,
                "public_key": public_key,
                "ciphertext": base64.b64encode(
                    ciphertext
                ).decode(),
                "nonce": base64.b64encode(
                    nonce
                ).decode(),
                "signature": "",
                "timestamp": timestamp.isoformat(),
                "source": "api"
            }

            start = time.perf_counter()
            # Publish to Redis.
            redis_client.publish(
                "chat_messages",
                json.dumps(payload)
            )
            redis_time = time.perf_counter() - start
            print(
                f"{NAME}: "
                f"encrypt={encrypt_time * 1000:.2f}ms "
                f"key_load={key_load_time * 1000:.2f}ms "
                f"redis={redis_time * 1000:.2f}ms"
            )

            self.send_json(
                200,
                {
                    "status": "submitted",
                    "id": message_id
                }
            )

        except Exception as error:

            print(
                f"{NAME}: /message error: "
                f"{repr(error)}"
            )

            self.send_json(
                500,
                {
                    "error": "Internal server error"
                }
            )

    def do_GET(self):

        if self.path == "/health":

            cpu_percent = get_cgroup_cpu_percent()
            memory_percent = psutil.virtual_memory().percent

            response = {
                "status": "ok",
                "backend": NAME,
                "cpu_percent": round(cpu_percent, 2),
                "memory_percent": round(memory_percent, 2),
            }

            self.send_json(200, response)
            return


        if self.path == "/feed":

            try:

                messages = load_history()
                
                plaintext_messages = []

                for message in messages:
                    try:
                        ciphertext = base64.b64decode(
                            message["ciphertext"]
                        )

                        nonce = base64.b64decode(
                            message["nonce"]
                        )

                        plaintext = decrypt_message(
                            ciphertext,
                            nonce
                        )

                        plaintext_messages.append({
                            "username": message["username"],
                            "message": plaintext,
                            "timestamp": message["timestamp"],
                        })

                    except Exception as error:
                        print(
                            f"{NAME}: failed to decrypt message: "
                            f"{repr(error)}"
                        )

                self.send_json(
                    200,
                    plaintext_messages
                )

            except Exception as error:

                print(
                    f"{NAME}: /feed error: "
                    f"{repr(error)}"
                )

                self.send_json(
                    500,
                    {
                        "error": "Internal server error"
                    }
                )

        self.send_json(
            404,
            {"error": "Not found"}
        )

    def log_message(self, format, *args):
        # Prevent BaseHTTPRequestHandler from
        # filling your terminal with access logs.
        pass

def start_api_server():
    server = ThreadingHTTPServer(
        (HOST, API_PORT),
        APIHandler
    )

    print(
        f"API & Health server running on "
        f"http://{HOST}:{API_PORT}"
    )

    server.serve_forever()

async def process_request(connection, request):
    query = parse_qs(
        urlparse(request.path).query
    )

    # Manual failure
    if query.get("fail", ["false"])[0].lower() == "true":
        print(f"[FAILURE SIMULATION] {NAME}: manual 503")

        return (
            503,
            [],
            b"Synthetic backend failure\n",
        )

    if random.random() < args.failure_rate:
        print(f"[FAILURE SIMULATION] {NAME}: synthetic 503")

        return (
            503,
            [],
            b"Synthetic backend failure\n",
        )

    return None

async def handle_redis_message(payload):
    """Process a message received from Redis."""

    try:
        message_id = payload["id"]
        username = payload["username"]
        public_key = payload["public_key"]

        ciphertext = base64.b64decode(
            payload["ciphertext"]
        )

        nonce = base64.b64decode(
            payload["nonce"]
        )

        signature = base64.b64decode(
            payload["signature"]
        )

        timestamp = payload["timestamp"]

        source = payload.get("source", "websocket")

        # Save to THIS backend's PostgreSQL database.
        await asyncio.to_thread(
            store_message,
            message_id,
            username,
            public_key,
            ciphertext,
            nonce,
            signature,
            timestamp,
        )

        # Decrypt so this backend can broadcast
        # plaintext to its connected clients.
        message = decrypt_message(
            ciphertext,
            nonce,
        )

        # Verify the signature.
        if source != "api":

            valid_signature = verify_signature(
                public_key,
                message,
                signature,
            )

            if not valid_signature:
                print(
                    f"{NAME}: invalid signature "
                    f"for message {message_id}"
                )
                return

        # Send to clients connected to THIS backend.
        await broadcast(
            {
                "type": "chat",
                "username": username,
                "content": message,
                "timestamp": timestamp,
            }
        )

        print(
            f"{NAME}: broadcast message "
            f"{message_id} from {username}"
        )

    except Exception as error:
        print(
            f"{NAME}: Redis message processing error: "
            f"{repr(error)}"
        )

def redis_listener(loop):
    """Listen for messages published by any backend."""

    pubsub = redis_client.pubsub()
    pubsub.subscribe("chat_messages")

    print(
        f"{NAME}: subscribed to Redis "
        f"channel 'chat_messages'"
    )

    for item in pubsub.listen():

        if item["type"] != "message":
            continue

        try:
            payload = json.loads(item["data"])

            asyncio.run_coroutine_threadsafe(
                handle_redis_message(payload),
                loop,
            )

        except Exception as error:
            print(
                f"{NAME}: Redis message error: "
                f"{repr(error)}"
            )

async def main():
    """Start the WebSocket server."""

    init_db()

    api_thread = threading.Thread(
        target=start_api_server,
        daemon=True,
    )

    api_thread.start()

    loop = asyncio.get_running_loop()

    redis_thread = threading.Thread(
        target=redis_listener,
        args=(loop,),
        daemon=True,
    )

    redis_thread.start()

    async with websockets.serve(
        handle_client,
        HOST,
        PORT,
        process_request=process_request,
        ssl=ssl_context,
    ):
        print(f"WebSocket server running on wss://{HOST}:{PORT} (backend server name: {NAME})")
        print("Waiting for clients...")

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
