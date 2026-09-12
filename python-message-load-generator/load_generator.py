import argparse
import random
import string
import threading
import time
import requests
import urllib3
import csv
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEALTH_URLS = {
    "system1": "http://10.1.75.51:3266/health",
    "system2": "http://10.1.75.51:5266/health",
    "system3": "http://10.1.75.51:3267/health",
    "system4": "http://10.1.75.51:5267/health",
    "system5": "http://10.1.75.51:3268/health",
    "system6": "http://10.1.75.51:5268/health",
}

latency_file = open(
    "latency.csv",
    "w",
    newline="",
    encoding="utf-8"
)

latency_writer = csv.writer(latency_file)

latency_writer.writerow([
    "timestamp",
    "user_id",
    "response_time_ms",
    "status"
])

latency_lock = threading.Lock()


utilization_file = open(
    "utilization.csv",
    "w",
    newline="",
    encoding="utf-8"
)

utilization_writer = csv.writer(utilization_file)

utilization_writer.writerow([
    "timestamp",
    "system1_cpu",
    "system1_memory",
    "system2_cpu",
    "system2_memory",
    "system3_cpu",
    "system3_memory",
    "system4_cpu",
    "system4_memory",
    "system5_cpu",
    "system5_memory",
    "system6_cpu",
    "system6_memory"
])

utilization_lock = threading.Lock()

def random_message(min_length, max_length):
    length = random.randint(min_length, max_length)

    characters = string.ascii_letters + string.digits + " "

    return "".join(random.choices(characters, k=length))

class LoadGenerator:
    def __init__(self, url, users, duration, min_length, max_length, min_interval, max_interval):

        self.url = url
        self.users = users
        self.duration = duration

        self.min_length = min_length
        self.max_length = max_length

        self.min_interval = min_interval
        self.max_interval = max_interval

        self.total = 0
        self.success = 0
        self.failed = 0

        self.latencies = []

        self.lock = threading.Lock()

        self.stop_event = threading.Event()

    def send_message(self, user_id):
        username = f"LoadUser{user_id}"

        while not self.stop_event.is_set():

            message = random_message(self.min_length, self.max_length)

            payload = {
                "client-name": username,
                "msg": message,
            }

            start = time.perf_counter()

            try:
                response = requests.post(
                    self.url,
                    json=payload,
                    verify=False,
                    timeout=10,
                )


                elapsed_ms = (time.perf_counter() - start) * 1000

                with latency_lock:
                        latency_writer.writerow([
                            datetime.now(timezone.utc).isoformat(),
                            user_id,
                            round(elapsed_ms, 2),
                            response.status_code
                        ])

                with self.lock:
                    self.total += 1

                    elapsed = time.perf_counter() - start
                    self.latencies.append(elapsed)

                    if 200 <= response.status_code < 300:
                        self.success += 1
                    else:
                        self.failed += 1

            except requests.RequestException:
                elapsed_ms = (time.perf_counter() - start) * 1000
                with latency_lock:
                        latency_writer.writerow([
                            datetime.now(timezone.utc).isoformat(),
                            user_id,
                            round(elapsed_ms, 2),
                            "ERROR"
                        ])
                elapsed = time.perf_counter() - start

                with self.lock:
                    self.total += 1
                    self.failed += 1
                    self.latencies.append(elapsed)

            interval = random.uniform(
                self.min_interval,
                self.max_interval,
            )

            self.stop_event.wait(interval)

    def run(self):
        print("Starting load generator")
        print(f"URL:       {self.url}")
        print(f"Users:     {self.users}")
        print(f"Duration:  {self.duration}s")
        print(
            f"Message:   "
            f"{self.min_length}-{self.max_length} chars"
        )
        print(
            f"Interval:  "
            f"{self.min_interval}-{self.max_interval}s"
        )
        print()

        threads = []
        start_time = time.perf_counter()

        stop_event = threading.Event()

        monitor_thread = threading.Thread(
            target=collect_utilization,
            args=(stop_event,),
            daemon=True
        )

        monitor_thread.start()
        
        for user_id in range(1, self.users + 1):

            thread = threading.Thread(
                target=self.send_message,
                args=(user_id,),
            )

            thread.daemon = True
            thread.start()

            threads.append(thread)

        try:
            time.sleep(self.duration)

        except KeyboardInterrupt:
            print("\nStopping...")

        self.stop_event.set()

        for thread in threads:
            thread.join()

        stop_event.set()
        monitor_thread.join()

        latency_file.close()
        utilization_file.close()

        elapsed = time.perf_counter() - start_time

        self.print_results(elapsed)

    def print_results(self, elapsed):
        with self.lock:
            total = self.total
            success = self.success
            failed = self.failed
            latencies = list(self.latencies)

        if latencies:
            latencies.sort()

            average = sum(latencies) / len(latencies)

            p50 = percentile(latencies, 0.50)
            p95 = percentile(latencies, 0.95)
            p99 = percentile(latencies, 0.99)

        else:
            average = 0
            p50 = 0
            p95 = 0
            p99 = 0

        requests_per_second = (
            total / elapsed
            if elapsed > 0
            else 0
        )

        success_rate = (
            success / total * 100
            if total > 0
            else 0
        )

        print()
        print("=" * 50)
        print("LOAD TEST RESULTS")
        print("=" * 50)

        print(f"Duration:          {elapsed:.2f}s")
        print(f"Users:             {self.users}")
        print(f"Total requests:    {total}")
        print(f"Successful:        {success}")
        print(f"Failed:            {failed}")
        print(f"Success rate:      {success_rate:.2f}%")
        print(f"Requests/sec:      {requests_per_second:.2f}")

        print()
        print("Latency")
        print(f"Average:           {average * 1000:.2f} ms")
        print(f"P50:               {p50 * 1000:.2f} ms")
        print(f"P95:               {p95 * 1000:.2f} ms")
        print(f"P99:               {p99 * 1000:.2f} ms")

        print("=" * 50)


def percentile(values, p):
    if not values:
        return 0

    index = int((len(values) - 1) * p)

    return values[index]

def get_system_utilization(name):
    try:
        response = requests.get(
            HEALTH_URLS[name],
            timeout=2
        )

        data = response.json()

        return (
            data.get("cpu_percent"),
            data.get("memory_percent")
        )

    except Exception:
        return None, None

def collect_utilization(stop_event):

    while not stop_event.is_set():

        timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        results = {}

        threads = []

        def collect(name):
            results[name] = get_system_utilization(name)

        # Start all six health checks concurrently
        for name in HEALTH_URLS:
            thread = threading.Thread(
                target=collect,
                args=(name,)
            )

            thread.start()
            threads.append(thread)

        # Wait for all six
        for thread in threads:
            thread.join()

        row = [timestamp]

        for name in [
            "system1",
            "system2",
            "system3",
            "system4",
            "system5",
            "system6"
        ]:
            cpu, memory = results.get(
                name,
                (None, None)
            )

            row.append(cpu)
            row.append(memory)

        with utilization_lock:
            utilization_writer.writerow(row)
            utilization_file.flush()

        # Wait one second before next sample
        stop_event.wait(1)

def main():
    parser = argparse.ArgumentParser(
        description="Custom HTTP load generator"
    )

    parser.add_argument(
        "--url",
        required=True,
        help="Load balancer /message URL",
    )

    parser.add_argument(
        "--users",
        type=int,
        default=10,
        help="Number of simulated users",
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds",
    )

    parser.add_argument(
        "--min-length",
        type=int,
        default=10,
        help="Minimum message length",
    )

    parser.add_argument(
        "--max-length",
        type=int,
        default=200,
        help="Maximum message length",
    )

    parser.add_argument(
        "--min-interval",
        type=float,
        default=0.5,
        help="Minimum interval between messages",
    )

    parser.add_argument(
        "--max-interval",
        type=float,
        default=2.0,
        help="Maximum interval between messages",
    )

    args = parser.parse_args()

    if args.users <= 0:
        parser.error("--users must be greater than 0")

    if args.duration <= 0:
        parser.error("--duration must be greater than 0")

    if args.min_length <= 0:
        parser.error("--min-length must be greater than 0")

    if args.max_length < args.min_length:
        parser.error(
            "--max-length must be >= --min-length"
        )

    if args.min_interval < 0:
        parser.error(
            "--min-interval cannot be negative"
        )

    if args.max_interval < args.min_interval:
        parser.error(
            "--max-interval must be >= --min-interval"
        )

    generator = LoadGenerator(
        url=args.url,
        users=args.users,
        duration=args.duration,
        min_length=args.min_length,
        max_length=args.max_length,
        min_interval=args.min_interval,
        max_interval=args.max_interval,
    )

    generator.run()


if __name__ == "__main__":
    main()
