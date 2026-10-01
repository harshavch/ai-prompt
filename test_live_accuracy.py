"""
Real end-to-end accuracy validation.

Unlike test_accuracy.py (a fast statistical model of the real distributions),
this script drives an ACTUAL RUNNING instance of the system over real HTTP,
end to end: real /deploy, real /simulate-failure hitting a real service,
real waiting through real engine ticks, real /status polling until an
outcome is reached. This is slow (each trial takes real wall-clock seconds)
but it's the ground truth the fast model is checked against.

Usage:
    # in one terminal:
    uvicorn main:app --port 8000

    # in another:
    python3 test_live_accuracy.py
"""

import time
import urllib.request
import urllib.parse
import json

BASE = "http://localhost:8000"
HEALTHY_TRIALS = 5
FAILURE_TRIALS_PER_SCENARIO = 3
POLL_INTERVAL = 1.0
MAX_WAIT = 60


def call(method, path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def status():
    return call("GET", "/status")


def wait_for_outcome(terminal_statuses, max_wait=MAX_WAIT):
    start = time.time()
    while time.time() - start < max_wait:
        s = status()
        if s["status"] in terminal_statuses:
            return s
        time.sleep(POLL_INTERVAL)
    return status()  # timed out - return whatever we have


def run_healthy_trial():
    call("POST", "/reset")
    time.sleep(0.5)
    call("POST", "/deploy")
    result = wait_for_outcome(["PROMOTED", "ROLLED_BACK"], max_wait=40)
    return result["status"]


def run_failure_trial(mode, severity):
    call("POST", "/reset")
    time.sleep(0.5)
    call("POST", "/deploy")
    time.sleep(1.5)
    call("POST", "/simulate-failure", params={"mode": mode, "severity": severity})
    result = wait_for_outcome(["ROLLED_BACK", "PROMOTED"], max_wait=30)
    return result["status"]


if __name__ == "__main__":
    print("Confirming server is reachable...")
    print(status()["status"])

    print(f"\n=== {HEALTHY_TRIALS} REAL healthy deployment trials ===")
    healthy_outcomes = []
    for i in range(HEALTHY_TRIALS):
        outcome = run_healthy_trial()
        healthy_outcomes.append(outcome)
        print(f"  trial {i+1}: {outcome}")
    promoted = healthy_outcomes.count("PROMOTED")
    print(f"  -> {promoted}/{HEALTHY_TRIALS} promoted correctly "
          f"({HEALTHY_TRIALS - promoted} false rollbacks)")

    scenarios = [(m, s) for m in ("errors", "latency") for s in ("mild", "moderate", "severe")]
    all_results = {}
    for mode, severity in scenarios:
        print(f"\n=== {FAILURE_TRIALS_PER_SCENARIO} REAL trials: {mode}/{severity} ===")
        outcomes = []
        for i in range(FAILURE_TRIALS_PER_SCENARIO):
            outcome = run_failure_trial(mode, severity)
            outcomes.append(outcome)
            print(f"  trial {i+1}: {outcome}")
        detected = outcomes.count("ROLLED_BACK")
        all_results[(mode, severity)] = detected
        print(f"  -> {detected}/{FAILURE_TRIALS_PER_SCENARIO} correctly rolled back")

    call("POST", "/reset")

    print(f"\n{'='*60}\nREAL END-TO-END SUMMARY\n{'='*60}")
    total_trials = HEALTHY_TRIALS + len(scenarios) * FAILURE_TRIALS_PER_SCENARIO
    total_correct = promoted + sum(all_results.values())
    print(f"Overall: {total_correct}/{total_trials} correct decisions "
          f"({100*total_correct/total_trials:.1f}%)")
    print("(Small N - this validates the fast statistical model in "
          "test_accuracy.py, it doesn't replace it.)")
