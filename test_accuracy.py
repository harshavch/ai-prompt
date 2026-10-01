"""
Accuracy test for the REAL canary system.

This system no longer has a central "fake metrics" function to Monte-Carlo
against - error rates and latencies are the real outcome of real HTTP calls
to a real service that may or may not be faulted. So this file does two
different, complementary things:

  1. FAST STATISTICAL MODEL (this file, runs in seconds): reproduces the
     exact probability parameters that are hardcoded into main.py's
     make_service_app() - BASELINE_ERROR_RATE, BASELINE_LATENCY_RANGE,
     FAULT_ERROR_PROB, FAULT_EXTRA_LATENCY - and the same PROBE_COUNT /
     debounce logic from engine_loop(). It samples those exact
     distributions thousands of times to get a statistically meaningful
     accuracy figure without waiting out thousands of real deployments
     (each real deployment takes ~15-45 real seconds).

  2. REAL END-TO-END VALIDATION (test_live_accuracy.py, runs in ~10 min):
     actually starts the real embedded services and drives a smaller batch
     of genuine deployments over real HTTP, to confirm the fast model above
     isn't lying to you. Run that one before you trust these numbers.

Run: python3 test_accuracy.py
"""

import random
import statistics as stats

import main  # reuse the exact constants and check_breach() from the real app

TRIALS = 3000
MAX_TICKS = 30


def sample_baseline_request():
    """Models one real /work call under normal (unfaulted) conditions,
    using main.py's actual BASELINE_ERROR_RATE / BASELINE_LATENCY_RANGE."""
    latency_s = random.uniform(*main.BASELINE_LATENCY_RANGE)
    is_error = random.random() < main.BASELINE_ERROR_RATE
    return is_error, latency_s * 1000


def sample_faulted_request(mode, severity):
    """Models one real /work call while the service has a real fault
    injected, using main.py's actual FAULT_ERROR_PROB / FAULT_EXTRA_LATENCY."""
    latency_s = random.uniform(*main.BASELINE_LATENCY_RANGE)
    is_error = random.random() < main.BASELINE_ERROR_RATE

    if mode == "errors":
        if random.random() < main.FAULT_ERROR_PROB[severity]:
            is_error = True
    elif mode == "latency":
        extra = main.FAULT_EXTRA_LATENCY[severity]
        latency_s += random.uniform(extra * 0.7, extra * 1.3)

    return is_error, latency_s * 1000


def probe_tick(faulted, mode=None, severity=None, n=main.PROBE_COUNT):
    """Models one engine tick's PROBE_COUNT concurrent real requests and
    aggregates them exactly the way probe_canary_health() does."""
    results = [
        sample_faulted_request(mode, severity) if faulted else sample_baseline_request()
        for _ in range(n)
    ]
    successes = sum(1 for err, _ in results if not err)
    error_rate = 100 * (n - successes) / n
    avg_latency = stats.mean(lat for _, lat in results)
    return error_rate, avg_latency


def run_trial(cfg, faulted=False, mode=None, severity=None):
    consecutive = 0
    for tick in range(1, MAX_TICKS + 1):
        error_rate, response_time = probe_tick(faulted, mode, severity)
        breached, reason = main.check_breach(error_rate, response_time, cfg)
        if breached:
            consecutive += 1
            if consecutive >= cfg.debounce_ticks:
                branch = "error" if "error rate" in reason else "latency"
                return {"outcome": "rolled_back", "tick": tick, "branch": branch}
        else:
            consecutive = 0
            if tick >= cfg.promotion_interval_ticks * 4:  # reached 100% traffic + confirmed
                return {"outcome": "promoted", "tick": tick}
    return {"outcome": "timeout", "tick": MAX_TICKS}


def run_suite(debounce_ticks):
    cfg = main.Config()
    cfg.debounce_ticks = debounce_ticks

    print(f"\n{'#'*70}\n# DEBOUNCE = {debounce_ticks}\n{'#'*70}")

    healthy = [run_trial(cfg, faulted=False) for _ in range(TRIALS)]
    fp = [r for r in healthy if r["outcome"] == "rolled_back"]
    promoted = [r for r in healthy if r["outcome"] == "promoted"]
    fpr = len(fp) / TRIALS
    print(f"Healthy: {len(promoted)}/{TRIALS} promoted, {len(fp)}/{TRIALS} false rollback "
          f"-> FPR {fpr*100:.2f}%")

    tprs = {}
    for mode in ("errors", "latency"):
        for severity in ("mild", "moderate", "severe"):
            results = [run_trial(cfg, faulted=True, mode=mode, severity=severity) for _ in range(TRIALS)]
            detected = [r for r in results if r["outcome"] == "rolled_back"]
            tpr = len(detected) / TRIALS
            tprs[(mode, severity)] = tpr
            ticks = [r["tick"] for r in detected]
            mean_tick = stats.mean(ticks) if ticks else float("nan")
            print(f"  {mode:8s} {severity:8s}: TPR {tpr*100:6.2f}%  mean detect tick {mean_tick:.2f}")

    total = TRIALS * (1 + 6)
    correct = len(promoted) + sum(round(tpr * TRIALS) for tpr in tprs.values())
    overall = 100 * correct / total
    print(f"\nOverall accuracy @ debounce={debounce_ticks}: {overall:.2f}%  |  FPR {fpr*100:.2f}%")
    return {"debounce": debounce_ticks, "accuracy": overall, "fpr": fpr, "tprs": tprs}


if __name__ == "__main__":
    results = [run_suite(d) for d in (1, 2, 3)]
    print(f"\n{'='*70}\nSUMMARY\n{'='*70}")
    print(f"{'debounce':<10}{'accuracy':<12}{'FPR':<10}")
    for r in results:
        print(f"{r['debounce']:<10}{r['accuracy']:<11.2f}%{r['fpr']*100:<9.2f}%")
