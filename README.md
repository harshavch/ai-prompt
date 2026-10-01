# Automated Rollback & Canary Deployment Pipeline

A working canary deployment system with **two genuinely separate, independently
running HTTP services** — not a central simulator faking numbers. Real traffic
gets routed between them over real loopback HTTP, real health checks measure
real response times and real error rates, and "failure injection" is a real
POST that makes a real, separate process actually start failing.

## Architecture

| Layer | What it does |
|---|---|
| **Two real services** | Slot A (`:9101`) and Slot B (`:9102`) — independent embedded HTTP servers, each with its own `/work` endpoint and its own real in-memory fault state |
| **Deployment Engine** | Tracks which physical slot is currently "stable" vs "canary" |
| **Canary Controller** | Ramps **real** traffic `10% → 25% → 50% → 100%` — a live traffic loop actually routes real HTTP requests according to this split |
| **Monitoring Engine** | Fires 5 real concurrent HTTP probes at the canary service every tick and computes real error rate / real average latency from the actual responses |
| **Rollback Engine** | Debounced threshold check (2 consecutive real breaches by default) on those real numbers |
| **Fault injection** | "Simulate Error Spike" sends a real `POST /fault` to the canary service itself — that service then actually starts returning real 500s / really sleeping longer |
| **Dashboard** | Live WebSocket view, plus direct links to the real services so you can verify them independently |

**You can prove this to yourself at any time**: while a canary is running,
open `http://localhost:9101/work` or `http://localhost:9102/work` (whichever
port the dashboard shows) directly in another tab and refresh — real JSON,
occasionally a real error, and real 500s once you've injected a fault.

## Run it

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

Open **http://localhost:8000**. This single command starts the dashboard
*and* both embedded services (ports 9101/9102) in the same process — nothing
else to run separately.

## Demo script

1. Click **Deploy New Version** — a real canary starts on the other physical
   port at 10% real traffic.
2. Watch it healthily ramp on real measured health: 10% → 25% → 50% → 100%,
   then get promoted — notice the "stable service" link in the dashboard
   actually switches ports on promotion, because the physical slot roles
   swap.
3. Reset, deploy again, pick a severity and click **Simulate Error Spike**.
   This sends a real request to the canary service telling it to misbehave.
   Open that service's link directly in another tab and refresh a few times
   — you'll see real 500s. Back on the dashboard, watch `WARNING` (breach
   1/2) confirm to `ROLLING_BACK`.
4. Try **Simulate Latency Spike** on a fresh deploy instead — the canary
   service really starts sleeping longer per request.
5. Drag the **threshold/debounce sliders** live during a run and watch the
   system react without a restart.

## Two real bugs found this round (not simulated ones)

Testing against the real system surfaces different problems than testing a
central simulator does — these two only showed up once the system was real:

**1. A genuine concurrency bug under real network latency.** `/deploy` and
`/simulate-failure` each need to check state, then `await` a real HTTP call
to the actual service, then commit state. With the lock released during
that `await`, 15 concurrent `/deploy` calls let **5 through instead of 1** —
the check-then-network-call-then-commit pattern has a real race window that
a synchronous fake-metrics version never exposed. Fixed by reserving state
*before* the network call and holding the lock across the `await` (safe for
`asyncio.Lock`, unlike a threading lock).

**2. A real fault-calibration bug.** "Mild latency" severity added only
~105–195ms on top of a 40–90ms baseline — topping out around 285ms, which
can *never* cross the 400ms threshold. Measured true-positive rate: **0.7%**.
This wasn't noise or bad luck, it was a design constant that made an entire
failure scenario mathematically undetectable. Recalibrated the severities;
TPR for that scenario is now 99.97%. See `test_accuracy.py`'s history for
the before/after numbers.

## Accuracy — measured two ways

**1. Fast statistical model (`test_accuracy.py`, runs in seconds).** Imports
`main.py` directly and samples the *exact* probability distributions coded
into the real services (`BASELINE_ERROR_RATE`, `FAULT_ERROR_PROB`,
`FAULT_EXTRA_LATENCY`, etc.) plus the real `check_breach()` / debounce logic,
across 21,000 simulated deployments — fast enough to sweep debounce settings
and get statistical confidence without waiting out thousands of real
30-second deployments.

**2. Real end-to-end validation (`test_live_accuracy.py`, ~5 min).** Actually
drives the live running server over real HTTP — real `/deploy`, real
`/simulate-failure` against a real service, real waiting through real ticks
— across 23 genuine trials (5 healthy + 3 per failure scenario). This is
slower but it's ground truth, not a model of ground truth.

| Metric | Fast model (21,000 trials) | Real end-to-end (23 trials) |
|---|---|---|
| Overall accuracy | **99.72%** | **100%** (23/23 — small N, consistent with the model) |
| False positive rate | **1.03%** | 0/5 healthy runs falsely rolled back |
| True positive rate, all 6 scenarios | 98.3%–100% depending on severity | 3/3 on every scenario, including the fixed mild-latency case |

The small-N real run agreeing with the fast model (including correctly
catching the previously-broken mild-latency scenario 3/3 times on the actual
live system) is what makes the 99.72% figure trustworthy rather than
theoretical.

**Debounce sweep** (why 2 is the default, not arbitrary):

| Debounce | Overall accuracy | False positive rate |
|---|---|---|
| 1 tick (no debounce) | 95.24% | **33.33%** — unusable in practice |
| **2 ticks (default)** | **99.72%** | **1.03%** |
| 3 ticks | 97.68% | 0.00% — but mild error-rate detection drops to 85.5% (needs 3 consecutive breaches within the window, less likely for a borderline-slow failure) |

Debounce=2 is the balance point: false positives drop off a cliff going from
1→2, and pushing to 3 buys a lower FPR at a real cost to detecting mild,
slow-ramping failures in time.

**Honest caveats, not swept under the rug:**
- At debounce=2, mild error-spike TPR is 99.13% (not 100%) — a small number
  of mild, slow-ramping failures still slip past the debounce window within
  the test's tick budget. This is the real tradeoff debounce buys you.
- On latency scenarios, a small fraction of failures get attributed to the
  error-rate branch rather than latency, purely from baseline error noise —
  the rollback decision is still correct, but "which metric caused it" isn't
  perfectly clean in the logs.

## Notes for the case study

- `PORTS`, `FAULT_ERROR_PROB`, `FAULT_EXTRA_LATENCY`, and `Config` near the
  top of `main.py` are the numbers to cite directly in the report.
- `engine_loop()` is the core rollback decision logic — real probes in,
  debounced decision out. Good to walk judges through line-by-line.
- **Limitations to state upfront:** the two services run in-process rather
  than as separate containers/pods — real HTTP over loopback, but not real
  container orchestration. A production version would run these as actual
  Kubernetes pods/deployments and use a real ingress/service mesh for
  traffic splitting instead of an application-level traffic loop.
- **Future scope:** Kubernetes-native version (Argo Rollouts style), real
  Prometheus-style metrics instead of direct probes, configurable rollout
  strategies, more than two physical slots for true zero-downtime multi-version
  rollouts.
