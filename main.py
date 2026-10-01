"""
Automated Canary Deployment & Rollback Pipeline — REAL VERSION
-----------------------------------------------------------------
This is not a metrics simulator. Two real, independent HTTP services run
inside this process as separate embedded servers on their own real ports:
  - Slot A: http://127.0.0.1:9101
  - Slot B: http://127.0.0.1:9102

All traffic, health checks, and failure injection go over real loopback
HTTP requests to those real services. Nothing about "the canary is failing"
is a centrally-faked number — it's the measured outcome of real HTTP calls
to a real process that has actually been told to misbehave.

You can verify this independently at any time by opening, in another tab,
whichever port is currently the canary:
    http://localhost:9101/work   or   http://localhost:9102/work
and refreshing — you'll see real (occasionally erroring, once faulted)
JSON responses, live.

  1. Deployment Engine   - tracks which physical slot (A/B) is currently
                            "stable" vs "canary"
  2. Canary Controller   - ramps REAL traffic 10% -> 25% -> 50% -> 100%;
                            a live traffic loop actually routes real HTTP
                            requests according to this split
  3. Monitoring Engine   - fires real concurrent HTTP probes at the canary
                            service every tick and measures REAL latency
                            and REAL error rate from the actual responses
  4. Rollback Engine     - debounced threshold check on those real numbers
  5. Fault injection     - "Simulate Error Spike" sends a real POST to the
                            canary service's own /fault endpoint, which
                            changes that service's real behavior

Run:
    pip install -r requirements.txt
    uvicorn main:app --reload
    open http://localhost:8000
"""

import asyncio
import os
import random
import statistics
import time
from datetime import datetime
from enum import Enum
from typing import List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Two real backend services (slots A / B). Each is a genuinely separate
# FastAPI app, bound to its own real TCP port, with its own real in-memory
# state. /work does real (small) async work and can really fail. /fault
# lets us really make it misbehave, from the outside, over real HTTP.
# ---------------------------------------------------------------------------
PORTS = {"A": 9101, "B": 9102}

FAULT_ERROR_PROB = {"mild": 0.15, "moderate": 0.35, "severe": 0.70}
FAULT_EXTRA_LATENCY = {"mild": 0.35, "moderate": 0.65, "severe": 1.10}
BASELINE_ERROR_RATE = 0.005       # real 0.5% baseline error rate, like real infra
BASELINE_LATENCY_RANGE = (0.04, 0.09)  # real 40-90ms of simulated work


def make_service_app(slot_label: str) -> FastAPI:
    svc = FastAPI()
    svc_state = {"fault_mode": None, "severity": "moderate", "requests_served": 0}

    @svc.get("/work")
    async def work():
        svc_state["requests_served"] += 1
        latency = random.uniform(*BASELINE_LATENCY_RANGE)
        is_error = random.random() < BASELINE_ERROR_RATE

        if svc_state["fault_mode"] == "errors":
            if random.random() < FAULT_ERROR_PROB[svc_state["severity"]]:
                is_error = True
        elif svc_state["fault_mode"] == "latency":
            extra = FAULT_EXTRA_LATENCY[svc_state["severity"]]
            latency += random.uniform(extra * 0.7, extra * 1.3)

        await asyncio.sleep(latency)
        if is_error:
            raise HTTPException(status_code=500, detail=f"slot {slot_label} internal error")
        return {"slot": slot_label, "ok": True, "requests_served": svc_state["requests_served"]}

    @svc.post("/fault")
    async def inject_fault(mode: str = "errors", severity: str = "moderate"):
        svc_state["fault_mode"] = mode
        svc_state["severity"] = severity
        return {"ok": True, "fault_mode": mode, "severity": severity}

    @svc.delete("/fault")
    async def clear_fault():
        svc_state["fault_mode"] = None
        return {"ok": True}

    @svc.get("/fault")
    async def get_fault():
        return {"fault_mode": svc_state["fault_mode"], "severity": svc_state["severity"],
                "requests_served": svc_state["requests_served"]}

    return svc


v1_app = make_service_app("A")
v2_app = make_service_app("B")


async def run_embedded_service(asgi_app: FastAPI, port: int):
    config = uvicorn.Config(asgi_app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    server = uvicorn.Server(config)
    await server.serve()


# ---------------------------------------------------------------------------
# Control plane
# ---------------------------------------------------------------------------
app = FastAPI(title="Canary Deployment & Auto-Rollback Pipeline")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

TICK_SECONDS = 1.5
TRAFFIC_STEPS = [10, 25, 50, 100]
HISTORY_LEN = 40
PROBE_COUNT = 5            # real concurrent health-check requests per engine tick
TRAFFIC_INTERVAL = 0.3     # a real synthetic "user" request every 300ms


class Status(str, Enum):
    IDLE = "IDLE"
    CANARY = "CANARY"
    WARNING = "WARNING"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    PROMOTED = "PROMOTED"


class Config:
    def __init__(self):
        self.error_threshold = 5.0
        self.latency_threshold = 400.0
        self.debounce_ticks = 2
        self.promotion_interval_ticks = 4

    def as_dict(self):
        return {
            "error_threshold": self.error_threshold,
            "latency_threshold": self.latency_threshold,
            "debounce_ticks": self.debounce_ticks,
            "promotion_interval_ticks": self.promotion_interval_ticks,
        }


config = Config()


def check_breach(error_rate: float, response_time: float, cfg: Config):
    if error_rate > cfg.error_threshold:
        return True, f"error rate {error_rate:.1f}% > {cfg.error_threshold:.1f}%"
    if response_time > cfg.latency_threshold:
        return True, f"response time {response_time:.0f}ms > {cfg.latency_threshold:.0f}ms"
    return False, None


class State:
    def __init__(self):
        self.reset()

    def reset(self):
        self.status = Status.IDLE
        self.stable_slot = "A"
        self.canary_slot: Optional[str] = None
        self.stable_version = "V1"
        self.canary_version: Optional[str] = None
        self.canary_traffic = 0
        self.error_rate = 0.0
        self.response_time = 0.0
        self.healthy_ticks = 0
        self.consecutive_breaches = 0
        self.last_breach_reason: Optional[str] = None
        self.failure_mode: Optional[str] = None
        self.failure_severity = "moderate"
        self.error_history: List[float] = []
        self.latency_history: List[float] = []
        self.total_requests = 0
        self.canary_requests = 0
        self.request_log: List[dict] = []
        self.logs: List[str] = []
        self.log("System initialized. Slot A (port 9101) is stable @ 100% real traffic.")

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{ts}] {msg}")
        self.logs = self.logs[-30:]

    def push_history(self):
        self.error_history.append(round(self.error_rate, 2))
        self.latency_history.append(round(self.response_time, 1))
        self.error_history = self.error_history[-HISTORY_LEN:]
        self.latency_history = self.latency_history[-HISTORY_LEN:]

    def as_dict(self):
        return {
            "status": self.status,
            "stable_version": self.stable_version,
            "canary_version": self.canary_version,
            "stable_port": PORTS[self.stable_slot],
            "canary_port": PORTS[self.canary_slot] if self.canary_slot else None,
            "canary_traffic": self.canary_traffic,
            "stable_traffic": 100 - self.canary_traffic if self.canary_version else 100,
            "error_rate": round(self.error_rate, 2),
            "response_time": round(self.response_time),
            "consecutive_breaches": self.consecutive_breaches,
            "last_breach_reason": self.last_breach_reason,
            "failure_mode": self.failure_mode,
            "failure_severity": self.failure_severity,
            "error_history": self.error_history,
            "latency_history": self.latency_history,
            "total_requests": self.total_requests,
            "canary_requests": self.canary_requests,
            "request_log": self.request_log[-12:],
            "logs": self.logs,
            "config": config.as_dict(),
        }


state = State()
state_lock = asyncio.Lock()
http_client: Optional[httpx.AsyncClient] = None


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        async def send(ws):
            try:
                await ws.send_json(data)
                return None
            except Exception:
                return ws
        results = await asyncio.gather(*(send(ws) for ws in self.active), return_exceptions=False)
        for dead in results:
            if dead is not None:
                self.disconnect(dead)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Real HTTP helpers
# ---------------------------------------------------------------------------
async def real_request(port: int):
    """Fires one real HTTP GET at a real embedded service and measures the
    real wall-clock round-trip time and real response status."""
    start = time.perf_counter()
    try:
        r = await http_client.get(f"http://127.0.0.1:{port}/work")
        elapsed_ms = (time.perf_counter() - start) * 1000
        return r.status_code < 500, elapsed_ms
    except Exception:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return False, elapsed_ms


async def probe_canary_health(port: int, n: int = PROBE_COUNT):
    """Fires N real concurrent requests at the canary service and computes
    a real error rate / average latency from the actual responses. This is
    what the rollback engine's decision is based on."""
    results = await asyncio.gather(*(real_request(port) for _ in range(n)))
    successes = sum(1 for ok, _ in results if ok)
    error_rate = 100 * (n - successes) / n
    avg_latency = statistics.mean(lat for _, lat in results)
    return error_rate, avg_latency


# ---------------------------------------------------------------------------
# Live traffic generator — real synthetic "users" continuously hitting
# whichever real service the current traffic split says they should.
# ---------------------------------------------------------------------------
async def traffic_loop():
    while True:
        await asyncio.sleep(TRAFFIC_INTERVAL)
        async with state_lock:
            if state.status not in (Status.CANARY, Status.WARNING, Status.PROMOTED,
                                     Status.ROLLED_BACK, Status.IDLE):
                continue
            if state.canary_slot and random.uniform(0, 100) < state.canary_traffic:
                target_slot, target_version = state.canary_slot, state.canary_version
            else:
                target_slot, target_version = state.stable_slot, state.stable_version
            port = PORTS[target_slot]

        ok, latency_ms = await real_request(port)

        async with state_lock:
            state.total_requests += 1
            if target_slot == state.canary_slot:
                state.canary_requests += 1
            state.request_log.append({
                "version": target_version,
                "port": port,
                "ok": ok,
                "latency": round(latency_ms, 1),
                "ts": datetime.now().strftime("%H:%M:%S"),
            })
            state.request_log = state.request_log[-15:]

        await manager.broadcast(state.as_dict())


# ---------------------------------------------------------------------------
# Canary Controller + Rollback Engine — driven entirely by REAL measured
# health from real HTTP calls to the real canary service.
# ---------------------------------------------------------------------------
async def engine_loop():
    while True:
        await asyncio.sleep(TICK_SECONDS)

        async with state_lock:
            active = state.status in (Status.CANARY, Status.WARNING)
            canary_port = PORTS[state.canary_slot] if (active and state.canary_slot) else None

        if canary_port is None:
            continue

        error_rate, response_time = await probe_canary_health(canary_port)

        do_finalize_rollback = False
        snapshot = None

        async with state_lock:
            if state.status not in (Status.CANARY, Status.WARNING):
                continue  # state changed mid-probe (e.g. a reset landed)

            state.error_rate = error_rate
            state.response_time = response_time
            state.push_history()

            breached, reason = check_breach(error_rate, response_time, config)

            if breached:
                state.consecutive_breaches += 1
                state.last_breach_reason = reason
                if state.consecutive_breaches >= config.debounce_ticks:
                    state.status = Status.ROLLING_BACK
                    state.log(
                        f"CANARY FAILURE CONFIRMED after {state.consecutive_breaches} "
                        f"consecutive real breach(es) — {reason}. Rollback initiated."
                    )
                    snapshot = state.as_dict()
                    do_finalize_rollback = True
                else:
                    state.status = Status.WARNING
                    state.log(
                        f"Real health check breach {state.consecutive_breaches}/"
                        f"{config.debounce_ticks} — {reason}. Watching for confirmation."
                    )
            else:
                if state.consecutive_breaches > 0:
                    state.log("Metrics recovered — breach streak reset, no rollback triggered.")
                state.consecutive_breaches = 0
                state.status = Status.CANARY
                state.healthy_ticks += 1
                if state.healthy_ticks >= config.promotion_interval_ticks:
                    state.healthy_ticks = 0
                    idx = TRAFFIC_STEPS.index(state.canary_traffic)
                    if idx < len(TRAFFIC_STEPS) - 1:
                        state.canary_traffic = TRAFFIC_STEPS[idx + 1]
                        state.log(
                            f"Real health checks passing. Increasing {state.canary_version} "
                            f"traffic to {state.canary_traffic}%."
                        )
                    else:
                        state.status = Status.PROMOTED
                        state.stable_slot, state.canary_slot = state.canary_slot, state.stable_slot
                        state.stable_version = state.canary_version
                        state.canary_version = None
                        state.canary_traffic = 0
                        state.log(f"{state.stable_version} promoted to stable @ 100% real traffic "
                                  f"(now served from port {PORTS[state.stable_slot]}).")

        if do_finalize_rollback:
            await manager.broadcast(snapshot)
            await asyncio.sleep(TICK_SECONDS)
            async with state_lock:
                failed_port = PORTS[state.canary_slot]
            try:
                await http_client.delete(f"http://127.0.0.1:{failed_port}/fault")
            except Exception:
                pass
            async with state_lock:
                state.canary_traffic = 0
                state.canary_slot = None
                state.canary_version = None
                state.failure_mode = None
                state.consecutive_breaches = 0
                state.status = Status.ROLLED_BACK
                state.log(f"Rollback complete. {state.stable_version} restored to 100% real traffic.")

        await manager.broadcast(state.as_dict())


@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(timeout=3.0)
    asyncio.create_task(run_embedded_service(v1_app, PORTS["A"]))
    asyncio.create_task(run_embedded_service(v2_app, PORTS["B"]))
    await asyncio.sleep(0.6)  # let the embedded services bind before anything probes them
    asyncio.create_task(engine_loop())
    asyncio.create_task(traffic_loop())


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
@app.get("/status")
def get_status():
    return state.as_dict()


@app.post("/deploy")
async def deploy():
    async with state_lock:
        if state.status in (Status.CANARY, Status.WARNING, Status.ROLLING_BACK):
            return {"ok": False, "message": "Deployment already in progress."}
        next_version = "V2" if state.stable_version == "V1" else f"V{int(state.stable_version[1:]) + 1}"
        canary_slot = "B" if state.stable_slot == "A" else "A"
        canary_port = PORTS[canary_slot]

        # Reserve the deployment slot immediately, before the network call,
        # so a concurrent request can't slip through the check above while
        # we're awaiting the real HTTP call to reset the canary service.
        # The lock is an asyncio.Lock, so holding it across an `await` is
        # safe (other coroutines just wait their turn) - it does not block
        # the event loop the way a threading.Lock would.
        state.status = Status.CANARY
        state.canary_slot = canary_slot
        state.canary_version = next_version

        try:
            await http_client.delete(f"http://127.0.0.1:{canary_port}/fault")
        except Exception:
            # Roll back the reservation - the canary service was unreachable.
            state.status = Status.IDLE
            state.canary_slot = None
            state.canary_version = None
            return {"ok": False, "message": "Could not reach the canary service to reset it."}

        state.canary_traffic = TRAFFIC_STEPS[0]
        state.healthy_ticks = 0
        state.consecutive_breaches = 0
        state.failure_mode = None
        state.error_history = []
        state.latency_history = []
        state.log(f"Deploying {next_version} to real service on port {canary_port} "
                   f"as canary @ {state.canary_traffic}% real traffic.")
        return {"ok": True}


@app.post("/simulate-failure")
async def simulate_failure(mode: str = "errors", severity: str = "moderate"):
    if mode not in ("errors", "latency"):
        return {"ok": False, "message": "mode must be 'errors' or 'latency'."}
    if severity not in ("mild", "moderate", "severe"):
        return {"ok": False, "message": "severity must be 'mild', 'moderate', or 'severe'."}

    async with state_lock:
        if state.status not in (Status.CANARY, Status.WARNING):
            return {"ok": False, "message": "No active canary to fail."}
        if state.failure_mode:
            return {"ok": False, "message": "Failure already injected."}
        canary_port = PORTS[state.canary_slot]
        canary_version = state.canary_version

        # Reserve the injection before awaiting the real HTTP call, for the
        # same reason as /deploy above - otherwise two concurrent calls can
        # both pass the check while the first is still awaiting the network.
        state.failure_mode = mode
        state.failure_severity = severity

        try:
            r = await http_client.post(f"http://127.0.0.1:{canary_port}/fault",
                                        params={"mode": mode, "severity": severity})
            r.raise_for_status()
        except Exception as e:
            state.failure_mode = None
            return {"ok": False, "message": f"Could not inject fault into the real service: {e}"}

        label = "elevated error rate" if mode == "errors" else "latency spike"
        state.log(f"Real fault injected into {canary_version} on port {canary_port} "
                   f"({severity} {label}) — the service itself is now misbehaving.")
        return {"ok": True}


@app.post("/reset")
async def reset():
    for port in PORTS.values():
        try:
            await http_client.delete(f"http://127.0.0.1:{port}/fault")
        except Exception:
            pass
    async with state_lock:
        state.reset()
        return {"ok": True}


class ConfigUpdate(BaseModel):
    error_threshold: Optional[float] = None
    latency_threshold: Optional[float] = None
    debounce_ticks: Optional[int] = None


@app.post("/configure")
async def configure(update: ConfigUpdate):
    async with state_lock:
        if update.error_threshold is not None:
            if not (0.1 <= update.error_threshold <= 50):
                return {"ok": False, "message": "error_threshold must be between 0.1 and 50."}
            config.error_threshold = update.error_threshold
        if update.latency_threshold is not None:
            if not (50 <= update.latency_threshold <= 5000):
                return {"ok": False, "message": "latency_threshold must be between 50 and 5000."}
            config.latency_threshold = update.latency_threshold
        if update.debounce_ticks is not None:
            if not (1 <= update.debounce_ticks <= 10):
                return {"ok": False, "message": "debounce_ticks must be between 1 and 10."}
            config.debounce_ticks = update.debounce_ticks
        state.log(f"Config updated: error>{config.error_threshold}%, "
                  f"latency>{config.latency_threshold}ms, debounce={config.debounce_ticks} ticks.")
        await manager.broadcast(state.as_dict())
        return {"ok": True, "config": config.as_dict()}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    await websocket.send_json(state.as_dict())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


FRONTEND_PATH = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")


@app.get("/")
def root():
    return FileResponse(FRONTEND_PATH)
