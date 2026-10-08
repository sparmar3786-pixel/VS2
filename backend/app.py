"""
Trading Terminal backend: Angel One SmartAPI (live feed + orders) + OpenAI (strategy / entry / SL / target / strike).

Default mode is PAPER. Real orders only go out if TRADING_MODE=live in .env AND the UI sends confirm=true.
"""
import os
import time
import json
import asyncio
import threading
import datetime as dt
from typing import Optional, Dict, Any, List

import pyotp
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

API_KEY = os.getenv("ANGEL_API_KEY", "")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE", "")
PIN = os.getenv("ANGEL_PIN", "")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
TRADING_MODE = os.getenv("TRADING_MODE", "paper").lower()  # paper | live
MAX_QTY = int(os.getenv("MAX_QTY_PER_ORDER", "100"))

EXCH_TYPE = {"NSE": 1, "NFO": 2, "BSE": 3, "BFO": 4, "MCX": 5, "NCDEX": 7, "CDS": 13}
MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

app = FastAPI(title="Trading Terminal")
loop: asyncio.AbstractEventLoop = None  # set on startup
clients: set = set()


class State:
    smart: Optional[SmartConnect] = None
    sws: Optional[SmartWebSocketV2] = None
    logged_in = False
    login_error = ""
    feed_token = ""
    auth_token = ""
    ticks: Dict[str, Dict[str, Any]] = {}      # "NSE:3045" -> tick
    meta: Dict[str, Dict[str, Any]] = {}       # "NSE:3045" -> {symbol, exchange, token}
    subs: Dict[int, set] = {}                  # exch_type -> tokens
    master: List[Dict[str, Any]] = []
    positions: List[Dict[str, Any]] = []       # paper positions
    log: List[str] = []
    ws_connected = False


S = State()


def log(msg: str):
    line = f"{dt.datetime.now().strftime('%H:%M:%S')} {msg}"
    S.log.append(line)
    S.log = S.log[-200:]
    push({"type": "log", "msg": line})


def push(payload: dict):
    """Thread-safe broadcast to browser clients."""
    if loop is None:
        return
    asyncio.run_coroutine_threadsafe(_broadcast(payload), loop)


async def _broadcast(payload: dict):
    dead = []
    for c in list(clients):
        try:
            await c.send_json(payload)
        except Exception:
            dead.append(c)
    for d in dead:
        clients.discard(d)


# ---------------------------------------------------------------- Angel login
def angel_login():
    S.login_error = ""
    if not all([API_KEY, CLIENT_CODE, PIN, TOTP_SECRET]):
        S.login_error = "ANGEL_* values missing in .env"
        return False
    try:
        S.smart = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = S.smart.generateSession(CLIENT_CODE, PIN, totp)
        if not data or not data.get("status"):
            S.login_error = f"Login failed: {data.get('message') if data else 'no response'}"
            return False
        S.auth_token = data["data"]["jwtToken"]
        S.feed_token = S.smart.getfeedToken()
        S.logged_in = True
        log("Angel One login OK")
        start_feed()
        return True
    except Exception as e:  # noqa
        S.login_error = f"Login error: {e}"
        log(S.login_error)
        return False


def start_feed():
    auth = S.auth_token.replace("Bearer ", "")
    sws = SmartWebSocketV2(auth, API_KEY, CLIENT_CODE, S.feed_token)

    def on_open(wsapp):
        S.ws_connected = True
        log("Live feed connected")
        # resubscribe anything we already had
        for et, toks in S.subs.items():
            if toks:
                sws.subscribe("sub", 2, [{"exchangeType": et, "tokens": list(toks)}])
        push({"type": "status", "ws": True})

    def on_data(wsapp, msg):
        try:
            et = msg.get("exchange_type")
            tok = str(msg.get("token", "")).strip('"')
            exch = next((k for k, v in EXCH_TYPE.items() if v == et), str(et))
            key = f"{exch}:{tok}"
            ltp = msg.get("last_traded_price", 0) / 100.0
            tick = {
                "key": key, "ltp": ltp,
                "open": msg.get("open_price_of_the_day", 0) / 100.0,
                "high": msg.get("high_price_of_the_day", 0) / 100.0,
                "low": msg.get("low_price_of_the_day", 0) / 100.0,
                "close": msg.get("closed_price", 0) / 100.0,
                "volume": msg.get("volume_trade_for_the_day", 0),
                "oi": msg.get("open_interest", 0),
                "ts": time.time(),
            }
            S.ticks[key] = tick
            paper_check(key, ltp)
            push({"type": "tick", **tick})
        except Exception as e:  # noqa
            log(f"tick parse error: {e}")

    def on_error(wsapp, err):
        log(f"Feed error: {err}")

    def on_close(wsapp):
        S.ws_connected = False
        push({"type": "status", "ws": False})
        log("Feed closed")

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close
    S.sws = sws
    threading.Thread(target=sws.connect, daemon=True).start()


def subscribe(exchange: str, token: str, symbol: str):
    et = EXCH_TYPE.get(exchange)
    if et is None:
        raise HTTPException(400, f"Unsupported exchange {exchange}")
    S.meta[f"{exchange}:{token}"] = {"symbol": symbol, "exchange": exchange, "token": token}
    S.subs.setdefault(et, set()).add(token)
    if S.sws and S.ws_connected:
        S.sws.subscribe("sub", 2, [{"exchangeType": et, "tokens": [token]}])


# ---------------------------------------------------------------- instrument master
def load_master():
    if S.master:
        return
    log("Downloading instrument master...")
    r = requests.get(MASTER_URL, timeout=60)
    r.raise_for_status()
    S.master = r.json()
    log(f"Instrument master loaded: {len(S.master)} rows")


# ---------------------------------------------------------------- paper engine
def paper_check(key: str, ltp: float):
    for p in S.positions:
        if p["status"] != "OPEN" or p["key"] != key:
            continue
        p["ltp"] = ltp
        sign = 1 if p["side"] == "BUY" else -1
        p["pnl"] = round((ltp - p["entry"]) * sign * p["qty"], 2)
        hit = None
        if p.get("sl"):
            if (p["side"] == "BUY" and ltp <= p["sl"]) or (p["side"] == "SELL" and ltp >= p["sl"]):
                hit = ("SL HIT", p["sl"])
        if p.get("target"):
            if (p["side"] == "BUY" and ltp >= p["target"]) or (p["side"] == "SELL" and ltp <= p["target"]):
                hit = ("TARGET HIT", p["target"])
        if hit:
            p["status"] = "CLOSED"
            p["exit"] = hit[1]
            p["reason"] = hit[0]
            p["pnl"] = round((hit[1] - p["entry"]) * sign * p["qty"], 2)
            log(f"[PAPER] {p['symbol']} {hit[0]} @ {hit[1]} pnl {p['pnl']}")
        push({"type": "positions", "positions": S.positions})


# ---------------------------------------------------------------- indicators
def ema(vals, n):
    k = 2 / (n + 1)
    out, e = [], None
    for v in vals:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def rsi(vals, n=14):
    if len(vals) <= n:
        return None
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = vals[i] - vals[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    ag, al = gains / n, losses / n
    for i in range(n + 1, len(vals)):
        d = vals[i] - vals[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
    return 100.0 if al == 0 else round(100 - 100 / (1 + ag / al), 2)


def atr(rows, n=14):
    if len(rows) < n + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h, l, pc = rows[i][2], rows[i][3], rows[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return round(sum(trs[-n:]) / n, 2)


def fetch_candles(exchange, token, interval, days):
    if not S.smart:
        raise HTTPException(503, "Angel One not logged in")
    now = dt.datetime.now()
    frm = (now - dt.timedelta(days=days)).replace(hour=9, minute=15, second=0, microsecond=0)
    res = S.smart.getCandleData({
        "exchange": exchange, "symboltoken": token, "interval": interval,
        "fromdate": frm.strftime("%Y-%m-%d %H:%M"), "todate": now.strftime("%Y-%m-%d %H:%M"),
    })
    if not res or not res.get("status"):
        raise HTTPException(502, f"Candle API: {res.get('message') if res else 'no response'}")
    return res["data"] or []


# ---------------------------------------------------------------- API
@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_running_loop()
    threading.Thread(target=angel_login, daemon=True).start()


@app.get("/api/status")
def status():
    return {
        "logged_in": S.logged_in, "login_error": S.login_error, "ws": S.ws_connected,
        "mode": TRADING_MODE, "openai": bool(OPENAI_KEY), "model": OPENAI_MODEL, "max_qty": MAX_QTY,
    }


@app.post("/api/login")
def relogin():
    ok = angel_login()
    return {"ok": ok, "error": S.login_error}


class SubReq(BaseModel):
    exchange: str
    token: str
    symbol: str


@app.post("/api/subscribe")
def api_subscribe(r: SubReq):
    subscribe(r.exchange, r.token, r.symbol)
    return {"ok": True, "key": f"{r.exchange}:{r.token}"}


@app.get("/api/search")
def search(q: str):
    load_master()
    ql = q.upper().strip()
    if len(ql) < 2:
        return []
    out = []
    for r in S.master:
        if r["exch_seg"] not in ("NSE", "BSE", "MCX"):
            continue
        sym, name = r["symbol"].upper(), r["name"].upper()
        if ql in sym or ql in name:
            if r["exch_seg"] == "NSE" and not (sym.endswith("-EQ") or r["instrumenttype"] == "AMXIDX"):
                continue
            out.append({"symbol": r["symbol"], "name": r["name"], "token": r["token"], "exchange": r["exch_seg"]})
        if len(out) >= 25:
            break
    return out


@app.get("/api/expiries")
def expiries(name: str):
    load_master()
    name = name.upper()
    exp = {r["expiry"] for r in S.master if r["exch_seg"] == "NFO" and r["name"] == name and r["instrumenttype"] in ("OPTIDX", "OPTSTK")}
    return sorted(exp, key=lambda x: dt.datetime.strptime(x, "%d%b%Y"))


@app.get("/api/options")
def options(name: str, expiry: str, spot: float, n: int = 8):
    """Strikes around spot with CE/PE tokens for one expiry."""
    load_master()
    name = name.upper()
    rows = [r for r in S.master if r["exch_seg"] == "NFO" and r["name"] == name and r["expiry"] == expiry and r["instrumenttype"] in ("OPTIDX", "OPTSTK")]
    by = {}
    for r in rows:
        strike = float(r["strike"]) / 100.0
        side = "CE" if r["symbol"].endswith("CE") else "PE"
        by.setdefault(strike, {})[side] = {"symbol": r["symbol"], "token": r["token"], "lot": int(r["lotsize"])}
    strikes = sorted(by)
    if not strikes:
        return []
    atm_i = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
    sel = strikes[max(0, atm_i - n): atm_i + n + 1]
    return [{"strike": s, **by[s], "atm": s == strikes[atm_i]} for s in sel]


@app.get("/api/candles")
def candles(exchange: str, token: str, interval: str = "FIVE_MINUTE", days: int = 3):
    rows = fetch_candles(exchange, token, interval, days)
    return [{"t": int(dt.datetime.fromisoformat(r[0]).timestamp()) + 19800, "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows]


@app.get("/api/snapshot")
def snapshot():
    return {"ticks": S.ticks, "meta": S.meta, "positions": S.positions, "log": S.log[-60:]}


# ---------------------------------------------------------------- AI
class AIReq(BaseModel):
    prompt: str = ""
    exchange: str
    token: str
    symbol: str
    interval: str = "FIVE_MINUTE"
    capital: float = 100000
    risk_pct: float = 1.0
    strategy: str = ""


AI_SYSTEM = """You are an Indian intraday/derivatives trading assistant inside a trading terminal.
You get live price, recent candles and indicators. Respond ONLY with a JSON object:
{
 "bias": "BULLISH|BEARISH|NEUTRAL",
 "strategy_name": str,
 "action": "BUY|SELL|WAIT",
 "entry": number, "stoploss": number, "target": number,
 "risk_reward": number,
 "option_suggestion": {"type": "CE|PE|NONE", "strike": number|null, "reason": str},
 "position_size": {"qty": int, "reason": str},
 "confidence": 0-100,
 "reasoning": str,
 "risks": [str],
 "answer": str
}
Rules: prices must be sensible relative to the live price and ATR; stoploss must be on the correct side of entry;
if the setup is unclear, set action to WAIT and explain. 'answer' is a short reply (Hinglish ok) to the user's prompt.
This is decision support, not guaranteed advice."""


@app.post("/api/ai")
def ai(req: AIReq):
    if not OPENAI_KEY:
        raise HTTPException(503, "OPENAI_API_KEY missing in .env")
    key = f"{req.exchange}:{req.token}"
    rows = []
    try:
        rows = fetch_candles(req.exchange, req.token, req.interval, 3)
    except HTTPException as e:
        log(f"AI candles unavailable: {e.detail}")
    closes = [r[4] for r in rows]
    tick = S.ticks.get(key, {})
    ltp = tick.get("ltp") or (closes[-1] if closes else None)
    ind = {}
    if closes:
        e9, e21 = ema(closes, 9), ema(closes, 21)
        ind = {"ema9": round(e9[-1], 2), "ema21": round(e21[-1], 2), "rsi14": rsi(closes), "atr14": atr(rows)}
    ctx = {
        "symbol": req.symbol, "exchange": req.exchange, "interval": req.interval,
        "ltp": ltp, "day": {k: tick.get(k) for k in ("open", "high", "low", "close", "volume", "oi")},
        "indicators": ind, "last_candles_ohlcv": rows[-40:],
        "capital": req.capital, "risk_per_trade_pct": req.risk_pct,
        "user_strategy_notes": req.strategy, "time_ist": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    client = OpenAI(api_key=OPENAI_KEY)
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            temperature=0.3,
            messages=[
                {"role": "system", "content": AI_SYSTEM},
                {"role": "user", "content": f"Context:\n{json.dumps(ctx)}\n\nUser request: {req.prompt or 'Give me the best trade plan right now.'}"},
            ],
        )
        plan = json.loads(resp.choices[0].message.content)
    except Exception as e:  # noqa
        raise HTTPException(502, f"OpenAI error: {e}")
    plan["_ltp"] = ltp
    plan["_indicators"] = ind
    log(f"AI plan for {req.symbol}: {plan.get('action')} entry {plan.get('entry')} sl {plan.get('stoploss')} tgt {plan.get('target')}")
    return plan


# ---------------------------------------------------------------- orders
class OrderReq(BaseModel):
    symbol: str
    token: str
    exchange: str
    side: str                       # BUY | SELL
    qty: int
    ordertype: str = "MARKET"       # MARKET | LIMIT
    price: float = 0
    product: str = "INTRADAY"       # INTRADAY | DELIVERY | CARRYFORWARD
    stoploss: Optional[float] = None
    target: Optional[float] = None
    confirm: bool = False


@app.post("/api/order")
def order(o: OrderReq):
    side = o.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "side must be BUY or SELL")
    if o.qty <= 0 or o.qty > MAX_QTY:
        raise HTTPException(400, f"qty must be 1..{MAX_QTY} (MAX_QTY_PER_ORDER)")
    key = f"{o.exchange}:{o.token}"
    ltp = S.ticks.get(key, {}).get("ltp")
    ref = o.price if o.ordertype == "LIMIT" and o.price else ltp
    if not ref:
        raise HTTPException(400, "No live price yet; subscribe and wait for a tick")
    if o.stoploss:
        if (side == "BUY" and o.stoploss >= ref) or (side == "SELL" and o.stoploss <= ref):
            raise HTTPException(400, "Stoploss is on the wrong side of entry")
    if o.target:
        if (side == "BUY" and o.target <= ref) or (side == "SELL" and o.target >= ref):
            raise HTTPException(400, "Target is on the wrong side of entry")

    if TRADING_MODE != "live":
        pos = {
            "id": f"P{int(time.time()*1000)}", "key": key, "symbol": o.symbol, "side": side, "qty": o.qty,
            "entry": ref, "sl": o.stoploss, "target": o.target, "ltp": ref, "pnl": 0.0,
            "status": "OPEN", "mode": "PAPER", "time": dt.datetime.now().strftime("%H:%M:%S"),
        }
        S.positions.append(pos)
        log(f"[PAPER] {side} {o.qty} {o.symbol} @ {ref} SL {o.stoploss} TGT {o.target}")
        push({"type": "positions", "positions": S.positions})
        return {"ok": True, "mode": "PAPER", "position": pos}

    # ---- LIVE
    if not o.confirm:
        raise HTTPException(400, "Live order needs confirm=true")
    if not S.smart:
        raise HTTPException(503, "Angel One not logged in")
    base = {
        "variety": "NORMAL", "tradingsymbol": o.symbol, "symboltoken": o.token, "exchange": o.exchange,
        "producttype": o.product, "duration": "DAY", "quantity": str(o.qty),
    }
    entry = {**base, "transactiontype": side, "ordertype": o.ordertype, "price": str(o.price if o.ordertype == "LIMIT" else 0)}
    ids = {}
    try:
        ids["entry"] = S.smart.placeOrder(entry)
        opp = "SELL" if side == "BUY" else "BUY"
        if o.stoploss:
            ids["stoploss"] = S.smart.placeOrder({**base, "transactiontype": opp, "ordertype": "STOPLOSS_MARKET",
                                                   "triggerprice": str(o.stoploss), "price": "0"})
        if o.target:
            ids["target"] = S.smart.placeOrder({**base, "transactiontype": opp, "ordertype": "LIMIT", "price": str(o.target)})
    except Exception as e:  # noqa
        log(f"[LIVE] order error: {e}")
        raise HTTPException(502, f"Order error: {e}  (placed so far: {ids})")
    log(f"[LIVE] {side} {o.qty} {o.symbol} ids={ids}")
    return {"ok": True, "mode": "LIVE", "order_ids": ids,
            "note": "SL and target are separate orders, not OCO. Cancel the other one manually after one fills."}


@app.post("/api/paper/close/{pid}")
def paper_close(pid: str):
    for p in S.positions:
        if p["id"] == pid and p["status"] == "OPEN":
            px = S.ticks.get(p["key"], {}).get("ltp", p["entry"])
            sign = 1 if p["side"] == "BUY" else -1
            p.update(status="CLOSED", exit=px, reason="MANUAL", pnl=round((px - p["entry"]) * sign * p["qty"], 2))
            push({"type": "positions", "positions": S.positions})
            return p
    raise HTTPException(404, "position not found")


@app.get("/api/live/positions")
def live_positions():
    if not S.smart:
        raise HTTPException(503, "Angel One not logged in")
    return S.smart.position()


@app.get("/api/live/orders")
def live_orders():
    if not S.smart:
        raise HTTPException(503, "Angel One not logged in")
    return S.smart.orderBook()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    await ws.send_json({"type": "status", "ws": S.ws_connected})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)


FRONT = os.path.join(os.path.dirname(__file__), "..", "frontend")


@app.get("/")
def index():
    return FileResponse(os.path.join(FRONT, "index.html"))


app.mount("/static", StaticFiles(directory=FRONT), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))