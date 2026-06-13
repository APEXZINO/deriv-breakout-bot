"""
Deriv Breakout Scanner
Pairs: V75, V75_1s, V10, V25
Timeframes: H4 (breakout detection) + M30 (retest entry)
- No duplicate alerts for same breakout level within cooldown period
- HTML formatted Telegram messages
"""

import asyncio, json, logging, sys, urllib.request, urllib.parse, os, time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

WAT = timezone(timedelta(hours=1))


# =============================================================================
#  CONFIG
# =============================================================================
@dataclass
class Config:
    symbols: list = field(default_factory=lambda: [
        "R_75",
        "1HZ75V",
        "R_10",
        "R_25",
    ])

    tg_token:   str = ""
    tg_chat_id: str = ""

    h4_tf:     int = 14400
    m30_tf:    int = 1800
    h4_count:  int = 100
    m30_count: int = 80

    swing_lookback:    int   = 10
    breakout_body_pct: float = 0.5
    retest_proximity:  float = 0.3
    min_breakout_pct:  float = 0.05

    rr_ratio:   float = 2.0
    atr_period: int   = 14

    # Cooldown — hours before same level can alert again
    cooldown_hours: int = 8

    live_mode: bool = False

    @property
    def uri(self):
        return "wss://ws.derivws.com/websockets/v3?app_id=1089"

CFG = Config()

if os.environ.get("TG_TOKEN"):
    CFG.tg_token = os.environ["TG_TOKEN"]
if os.environ.get("TG_CHAT_ID"):
    CFG.tg_chat_id = os.environ["TG_CHAT_ID"]


# =============================================================================
#  SIGNAL COOLDOWN — prevents duplicate alerts
# =============================================================================
# Stored as: { "SYMBOL_DIRECTION_LEVEL": timestamp_of_last_alert }
# Uses a flat file in the repo to persist across GitHub Actions runs
COOLDOWN_FILE = "signal_cooldown.json"

def load_cooldown() -> dict:
    try:
        if os.path.exists(COOLDOWN_FILE):
            with open(COOLDOWN_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_cooldown(data: dict):
    try:
        with open(COOLDOWN_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.error("Could not save cooldown file: %s", e)

def is_duplicate(symbol: str, direction: str, level: float) -> bool:
    """Returns True if this signal was already sent within cooldown_hours."""
    key = f"{symbol}_{direction}_{round(level, 2)}"
    cooldown = load_cooldown()
    if key in cooldown:
        last_sent = cooldown[key]
        hours_elapsed = (time.time() - last_sent) / 3600
        if hours_elapsed < CFG.cooldown_hours:
            log.info("Duplicate suppressed: %s (%.1f hrs ago)", key, hours_elapsed)
            return True
    return False

def mark_sent(symbol: str, direction: str, level: float):
    """Record that this signal was just sent."""
    key = f"{symbol}_{direction}_{round(level, 2)}"
    cooldown = load_cooldown()
    # Clean old entries (older than 24 hours)
    now = time.time()
    cooldown = {k: v for k, v in cooldown.items() if now - v < 86400}
    cooldown[key] = now
    save_cooldown(cooldown)
    log.info("Signal recorded: %s", key)


# =============================================================================
#  TELEGRAM
# =============================================================================
def send_telegram(message: str):
    if not CFG.tg_token or not CFG.tg_chat_id:
        log.info("Telegram not configured.")
        return
    try:
        url  = f"https://api.telegram.org/bot{CFG.tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    CFG.tg_chat_id,
            "text":       message,
            "parse_mode": "HTML"
        }).encode()
        urllib.request.urlopen(
            urllib.request.Request(url, data=data), timeout=10
        )
        log.info("Telegram sent.")
    except Exception as e:
        log.error("Telegram error: %s", e)


def build_alert(symbol, direction, broken_level,
                entry, sl, tp1, tp2, risk,
                h4_time, retest_time, score, rating) -> str:
    icon   = "🟢 <b>BULLISH BREAKOUT</b>" if direction == "BULL" else "🔴 <b>BEARISH BREAKOUT</b>"
    action = "BUY on retest" if direction == "BULL" else "SELL on retest"
    ht     = h4_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    rt     = retest_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    stars  = (
        "🔥 PRIME"   if rating == "PRIME"  else
        "⭐⭐ STRONG" if rating == "STRONG" else
        "⭐  GOOD"   if rating == "GOOD"   else
        "✗   SKIP"
    )
    return (
        f"{icon}\n"
        f"\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\n"
        f"<b>Pair:</b>    {symbol}\n"
        f"<b>Action:</b>  {action}\n"
        f"<b>Rating:</b>  {stars}  ({score}/5)\n"
        f"\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\n"
        f"<b>Broken Level:</b> {broken_level}\n"
        f"<b>H4 Breakout:</b>  {ht}\n"
        f"<b>M30 Retest:</b>   {rt}\n"
        f"\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\n"
        f"<b>Entry:</b>   {entry}\n"
        f"<b>SL:</b>      {sl}\n"
        f"<b>TP1:</b>     {tp1}  <i>(50% close, move SL to BE)</i>\n"
        f"<b>TP2:</b>     {tp2}  <i>(let rest run)</i>\n"
        f"<b>Risk/pt:</b> {risk}\n"
        f"\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\u2014\n"
        f"<i>H4 breakout confirmed + M30 retest entry</i>"
    )


def build_headsup(symbol, direction, broken_level, now) -> str:
    icon           = "📈" if direction == "BULL" else "📉"
    direction_text = "BULLISH" if direction == "BULL" else "BEARISH"
    return (
        f"{icon} <b>{direction_text} BREAKOUT — {symbol}</b>\n"
        f"<b>Level:</b> {broken_level}\n"
        f"<b>Time:</b>  {now}\n"
        f"<i>Watching for M30 retest entry...</i>"
    )


# =============================================================================
#  WEBSOCKET
# =============================================================================
async def fetch_candles(ws, symbol, granularity, count) -> Optional[pd.DataFrame]:
    await ws.send(json.dumps({
        "ticks_history":     symbol,
        "adjust_start_time": 1,
        "count":             count,
        "end":               "latest",
        "style":             "candles",
        "granularity":       granularity,
    }))
    resp = json.loads(await ws.recv())
    if "error" in resp or not resp.get("candles"):
        log.error("%s fetch error (%ds): %s", symbol, granularity,
                  resp.get("error", {}).get("message", "no data"))
        return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    df.drop(columns=["epoch"], inplace=True)
    return df


async def fetch_symbol_data(symbol):
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            h4  = await fetch_candles(ws, symbol, CFG.h4_tf,  CFG.h4_count)
            m30 = await fetch_candles(ws, symbol, CFG.m30_tf, CFG.m30_count)
            return h4, m30
    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("%s connection error: %s", symbol, e)
        return None, None


# =============================================================================
#  INDICATORS
# =============================================================================
def atr(df, n=14):
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"]  - df["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


def body_ratio(df):
    rng = (df["High"] - df["Low"]).replace(0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng


# =============================================================================
#  BREAKOUT DETECTION — H4
# =============================================================================
def detect_h4_breakout(h4: pd.DataFrame) -> dict:
    df = h4.copy()
    df["BR"]  = body_ratio(df)
    df["ATR"] = atr(df, CFG.atr_period)

    n = CFG.swing_lookback
    df["SwingHigh"] = df["High"].shift(1).rolling(n).max()
    df["SwingLow"]  = df["Low"].shift(1).rolling(n).min()

    last = df.iloc[-1]
    prev = df.iloc[-2]
    min_size = last["Close"] * (CFG.min_breakout_pct / 100)

    # EMA trend for scoring
    df["E21"] = df["Close"].ewm(span=21, adjust=False).mean()
    df["E50"] = df["Close"].ewm(span=50, adjust=False).mean()
    last = df.iloc[-1]
    prev = df.iloc[-2]

    result = {
        "direction":    None,
        "broken_level": None,
        "breakout_bar": None,
        "atr":          float(last["ATR"]),
        "score":        0,
        "rating":       "SKIP",
    }

    if (last["Close"] > last["SwingHigh"] and
        last["BR"] >= CFG.breakout_body_pct and
        (last["Close"] - last["SwingHigh"]) >= min_size and
        prev["Close"] <= prev["SwingHigh"]):
        result["direction"]    = "BULL"
        result["broken_level"] = round(float(last["SwingHigh"]), 4)
        result["breakout_bar"] = last.name

        score = 1
        if last["BR"] >= 0.6:                                        score += 1
        if last["Close"] > last["E21"]:                              score += 1
        if last["E21"]  > last["E50"]:                               score += 1
        if (last["Close"] - last["SwingHigh"]) >= min_size * 2:      score += 1
        result["score"] = score

    elif (last["Close"] < last["SwingLow"] and
          last["BR"] >= CFG.breakout_body_pct and
          (last["SwingLow"] - last["Close"]) >= min_size and
          prev["Close"] >= prev["SwingLow"]):
        result["direction"]    = "BEAR"
        result["broken_level"] = round(float(last["SwingLow"]), 4)
        result["breakout_bar"] = last.name

        score = 1
        if last["BR"] >= 0.6:                                        score += 1
        if last["Close"] < last["E21"]:                              score += 1
        if last["E21"]  < last["E50"]:                               score += 1
        if (last["SwingLow"] - last["Close"]) >= min_size * 2:       score += 1
        result["score"] = score

    s = result["score"]
    result["rating"] = (
        "PRIME"  if s >= 5 else
        "STRONG" if s >= 4 else
        "GOOD"   if s >= 3 else
        "SKIP"
    )
    return result


# =============================================================================
#  RETEST DETECTION — M30
# =============================================================================
def detect_m30_retest(m30: pd.DataFrame, breakout: dict) -> dict:
    if not breakout["direction"]:
        return {"retest": False}

    df        = m30.copy()
    level     = breakout["broken_level"]
    tol       = level * (CFG.retest_proximity / 100)
    direction = breakout["direction"]

    if breakout["breakout_bar"] is not None:
        df = df[df.index >= breakout["breakout_bar"]]

    if df.empty:
        return {"retest": False}

    if direction == "BULL":
        retest_bars = df[
            (df["Low"] <= level + tol) &
            (df["Close"] > level)
        ]
        if not retest_bars.empty:
            rb = retest_bars.iloc[-1]
            return {
                "retest":     True,
                "retest_bar": rb.name,
                "entry":      round(float(rb["Close"]), 4),
                "retest_low": round(float(rb["Low"]), 4),
            }

    elif direction == "BEAR":
        retest_bars = df[
            (df["High"] >= level - tol) &
            (df["Close"] < level)
        ]
        if not retest_bars.empty:
            rb = retest_bars.iloc[-1]
            return {
                "retest":      True,
                "retest_bar":  rb.name,
                "entry":       round(float(rb["Close"]), 4),
                "retest_high": round(float(rb["High"]), 4),
            }

    return {"retest": False}


# =============================================================================
#  TRADE PLAN
# =============================================================================
def build_trade(breakout: dict, retest: dict) -> dict:
    if not retest.get("retest"):
        return {}

    direction = breakout["direction"]
    entry     = retest["entry"]
    buf       = breakout["atr"] * 0.5

    if direction == "BULL":
        sl   = round(retest["retest_low"] - buf, 4)
        risk = round(entry - sl, 4)
        tp1  = round(entry + risk * 1.0, 4)
        tp2  = round(entry + risk * CFG.rr_ratio, 4)
    else:
        sl   = round(retest["retest_high"] + buf, 4)
        risk = round(sl - entry, 4)
        tp1  = round(entry - risk * 1.0, 4)
        tp2  = round(entry - risk * CFG.rr_ratio, 4)

    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "risk": risk}


# =============================================================================
#  SCAN ALL SYMBOLS
# =============================================================================
async def scan_all():
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    print(f"\nBreakout Scanner | {now}", flush=True)
    print(f"Scanning: {', '.join(CFG.symbols)}", flush=True)

    for symbol in CFG.symbols:
        try:
            h4, m30 = await fetch_symbol_data(symbol)
            if h4 is None or m30 is None:
                continue

            breakout = detect_h4_breakout(h4)

            if not breakout["direction"]:
                print(f"\n  {symbol} | {now}", flush=True)
                print(f"  H4: No breakout detected", flush=True)
                await asyncio.sleep(2)
                continue

            direction = breakout["direction"]
            level     = breakout["broken_level"]

            print(f"\n  {symbol} | {now}", flush=True)
            print(f"  H4 Breakout: {direction} at {level}  |  {breakout["rating"]} ({breakout["score"]}/5)", flush=True)

            retest = detect_m30_retest(m30, breakout)
            trade  = build_trade(breakout, retest) if retest.get("retest") else {}

            if retest.get("retest") and trade:
                print(f"  M30 Retest: CONFIRMED", flush=True)
                print(f"  Entry: {trade['entry']}  SL: {trade['sl']}", flush=True)
                print(f"  TP1: {trade['tp1']}  TP2: {trade['tp2']}", flush=True)

                # Check cooldown before sending
                if is_duplicate(symbol, direction, level):
                    print(f"  Telegram: SKIPPED (duplicate — same level sent recently)", flush=True)
                else:
                    msg = build_alert(
                        symbol       = symbol,
                        direction    = direction,
                        broken_level = level,
                        entry        = trade["entry"],
                        sl           = trade["sl"],
                        tp1          = trade["tp1"],
                        tp2          = trade["tp2"],
                        risk         = trade["risk"],
                        h4_time      = breakout["breakout_bar"],
                        retest_time  = retest["retest_bar"],
                        score        = breakout["score"],
                        rating       = breakout["rating"],
                    )
                    send_telegram(msg)
                    mark_sent(symbol, direction, level)

            else:
                print(f"  M30 Retest: Watching... level {level}", flush=True)

                # Heads-up alert also has cooldown
                if not is_duplicate(symbol, direction + "_HEADSUP", level):
                    send_telegram(build_headsup(symbol, direction, level, now))
                    mark_sent(symbol, direction + "_HEADSUP", level)
                else:
                    print(f"  Heads-up: SKIPPED (already sent recently)", flush=True)

            await asyncio.sleep(2)

        except Exception as e:
            log.error("Error scanning %s: %s", symbol, e)
            continue


async def main():
    print("Deriv Breakout Scanner | V75, V75 1s, V10, V25", flush=True)
    print("H4 Breakout + M30 Retest | Cooldown: 8 hours per level", flush=True)

    if CFG.live_mode:
        try:
            while True:
                await scan_all()
                await asyncio.sleep(1800)
        except KeyboardInterrupt:
            print("Stopped.")
            sys.exit(0)
    else:
        await scan_all()

if __name__ == "__main__":
    asyncio.run(main())
                    
