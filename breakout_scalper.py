"""
Deriv Breakout Scalper
Pairs: R_75, 1HZ75V, R_10, R_25
Stack: H1 (trend + breakout) >> M15 (retest quality)
Logic:
  - H1 detects breakout of swing high/low
  - M15 confirms quality retest of broken level
  - ADX filter ensures trend is strong
  - Rejection wick required on M15 retest candle
  - Signal expiry: max 6 H1 bars after breakout
  - 4hr cooldown per level
  - Dynamic RR based on signal score
  - HTML Telegram alerts
"""

import asyncio, json, logging, sys, urllib.request, urllib.parse, os, time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
WAT = timezone(timedelta(hours=1))


# =============================================================================
#  CONFIG
# =============================================================================
@dataclass
class Config:
    symbols: list = field(default_factory=lambda: [
        "R_75", "1HZ75V", "R_10", "R_25"
    ])

    tg_token:   str = ""
    tg_chat_id: str = ""

    # Timeframes
    h4_tf:  int = 14400  # H4 — HTF trend bias
    h1_tf:  int = 3600   # H1 — breakout detection
    m15_tf: int = 900    # M15 — retest entry

    h4_count:  int = 50
    h1_count:  int = 80
    m15_count: int = 100

    # Breakout settings
    swing_lookback:    int   = 8
    breakout_body_pct: float = 0.45
    retest_proximity:  float = 0.2
    min_breakout_pct:  float = 0.03

    # Quality filters
    adx_period:        int   = 14
    atr_period:        int   = 14     # <-- ADD THIS LINE (Defaulting to 14)
    adx_min:           float = 18.0
    min_wick_ratio:    float = 0.25   # retest rejection wick
    max_retest_age:    int   = 6      # max H1 bars after breakout
  

    # Trade settings
    rr_min:  float = 1.5
    rr_max:  float = 2.5
    atr_sl_multiplier: float = 0.5

    cooldown_hours: int  = 4     # 4hr cooldown for scalper (shorter than swing)
    live_mode:      bool = False

    @property
    def uri(self):
        return "wss://ws.derivws.com/websockets/v3?app_id=1089"

CFG = Config()

if os.environ.get("TG_TOKEN"):   CFG.tg_token   = os.environ["TG_TOKEN"]
if os.environ.get("TG_CHAT_ID"): CFG.tg_chat_id = os.environ["TG_CHAT_ID"]


# =============================================================================
#  COOLDOWN
# =============================================================================
COOLDOWN_FILE = "scalper_cooldown.json"

def load_cooldown():
    try:
        if os.path.exists(COOLDOWN_FILE):
            with open(COOLDOWN_FILE) as f: return json.load(f)
    except Exception: pass
    return {}

def save_cooldown(data):
    try:
        with open(COOLDOWN_FILE, "w") as f: json.dump(data, f)
    except Exception as e: log.error("Cooldown save: %s", e)

def is_duplicate(symbol, direction, level):
    key = f"{symbol}_{direction}_{round(level, 2)}"
    cd  = load_cooldown()
    if key in cd:
        hrs = (time.time() - cd[key]) / 3600
        if hrs < CFG.cooldown_hours:
            log.info("Duplicate: %s (%.1fh ago)", key, hrs)
            return True
    return False

def mark_sent(symbol, direction, level):
    key = f"{symbol}_{direction}_{round(level, 2)}"
    cd  = load_cooldown()
    now = time.time()
    cd  = {k: v for k, v in cd.items() if now - v < 86400}
    cd[key] = now
    save_cooldown(cd)


# =============================================================================
#  TELEGRAM
# =============================================================================
def send_telegram(message):
    if not CFG.tg_token or not CFG.tg_chat_id:
        log.info("Telegram not configured.")
        return
    try:
        url  = f"https://api.telegram.org/bot{CFG.tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": CFG.tg_chat_id, "text": message, "parse_mode": "HTML"
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
        log.info("Telegram sent.")
    except Exception as e: log.error("Telegram error: %s", e)


def build_alert(symbol, direction, broken_level,
                entry, sl, tp1, tp2, risk, rr,
                h1_time, retest_time, score, rating, h4_bias):
    icon   = "🟢 <b>BULLISH SCALP BREAKOUT</b>" if direction == "BULL" else "🔴 <b>BEARISH SCALP BREAKOUT</b>"
    action = "BUY on M15 retest" if direction == "BULL" else "SELL on M15 retest"
    ht     = h1_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    rt     = retest_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    stars  = "🔥 PRIME" if rating=="PRIME" else "⭐⭐ STRONG" if rating=="STRONG" else "⭐ GOOD" if rating=="GOOD" else "✗ SKIP"
    return (
        f"{icon}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Pair:</b>    {symbol}\n"
        f"<b>Action:</b>  {action}\n"
        f"<b>Rating:</b>  {stars}  ({score}/6)\n"
        f"<b>H4 Bias:</b> {h4_bias}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Broken Level:</b> {broken_level}\n"
        f"<b>H1 Breakout:</b>  {ht}\n"
        f"<b>M15 Retest:</b>   {rt}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Entry:</b>   {entry}\n"
        f"<b>SL:</b>      {sl}\n"
        f"<b>TP1:</b>     {tp1}  <i>(close 50%, move SL to BE)</i>\n"
        f"<b>TP2:</b>     {tp2}  <i>(let rest run  1:{rr})</i>\n"
        f"<b>Risk/pt:</b> {risk}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<i>H4 bias + H1 breakout + M15 quality retest confirmed</i>"
    )

def build_headsup(symbol, direction, level, now, h4_bias):
    icon = "📈" if direction == "BULL" else "📉"
    dt   = "BULLISH" if direction == "BULL" else "BEARISH"
    return (
        f"{icon} <b>{dt} SCALP BREAKOUT — {symbol}</b>\n"
        f"<b>H1 Level:</b> {level}\n"
        f"<b>H4 Bias:</b>  {h4_bias}\n"
        f"<b>Time:</b>     {now}\n"
        f"<i>Watching for M15 quality retest...</i>"
    )


# =============================================================================
#  WEBSOCKET
# =============================================================================
async def fetch_candles(ws, symbol, granularity, count):
    await ws.send(json.dumps({
        "ticks_history": symbol, "adjust_start_time": 1,
        "count": count, "end": "latest", "style": "candles", "granularity": granularity,
    }))
    resp = json.loads(await ws.recv())
    if "error" in resp or not resp.get("candles"):
        log.error("%s fetch error (%ds): %s", symbol, granularity,
                  resp.get("error", {}).get("message", "no data"))
        return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(
        pd.to_numeric, errors="coerce")
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    df.drop(columns=["epoch"], inplace=True)
    return df


async def fetch_symbol_data(symbol):
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            h4  = await fetch_candles(ws, symbol, CFG.h4_tf,  CFG.h4_count)
            h1  = await fetch_candles(ws, symbol, CFG.h1_tf,  CFG.h1_count)
            m15 = await fetch_candles(ws, symbol, CFG.m15_tf, CFG.m15_count)
            return h4, h1, m15
    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("%s connection error: %s", symbol, e)
        return None, None, None


# =============================================================================
#  INDICATORS
# =============================================================================
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def atr_s(df, n=14):
    tr = pd.concat([df["High"]-df["Low"],
                    (df["High"]-df["Close"].shift()).abs(),
                    (df["Low"] -df["Close"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

def body_ratio(df):
    rng = (df["High"] - df["Low"]).replace(0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng

def calc_adx(df, n=14):
    up   = df["High"].diff()
    down = -df["Low"].diff()
    pdm  = pd.Series(0.0, index=df.index)
    ndm  = pd.Series(0.0, index=df.index)
    pdm[up > down] = up[up > down].clip(lower=0)
    ndm[down > up] = down[down > up].clip(lower=0)
    atr_ = atr_s(df, n)
    pdi  = 100 * pdm.ewm(span=n, adjust=False).mean() / atr_.replace(0, float("nan"))
    ndi  = 100 * ndm.ewm(span=n, adjust=False).mean() / atr_.replace(0, float("nan"))
    dx   = 100 * (pdi-ndi).abs() / (pdi+ndi).replace(0, float("nan"))
    return dx.ewm(span=n, adjust=False).mean()


# =============================================================================
#  H4 BIAS
# =============================================================================
def get_h4_bias(h4):
    df = h4.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    a, b = df.iloc[-1], df.iloc[-3]
    if a["E21"] > a["E50"] and a["Close"] > a["E21"] and a["E21"] > b["E21"]: return "BULLISH"
    if a["E21"] < a["E50"] and a["Close"] < a["E21"] and a["E21"] < b["E21"]: return "BEARISH"
    return "NEUTRAL"


# =============================================================================
#  H1 BREAKOUT DETECTION
# =============================================================================
def detect_h1_breakout(h1):
    df = h1.copy()
    df["BR"]  = body_ratio(df)
    df["ATR"] = atr_s(df, CFG.atr_period)
    df["ADX"] = calc_adx(df, CFG.adx_period)
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)

    n = CFG.swing_lookback
    df["SwingHigh"] = df["High"].shift(1).rolling(n).max()
    df["SwingLow"]  = df["Low"].shift(1).rolling(n).min()

    last     = df.iloc[-1]
    prev     = df.iloc[-2]
    min_size = last["Close"] * (CFG.min_breakout_pct / 100)
    adx_val  = float(last["ADX"])

    result = {
        "direction": None, "broken_level": None,
        "breakout_bar": None, "atr": float(last["ATR"]),
        "score": 0, "rating": "SKIP", "adx": adx_val,
    }

    if (last["Close"] > last["SwingHigh"] and
        last["BR"] >= CFG.breakout_body_pct and
        (last["Close"] - last["SwingHigh"]) >= min_size and
        prev["Close"] <= prev["SwingHigh"]):

        result["direction"]    = "BULL"
        result["broken_level"] = round(float(last["SwingHigh"]), 4)
        result["breakout_bar"] = last.name

        score = 1
        if last["BR"]    >= 0.6:          score += 1
        if last["Close"] > last["E21"]:   score += 1
        if last["E21"]   > last["E50"]:   score += 1
        if adx_val       >= CFG.adx_min:  score += 1
        if (last["Close"] - last["SwingHigh"]) >= min_size * 2: score += 1
        result["score"] = score

    elif (last["Close"] < last["SwingLow"] and
          last["BR"] >= CFG.breakout_body_pct and
          (last["SwingLow"] - last["Close"]) >= min_size and
          prev["Close"] >= prev["SwingLow"]):

        result["direction"]    = "BEAR"
        result["broken_level"] = round(float(last["SwingLow"]), 4)
        result["breakout_bar"] = last.name

        score = 1
        if last["BR"]    >= 0.6:          score += 1
        if last["Close"] < last["E21"]:   score += 1
        if last["E21"]   < last["E50"]:   score += 1
        if adx_val       >= CFG.adx_min:  score += 1
        if (last["SwingLow"] - last["Close"]) >= min_size * 2: score += 1
        result["score"] = score

    s = result["score"]
    result["rating"] = "PRIME" if s>=6 else "STRONG" if s>=5 else "GOOD" if s>=3 else "SKIP"
    return result


# =============================================================================
#  M15 QUALITY RETEST
# =============================================================================
def detect_m15_retest(m15, breakout):
    """
    Quality M15 retest requires:
    1. Price touches broken H1 level on M15
    2. Candle closes back on the correct side
    3. Rejection wick >= min_wick_ratio (real institutional rejection)
    4. Must occur within max_retest_age H1 bars of breakout
    """
    if not breakout["direction"]: return {"retest": False}

    df        = m15.copy()
    level     = breakout["broken_level"]
    tol       = level * (CFG.retest_proximity / 100)
    direction = breakout["direction"]

    if breakout["breakout_bar"] is not None:
        df = df[df.index >= breakout["breakout_bar"]]

    # Expiry: max_retest_age H1 bars = max_retest_age * 4 M15 bars
    if len(df) > CFG.max_retest_age * 4:
        return {"retest": False, "expired": True}

    if df.empty: return {"retest": False}

    if direction == "BULL":
        candidates = df[(df["Low"] <= level + tol) & (df["Close"] > level)]
        for _, rb in candidates.iloc[::-1].iterrows():
            rng        = rb["High"] - rb["Low"]
            wick       = level - rb["Low"]
            wick_ratio = wick / rng if rng > 0 else 0
            if wick_ratio >= CFG.min_wick_ratio:
                return {
                    "retest":      True,
                    "retest_bar":  rb.name,
                    "entry":       round(float(rb["Close"]), 4),
                    "retest_low":  round(float(rb["Low"]),   4),
                    "wick_ratio":  round(wick_ratio, 3),
                }

    elif direction == "BEAR":
        candidates = df[(df["High"] >= level - tol) & (df["Close"] < level)]
        for _, rb in candidates.iloc[::-1].iterrows():
            rng        = rb["High"] - rb["Low"]
            wick       = rb["High"] - level
            wick_ratio = wick / rng if rng > 0 else 0
            if wick_ratio >= CFG.min_wick_ratio:
                return {
                    "retest":      True,
                    "retest_bar":  rb.name,
                    "entry":       round(float(rb["Close"]),  4),
                    "retest_high": round(float(rb["High"]),   4),
                    "wick_ratio":  round(wick_ratio, 3),
                }

    return {"retest": False}


# =============================================================================
#  TRADE PLAN
# =============================================================================
def build_trade(breakout, retest):
    if not retest.get("retest"): return {}

    direction = breakout["direction"]
    entry     = retest["entry"]
    buf       = breakout["atr"] * CFG.atr_sl_multiplier
    score     = breakout["score"]
    rr        = round(CFG.rr_min + (score / 6) * (CFG.rr_max - CFG.rr_min), 2)

    if direction == "BULL":
        sl   = round(retest["retest_low"] - buf, 4)
        risk = round(entry - sl, 4)
        tp1  = round(entry + risk * 1.0, 4)
        tp2  = round(entry + risk * rr,  4)
    else:
        sl   = round(retest["retest_high"] + buf, 4)
        risk = round(sl - entry, 4)
        tp1  = round(entry - risk * 1.0, 4)
        tp2  = round(entry - risk * rr,  4)

    return {"entry":entry, "sl":sl, "tp1":tp1, "tp2":tp2, "risk":risk, "rr":rr}


# =============================================================================
#  SCAN ONE SYMBOL
# =============================================================================
async def scan_symbol(symbol):
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    h4, h1, m15 = await fetch_symbol_data(symbol)
    if any(x is None for x in (h4, h1, m15)):
        log.error("%s — fetch failed.", symbol); return

    h4_bias  = get_h4_bias(h4)
    breakout = detect_h1_breakout(h1)
    direction = breakout["direction"]
    level     = breakout.get("broken_level")
    rating    = breakout["rating"]
    score     = breakout["score"]
    adx_val   = breakout["adx"]

    print(f"\n  {symbol}  |  {now}", flush=True)
    print(f"  H4 Bias: {h4_bias}  |  ADX: {adx_val:.1f}", flush=True)

    if not direction:
        print(f"  H1: No breakout detected", flush=True)
        return

    print(f"  H1: {direction} breakout at {level}  |  {rating} ({score}/6)", flush=True)

    # H4 alignment check
    if direction == "BULL" and h4_bias != "BULLISH":
        print(f"  SKIPPED — H4 bias is {h4_bias} (needs BULLISH)", flush=True)
        return
    if direction == "BEAR" and h4_bias != "BEARISH":
        print(f"  SKIPPED — H4 bias is {h4_bias} (needs BEARISH)", flush=True)
        return

    # ADX filter
    if adx_val < CFG.adx_min:
        print(f"  SKIPPED — ADX {adx_val:.1f} too weak (min {CFG.adx_min})", flush=True)
        return

    # M15 quality retest
    retest = detect_m15_retest(m15, breakout)

    if retest.get("expired"):
        print(f"  Retest EXPIRED — signal too old", flush=True)
        return

    if not retest.get("retest"):
        print(f"  No quality M15 retest yet. Watching {level}...", flush=True)
        hu_key = direction + "_HU"
        if not is_duplicate(symbol, hu_key, level):
            send_telegram(build_headsup(symbol, direction, level, now, h4_bias))
            mark_sent(symbol, hu_key, level)
        return

    wick_ratio = retest.get("wick_ratio", 0)
    print(f"  M15 Retest: CONFIRMED  wick={wick_ratio:.2f}", flush=True)

    trade = build_trade(breakout, retest)
    if not trade: return

    rr_used = trade["rr"]
    print(f"  Entry: {trade['entry']}  SL: {trade['sl']}  RR: 1:{rr_used}", flush=True)
    print(f"  TP1: {trade['tp1']}  TP2: {trade['tp2']}", flush=True)

    if not is_duplicate(symbol, direction, level):
        msg = build_alert(
            symbol=symbol, direction=direction, broken_level=level,
            entry=trade["entry"], sl=trade["sl"],
            tp1=trade["tp1"], tp2=trade["tp2"],
            risk=trade["risk"], rr=rr_used,
            h1_time=breakout["breakout_bar"],
            retest_time=retest["retest_bar"],
            score=score, rating=rating, h4_bias=h4_bias,
        )
        send_telegram(msg)
        mark_sent(symbol, direction, level)
    else:
        print(f"  Telegram: SKIPPED (duplicate)", flush=True)


# =============================================================================
#  MAIN
# =============================================================================
async def scan_all():
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    print(f"\nBreakout Scalper | {now} | {', '.join(CFG.symbols)}", flush=True)
    for symbol in CFG.symbols:
        try:
            await scan_symbol(symbol)
            await asyncio.sleep(2)
        except Exception as e:
            log.error("Error scanning %s: %s", symbol, e)


async def main():
    print("Deriv Breakout Scalper | H4 Bias + H1 Breakout + M15 Retest", flush=True)
    if CFG.live_mode:
        try:
            while True:
                await scan_all()
                await asyncio.sleep(900)  # re-scan every 15 min
        except KeyboardInterrupt:
            print("Stopped."); sys.exit(0)
    else:
        await scan_all()

if __name__ == "__main__":
    asyncio.run(main())
