"""
Deriv Breakout Scanner
Pairs: V75, V75_1s, V10, V25
Timeframes: H4 (breakout detection) + M30 (retest entry)
Logic:
  - Detect breakout of H4 swing high (bullish) or swing low (bearish)
  - Confirm breakout with strong close and volume (body size)
  - Wait for M30 retest of the broken level
  - Fire Telegram alert with entry, SL and TP
"""

import asyncio, json, logging, sys, urllib.request, urllib.parse, os
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
    # Deriv pairs to scan
    symbols: list = field(default_factory=lambda: [
        "R_75",      # Volatility 75 Index
        "R_75_1S",   # Volatility 75 (1s) Index
        "R_10",      # Volatility 10 Index
        "R_25",      # Volatility 25 Index
    ])

    # Telegram
    tg_token:   str = ""
    tg_chat_id: str = ""

    # Timeframes
    h4_tf:    int = 14400   # H4 — breakout detection
    m30_tf:   int = 1800    # M30 — retest entry
    h4_count: int = 100     # H4 candles to analyse
    m30_count:int = 80      # M30 candles to analyse

    # Breakout settings
    swing_lookback:    int   = 10     # bars to define swing high/low
    breakout_body_pct: float = 0.5    # breakout candle body must be >= 50% of range
    retest_proximity:  float = 0.3    # M30 price within 0.3% of broken level = retest
    min_breakout_pct:  float = 0.05   # minimum breakout size as % of price

    # Trade settings
    rr_ratio:   float = 2.0   # risk reward
    atr_period: int   = 14    # ATR period for SL

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
            "parse_mode": "Markdown"
        }).encode()
        urllib.request.urlopen(
            urllib.request.Request(url, data=data), timeout=10
        )
        log.info("Telegram sent for %s", message[:30])
    except Exception as e:
        log.error("Telegram error: %s", e)


def breakout_alert(symbol, direction, broken_level,
                   entry, sl, tp1, tp2, risk,
                   h4_time, retest_time) -> str:
    icon   = "🟢 *BULLISH BREAKOUT*" if direction == "BULL" else "🔴 *BEARISH BREAKOUT*"
    action = "BUY on retest" if direction == "BULL" else "SELL on retest"
    ht     = h4_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    rt     = retest_time.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    return (
        f"{icon}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Pair:*    {symbol}\n"
        f"*Action:*  {action}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Broken Level:* {broken_level}\n"
        f"*H4 Breakout:*  {ht}\n"
        f"*M30 Retest:*   {rt}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Entry:*   {entry}\n"
        f"*SL:*      {sl}\n"
        f"*TP1:*     {tp1}  _(50% close, move SL to BE)_\n"
        f"*TP2:*     {tp2}  _(let rest run)_\n"
        f"*Risk/pt:* {risk}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_H4 breakout confirmed + M30 retest entry_"
    )


# =============================================================================
#  WEBSOCKET — fetch candles (no auth needed)
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
    """Fetch H4 and M30 data for a single symbol."""
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


def swing_high(df, n):
    """Highest high over n bars on each side."""
    return df["High"].rolling(n * 2 + 1, center=True).max()


def swing_low(df, n):
    """Lowest low over n bars on each side."""
    return df["Low"].rolling(n * 2 + 1, center=True).min()


# =============================================================================
#  BREAKOUT DETECTION — H4
# =============================================================================
def detect_h4_breakout(h4: pd.DataFrame) -> dict:
    """
    Breakout logic:
    ─────────────────
    BULLISH breakout:
      Current H4 candle closes ABOVE the swing high of the last N bars.
      Candle body must be strong (>= breakout_body_pct).
      Breakout size must exceed minimum % threshold.
      This signals end of a downtrend / start of uptrend.

    BEARISH breakout:
      Current H4 candle closes BELOW the swing low of the last N bars.
      Same body and size filters applied.
      Signals end of uptrend / start of downtrend.

    Returns the breakout direction and the broken level.
    """
    df = h4.copy()
    df["BR"]  = body_ratio(df)
    df["ATR"] = atr(df, CFG.atr_period)

    n = CFG.swing_lookback

    # Rolling swing high/low EXCLUDING current bar (shift by 1)
    df["SwingHigh"] = df["High"].shift(1).rolling(n).max()
    df["SwingLow"]  = df["Low"].shift(1).rolling(n).min()

    last = df.iloc[-1]
    prev = df.iloc[-2]

    result = {
        "direction":    None,
        "broken_level": None,
        "breakout_bar": None,
        "atr":          float(last["ATR"]),
    }

    # Minimum breakout size
    min_size = last["Close"] * (CFG.min_breakout_pct / 100)

    # Bullish breakout: close above swing high with strong body
    if (last["Close"] > last["SwingHigh"] and
        last["BR"] >= CFG.breakout_body_pct and
        (last["Close"] - last["SwingHigh"]) >= min_size and
        prev["Close"] <= prev["SwingHigh"]):   # previous bar was NOT broken yet
        result["direction"]    = "BULL"
        result["broken_level"] = round(float(last["SwingHigh"]), 4)
        result["breakout_bar"] = last.name

    # Bearish breakout: close below swing low with strong body
    elif (last["Close"] < last["SwingLow"] and
          last["BR"] >= CFG.breakout_body_pct and
          (last["SwingLow"] - last["Close"]) >= min_size and
          prev["Close"] >= prev["SwingLow"]):  # previous bar was NOT broken yet
        result["direction"]    = "BEAR"
        result["broken_level"] = round(float(last["SwingLow"]), 4)
        result["breakout_bar"] = last.name

    return result


# =============================================================================
#  RETEST DETECTION — M30
# =============================================================================
def detect_m30_retest(m30: pd.DataFrame, breakout: dict) -> dict:
    """
    After H4 breakout, watch M30 for a retest of the broken level.

    Bullish retest:
      Price pulls back DOWN to the broken swing high (now support)
      and closes back above it — entry signal.

    Bearish retest:
      Price pulls back UP to the broken swing low (now resistance)
      and closes back below it — entry signal.

    Proximity tolerance: ob_proximity_pct % around the broken level.
    """
    if not breakout["direction"]:
        return {"retest": False}

    df       = m30.copy()
    level    = breakout["broken_level"]
    tol      = level * (CFG.retest_proximity / 100)
    direction= breakout["direction"]

    # Only look at M30 bars AFTER the H4 breakout
    if breakout["breakout_bar"] is not None:
        df = df[df.index >= breakout["breakout_bar"]]

    if df.empty:
        return {"retest": False}

    result = {"retest": False}

    if direction == "BULL":
        # Retest: candle low touches the level and closes above it
        retest_bars = df[
            (df["Low"] <= level + tol) &
            (df["Close"] > level)
        ]
        if not retest_bars.empty:
            rb = retest_bars.iloc[-1]
            result = {
                "retest":       True,
                "retest_bar":   rb.name,
                "entry":        round(float(rb["Close"]), 4),
                "retest_low":   round(float(rb["Low"]), 4),
            }

    elif direction == "BEAR":
        # Retest: candle high touches the level and closes below it
        retest_bars = df[
            (df["High"] >= level - tol) &
            (df["Close"] < level)
        ]
        if not retest_bars.empty:
            rb = retest_bars.iloc[-1]
            result = {
                "retest":       True,
                "retest_bar":   rb.name,
                "entry":        round(float(rb["Close"]), 4),
                "retest_high":  round(float(rb["High"]), 4),
            }

    return result


# =============================================================================
#  TRADE PLAN
# =============================================================================
def build_trade(breakout: dict, retest: dict) -> dict:
    """
    Entry  = M30 retest close
    SL     = Below retest candle low (BULL) / Above retest candle high (BEAR)
             + 0.5x H4 ATR buffer
    TP1    = Entry + risk x 1.0  (50% close, move SL to BE)
    TP2    = Entry + risk x RR   (let rest run)
    """
    if not retest.get("retest"):
        return {}

    direction = breakout["direction"]
    entry     = retest["entry"]
    h4_atr    = breakout["atr"]
    buf       = h4_atr * 0.5

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

    return {
        "entry": entry,
        "sl":    sl,
        "tp1":   tp1,
        "tp2":   tp2,
        "risk":  risk,
    }


# =============================================================================
#  SCAN ONE SYMBOL
# =============================================================================
async def scan_symbol(symbol: str):
    log.info("Scanning %s...", symbol)
    h4, m30 = await fetch_symbol_data(symbol)

    if h4 is None or m30 is None:
        log.error("%s — data fetch failed.", symbol)
        return

    # Step 1: detect H4 breakout
    breakout = detect_h4_breakout(h4)

    if not breakout["direction"]:
        log.info("%s — No H4 breakout detected.", symbol)
        return

    log.info("%s — %s breakout at level %s",
             symbol, breakout["direction"], breakout["broken_level"])

    # Step 2: detect M30 retest
    retest = detect_m30_retest(m30, breakout)

    if not retest.get("retest"):
        log.info("%s — Breakout found but no M30 retest yet. Level: %s",
                 symbol, breakout["broken_level"])

        # Send a heads-up alert — breakout confirmed, watching for retest
        now = datetime.now(WAT).strftime("%H:%M WAT")
        icon = "📈" if breakout["direction"] == "BULL" else "📉"
        direction_text = "BULLISH" if breakout["direction"] == "BULL" else "BEARISH"
        msg = (
            f"{icon} *{direction_text} BREAKOUT DETECTED*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Pair:*   {symbol}\n"
            f"*Level:*  {breakout['broken_level']}\n"
            f"*Time:*   {now}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"_Watching for M30 retest entry..._\n"
            f"_Next alert fires when price retests the level._"
        )
        send_telegram(msg)
        return

    # Step 3: build trade plan
    trade = build_trade(breakout, retest)
    if not trade:
        return

    # Step 4: send full signal alert
    h4_time     = breakout["breakout_bar"]
    retest_time = retest["retest_bar"]

    msg = breakout_alert(
        symbol      = symbol,
        direction   = breakout["direction"],
        broken_level= breakout["broken_level"],
        entry       = trade["entry"],
        sl          = trade["sl"],
        tp1         = trade["tp1"],
        tp2         = trade["tp2"],
        risk        = trade["risk"],
        h4_time     = h4_time,
        retest_time = retest_time,
    )
    send_telegram(msg)
    log.info("%s — Signal sent! Entry: %s SL: %s TP2: %s",
             symbol, trade["entry"], trade["sl"], trade["tp2"])


# =============================================================================
#  REPORT — console output
# =============================================================================
def print_summary(symbol, breakout, retest, trade):
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    print(f"\n{'='*60}", flush=True)
    print(f"  {symbol}  |  {now}", flush=True)
    if not breakout["direction"]:
        print(f"  H4: No breakout detected", flush=True)
    else:
        print(f"  H4 Breakout: {breakout['direction']} at {breakout['broken_level']}", flush=True)
        if retest.get("retest"):
            print(f"  M30 Retest:  CONFIRMED", flush=True)
            print(f"  Entry: {trade.get('entry')}  SL: {trade.get('sl')}", flush=True)
            print(f"  TP1:   {trade.get('tp1')}  TP2: {trade.get('tp2')}", flush=True)
        else:
            print(f"  M30 Retest:  Watching... level {breakout['broken_level']}", flush=True)
    print(f"{'='*60}", flush=True)


# =============================================================================
#  MAIN SCAN LOOP
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
            retest   = detect_m30_retest(m30, breakout) if breakout["direction"] else {"retest": False}
            trade    = build_trade(breakout, retest) if retest.get("retest") else {}

            print_summary(symbol, breakout, retest, trade)

            # Send Telegram if retest confirmed
            if retest.get("retest") and trade:
                msg = breakout_alert(
                    symbol       = symbol,
                    direction    = breakout["direction"],
                    broken_level = breakout["broken_level"],
                    entry        = trade["entry"],
                    sl           = trade["sl"],
                    tp1          = trade["tp1"],
                    tp2          = trade["tp2"],
                    risk         = trade["risk"],
                    h4_time      = breakout["breakout_bar"],
                    retest_time  = retest["retest_bar"],
                )
                send_telegram(msg)

            # Send heads-up if breakout only (no retest yet)
            elif breakout["direction"] and not retest.get("retest"):
                icon = "📈" if breakout["direction"] == "BULL" else "📉"
                direction_text = "BULLISH" if breakout["direction"] == "BULL" else "BEARISH"
                msg = (
                    f"{icon} *{direction_text} BREAKOUT — {symbol}*\n"
                    f"*Level:* {breakout['broken_level']}\n"
                    f"*Time:*  {now}\n"
                    f"_Waiting for M30 retest..._"
                )
                send_telegram(msg)

            # Small delay between symbols to avoid rate limiting
            await asyncio.sleep(2)

        except Exception as e:
            log.error("Error scanning %s: %s", symbol, e)
            continue


async def main():
    print("Deriv Breakout Scanner | V75, V75 1s, V10, V25", flush=True)
    print("H4 Breakout + M30 Retest Entry", flush=True)

    if CFG.live_mode:
        try:
            while True:
                await scan_all()
                await asyncio.sleep(1800)  # re-scan every 30 min
        except KeyboardInterrupt:
            print("Stopped.")
            sys.exit(0)
    else:
        await scan_all()

if __name__ == "__main__":
    asyncio.run(main())
  
