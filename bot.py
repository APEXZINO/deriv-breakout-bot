"""
Deriv Volatility Index Breakout Bot
Monitors V75, V75 1s, and V25 on H4 for breakouts
Sends Telegram alerts with M30 retest entry zones
"""

import asyncio
import logging
import os
import json
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
import websockets
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
DERIV_APP_ID    = os.getenv("DERIV_APP_ID", "1089")           # Deriv demo app id
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")
DERIV_WS_URL    = "wss://ws.binaryws.com/websockets/v3"

SYMBOLS = {
    "V75":    "R_75",     # Volatility 75 Index
    "V75_1s": "1HZ75V",   # Volatility 75 (1s) Index
    "V25":    "R_25",     # Volatility 25 Index
}

H4_GRANULARITY  = 14400   # 4 hours in seconds
M30_GRANULARITY = 1800    # 30 minutes in seconds
H4_CANDLES      = 50      # history candles for H4 swing detection
M30_CANDLES     = 20      # M30 candles for retest zone
SWING_LOOKBACK  = 10      # bars each side for swing high/low
ATR_PERIOD      = 14
ATR_MULTIPLIER  = 0.3     # breakout must exceed swing by ATR * multiplier
POLL_INTERVAL   = 300     # seconds between checks (5 min)

# ─── Data classes ─────────────────────────────────────────────────────────────
@dataclass
class SwingLevel:
    price: float
    bar_index: int
    kind: str  # "high" or "low"

@dataclass
class BreakoutSignal:
    symbol_name: str
    symbol_code: str
    direction: str          # "BULLISH" or "BEARISH"
    breakout_price: float
    swing_level: float
    atr: float
    h4_close: float
    entry_zone_high: float
    entry_zone_low: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    timestamp: str
    trend: str              # "uptrend" or "downtrend"


# ─── Telegram ─────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.warning("Telegram credentials not set — skipping alert.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram alert sent.")
        return True
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def format_alert(sig: BreakoutSignal) -> str:
    emoji = "🟢🚀" if sig.direction == "BULLISH" else "🔴📉"
    arrow = "⬆️ BULLISH BREAKOUT" if sig.direction == "BULLISH" else "⬇️ BEARISH BREAKOUT"

    return f"""
{emoji} <b>DERIV BREAKOUT ALERT</b> {emoji}

<b>Pair:</b> {sig.symbol_name}
<b>Signal:</b> {arrow}
<b>Trend Context:</b> {sig.trend.upper()}
<b>Time (UTC):</b> {sig.timestamp}

━━━━━━━━━━━━━━━━━━
📊 <b>H4 BREAKOUT DETAILS</b>
━━━━━━━━━━━━━━━━━━
• Broken Level : <code>{sig.swing_level:.5f}</code>
• H4 Close     : <code>{sig.h4_close:.5f}</code>
• ATR (14)     : <code>{sig.atr:.5f}</code>

━━━━━━━━━━━━━━━━━━
🎯 <b>M30 RETEST ENTRY ZONE</b>
━━━━━━━━━━━━━━━━━━
• Entry High   : <code>{sig.entry_zone_high:.5f}</code>
• Entry Low    : <code>{sig.entry_zone_low:.5f}</code>
• Stop Loss    : <code>{sig.stop_loss:.5f}</code>
• TP 1 (1:1)   : <code>{sig.take_profit_1:.5f}</code>
• TP 2 (1:2)   : <code>{sig.take_profit_2:.5f}</code>

━━━━━━━━━━━━━━━━━━
⚠️ <i>Wait for price to RETEST the broken level on M30 before entering. 
Confirm with a rejection candle (pin bar / engulfing).</i>
━━━━━━━━━━━━━━━━━━
<i>Bot: Deriv Breakout Sentinel v1.0</i>
""".strip()


# ─── Deriv WebSocket fetch ─────────────────────────────────────────────────────
async def fetch_candles(symbol: str, granularity: int, count: int) -> list[dict]:
    """Fetch OHLC candles from Deriv WS API."""
    uri = f"{DERIV_WS_URL}?app_id={DERIV_APP_ID}"
    request = {
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "granularity": granularity,
        "style": "candles"
    }
    try:
        async with websockets.connect(uri, ping_interval=20) as ws:
            await ws.send(json.dumps(request))
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            data = json.loads(raw)
            if "error" in data:
                log.error(f"Deriv API error for {symbol}: {data['error']['message']}")
                return []
            return data.get("candles", [])
    except Exception as e:
        log.error(f"WebSocket error fetching {symbol}: {e}")
        return []


def candles_to_df(candles: list[dict]) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    df["epoch"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.rename(columns={"epoch": "time", "open": "open", "high": "high",
                        "low": "low", "close": "close"}, inplace=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ─── Technical Analysis ───────────────────────────────────────────────────────
def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    return float(atr.iloc[-1])


def find_swings(df: pd.DataFrame, lookback: int = SWING_LOOKBACK) -> tuple[list[SwingLevel], list[SwingLevel]]:
    """Return lists of swing highs and swing lows."""
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        window_h = df["high"].iloc[i - lookback: i + lookback + 1]
        if df["high"].iloc[i] == window_h.max():
            highs.append(SwingLevel(df["high"].iloc[i], i, "high"))
        window_l = df["low"].iloc[i - lookback: i + lookback + 1]
        if df["low"].iloc[i] == window_l.min():
            lows.append(SwingLevel(df["low"].iloc[i], i, "low"))
    return highs, lows


def detect_trend(df: pd.DataFrame) -> str:
    """Simple trend detection using HH/HL vs LH/LL on last 3 swings."""
    highs, lows = find_swings(df)
    if len(highs) >= 2 and len(lows) >= 2:
        last_highs = sorted(highs, key=lambda x: x.bar_index)[-2:]
        last_lows  = sorted(lows,  key=lambda x: x.bar_index)[-2:]
        hh = last_highs[1].price > last_highs[0].price
        hl = last_lows[1].price  > last_lows[0].price
        ll = last_lows[1].price  < last_lows[0].price
        lh = last_highs[1].price < last_highs[0].price
        if hh and hl:
            return "uptrend"
        if ll and lh:
            return "downtrend"
    return "ranging"


def detect_breakout(df: pd.DataFrame, atr: float) -> Optional[tuple[str, float, str]]:
    """
    Returns (direction, broken_level, trend) or None.
    Bullish: price closes above recent swing high.
    Bearish: price closes below recent swing low.
    """
    if len(df) < SWING_LOOKBACK * 2 + 5:
        return None

    trend = detect_trend(df)
    highs, lows = find_swings(df)
    last_close = df["close"].iloc[-1]

    # Use last completed candle (not the current forming one)
    analysis_df = df.iloc[:-1]  # exclude last bar
    conf_close  = analysis_df["close"].iloc[-1]

    # ── Bullish breakout: close above swing high
    recent_highs = [h for h in highs if h.bar_index < len(analysis_df) - 1]
    if recent_highs:
        last_swing_high = max(recent_highs, key=lambda x: x.bar_index)
        if conf_close > last_swing_high.price + (ATR_MULTIPLIER * atr):
            return ("BULLISH", last_swing_high.price, trend)

    # ── Bearish breakout: close below swing low
    recent_lows = [l for l in lows if l.bar_index < len(analysis_df) - 1]
    if recent_lows:
        last_swing_low = min(recent_lows, key=lambda x: x.bar_index)
        if conf_close < last_swing_low.price - (ATR_MULTIPLIER * atr):
            return ("BEARISH", last_swing_low.price, trend)

    return None


def compute_entry_zones(
    direction: str,
    swing_level: float,
    atr: float,
    m30_df: pd.DataFrame
) -> tuple[float, float, float, float, float]:
    """
    Returns: entry_high, entry_low, stop_loss, tp1, tp2
    Entry zone = retest of broken level ± 0.2 ATR buffer
    """
    buf = atr * 0.2
    risk = atr * 1.0   # SL = 1 ATR from broken level

    if direction == "BULLISH":
        entry_high = swing_level + buf
        entry_low  = swing_level - buf
        stop_loss  = swing_level - risk
        rr_dist    = entry_high - stop_loss
        tp1        = entry_high + rr_dist        # 1:1
        tp2        = entry_high + (rr_dist * 2)  # 1:2
    else:
        entry_high = swing_level + buf
        entry_low  = swing_level - buf
        stop_loss  = swing_level + risk
        rr_dist    = stop_loss - entry_low
        tp1        = entry_low - rr_dist         # 1:1
        tp2        = entry_low - (rr_dist * 2)   # 1:2

    return entry_high, entry_low, stop_loss, tp1, tp2


# ─── Core scanner ─────────────────────────────────────────────────────────────
async def scan_symbol(name: str, code: str, seen_breakouts: set) -> Optional[BreakoutSignal]:
    log.info(f"Scanning {name} ({code}) ...")

    # Fetch H4 data
    h4_candles = await fetch_candles(code, H4_GRANULARITY, H4_CANDLES)
    h4_df = candles_to_df(h4_candles)
    if h4_df.empty or len(h4_df) < SWING_LOOKBACK * 2 + 5:
        log.warning(f"{name}: insufficient H4 data ({len(h4_df)} bars)")
        return None

    atr = compute_atr(h4_df, ATR_PERIOD)
    result = detect_breakout(h4_df, atr)
    if result is None:
        log.info(f"{name}: no breakout detected.")
        return None

    direction, swing_level, trend = result

    # Deduplicate: skip if same symbol+direction already alerted in this session
    sig_key = f"{name}_{direction}_{round(swing_level, 3)}"
    if sig_key in seen_breakouts:
        log.info(f"{name}: breakout already alerted ({sig_key}), skipping.")
        return None
    seen_breakouts.add(sig_key)

    # Fetch M30 data for entry zone
    m30_candles = await fetch_candles(code, M30_GRANULARITY, M30_CANDLES)
    m30_df = candles_to_df(m30_candles)

    entry_h, entry_l, sl, tp1, tp2 = compute_entry_zones(direction, swing_level, atr, m30_df)
    h4_close = float(h4_df["close"].iloc[-2])  # last confirmed close

    signal = BreakoutSignal(
        symbol_name    = name,
        symbol_code    = code,
        direction      = direction,
        breakout_price = float(h4_df["close"].iloc[-2]),
        swing_level    = swing_level,
        atr            = atr,
        h4_close       = h4_close,
        entry_zone_high= entry_h,
        entry_zone_low = entry_l,
        stop_loss      = sl,
        take_profit_1  = tp1,
        take_profit_2  = tp2,
        timestamp      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        trend          = trend
    )
    return signal


async def run_bot():
    log.info("═══════════════════════════════════════")
    log.info("  Deriv Breakout Sentinel — STARTED")
    log.info(f"  Symbols : {', '.join(SYMBOLS.keys())}")
    log.info(f"  H4 scan every {POLL_INTERVAL}s")
    log.info("═══════════════════════════════════════")

    seen_breakouts: set = set()

    while True:
        for name, code in SYMBOLS.items():
            try:
                signal = await scan_symbol(name, code, seen_breakouts)
                if signal:
                    log.info(f"🚨 BREAKOUT: {name} {signal.direction} @ {signal.swing_level}")
                    alert = format_alert(signal)
                    send_telegram(alert)
                    # Also log to file
                    with open("signals.log", "a") as f:
                        f.write(f"{signal.timestamp} | {name} | {signal.direction} | "
                                f"Swing={signal.swing_level:.5f} | "
                                f"Entry={signal.entry_zone_low:.5f}-{signal.entry_zone_high:.5f} | "
                                f"SL={signal.stop_loss:.5f}\n")
            except Exception as e:
                log.error(f"Error scanning {name}: {e}")

            await asyncio.sleep(2)  # brief pause between symbols

        log.info(f"Scan complete. Next scan in {POLL_INTERVAL}s ...")
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run_bot())
