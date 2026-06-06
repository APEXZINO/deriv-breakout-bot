"""
Single-pass version of the bot for GitHub Actions.
Scans all symbols once and exits (cron handles scheduling).
"""

import asyncio
import logging
import os
import sys

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))
from bot import SYMBOLS, scan_symbol, format_alert, send_telegram

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

seen: set = set()   # fresh each run — GitHub Actions is stateless


async def main():
    log.info("▶ Single-pass breakout scan starting ...")
    for name, code in SYMBOLS.items():
        try:
            signal = await scan_symbol(name, code, seen)
            if signal:
                log.info(f"🚨 BREAKOUT detected: {name} {signal.direction}")
                send_telegram(format_alert(signal))
            else:
                log.info(f"✓ {name}: no breakout.")
        except Exception as e:
            log.error(f"Error scanning {name}: {e}")
        await asyncio.sleep(2)
    log.info("✅ Scan complete.")


if __name__ == "__main__":
    asyncio.run(main())
  
