# Collect the float shares
# Collect earnings dates

# scanner_handler.py
import time
from db import get_symbol, upsert_symbol, init_db
from yf_fetcher import fetch_float_and_earnings

# Simple in-memory throttle so scanner hits don't spam Yahoo
_recently_fetched = {}
FETCH_COOLDOWN_SECONDS = 300  # don't re-fetch same symbol within 5 min

def on_scanner_hit(symbol: str) -> dict:
    """
    Called each time a symbol appears in your IB scanner.
    Returns merged data immediately.
    """
    row = get_symbol(symbol)

    if row is None or _needs_realtime_fetch(symbol, row):
        print(f"[RT] Fetching {symbol} from Yahoo...")
        data = fetch_float_and_earnings(symbol)

        if not data["error"]:
            upsert_symbol(
                symbol,
                float_shares=data["float_shares"],
                next_earnings=data["next_earnings"],
                last_earnings=data["last_earnings"]
            )
            _recently_fetched[symbol] = time.time()
            row = get_symbol(symbol)
        else:
            print(f"[RT] Error fetching {symbol}: {data['error']}")
    else:
        print(f"[RT] {symbol} served from cache")

    return _row_to_dict(row)

def _needs_realtime_fetch(symbol, row) -> bool:
    # If we fetched it recently in this session, skip
    if symbol in _recently_fetched:
        if time.time() - _recently_fetched[symbol] < FETCH_COOLDOWN_SECONDS:
            return False
    # If no float data yet, always fetch
    if row[1] is None:  # float_shares column
        return True
    return False

def _row_to_dict(row):
    if row is None:
        return {}
    return {
        "symbol": row[0],
        "float_shares": row[1],
        "next_earnings": row[2],
        "last_earnings": row[3],
        "updated_at": row[4]
    }