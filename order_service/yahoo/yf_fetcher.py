# yf_fetcher.py
import yfinance as yf

def fetch_float_and_earnings(symbol: str) -> dict:
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        float_shares = info.get("floatShares")

        cal = ticker.calendar  # returns a dict or DataFrame depending on version
        next_earnings = None
        last_earnings = None

        if cal is not None:
            # Newer yfinance returns a dict
            if isinstance(cal, dict):
                earnings_dates = cal.get("Earnings Date", [])
                if earnings_dates:
                    next_earnings = str(earnings_dates[0])
            else:
                # Older versions return a DataFrame
                if "Earnings Date" in cal.index:
                    next_earnings = str(cal.loc["Earnings Date"].iloc[0])

        # Last earnings from quarterly financials
        financials = ticker.quarterly_financials
        if financials is not None and not financials.empty:
            last_earnings = financials.columns[0]

        return {
            "symbol": symbol,
            "float_shares": float_shares,
            "next_earnings": next_earnings,
            "last_earnings": last_earnings,
            "error": None
        }

    except Exception as e:
        return {"symbol": symbol, "error": str(e)}