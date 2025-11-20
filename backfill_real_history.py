import os
import requests
import json
from datetime import datetime, timedelta

API_KEY = os.environ.get("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_series_history(series_id, start_date, api_key):
    """
    Fetch full observation history for a FRED series from start_date to today.
    Returns a list of (date, value) tuples sorted ascending by date.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.strftime("%Y-%m-%d"),
        "sort_order": "asc",
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    observations = []
    for obs in data.get("observations", []):
        val = obs.get("value")
        if val is None or val == ".":
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        d = datetime.strptime(obs["date"], "%Y-%m-%d").date()
        observations.append((d, v))

    observations.sort(key=lambda x: x[0])
    return observations


def compute_risk(yield_10y, yield_2y, unemployment, initial_claims,
                 gdp_growth, industrial_prod, consumer_sentiment,
                 credit_spread, housing_starts):
    """
    Exact same logic as in update-data.yml, but as a reusable function.
    Returns (risk_score, yield_curve_value).
    """
    # Base risk
    risk = 15.0

    # Yield curve (10Y - 2Y)
    yc = None
    if yield_10y is not None and yield_2y is not None:
        try:
            yc = float(yield_10y) - float(yield_2y)
        except Exception:
            yc = None

    if yc is not None:
        if yc < -0.5:
            risk += 25
        elif yc < 0:
            risk += abs(yc) * 30
        elif yc > 0.5:
            risk -= 5

    # Unemployment
    if unemployment is not None:
        u_rate = float(unemployment)
        if u_rate > 5.0:
            risk += (u_rate - 5.0) * 8
        elif u_rate < 4.0:
            risk -= 3

    # Initial claims
    if initial_claims is not None:
        claims = float(initial_claims)
        if claims > 300000:
            risk += (claims - 300000) / 10000.0

    # GDP
    if gdp_growth is not None:
        gdp = float(gdp_growth)
        if gdp < 0:
            risk += 15
        elif gdp < 2.0:
            risk += (2.0 - gdp) * 5

    # Consumer sentiment
    if consumer_sentiment is not None:
        cs = float(consumer_sentiment)
        if cs < 70:
            risk += 10
        elif cs < 90:
            risk += (90 - cs) / 4.0

    # Credit spread
    if credit_spread is not None:
        spread = float(credit_spread)
        if spread > 2.5:
            risk += (spread - 2.5) * 8

    # Clamp to [5, 95]
    risk = max(5.0, min(95.0, risk))
    return round(risk, 1), yc


def build_daily_history(start_date, end_date, api_key):
    """
    Build a daily risk history between start_date and end_date (inclusive),
    using latest-available values for each indicator from FRED.
    """
    series_map = {
        "DGS10": "yield_10y",
        "DGS2": "yield_2y",
        "UNRATE": "unemployment",
        "ICSA": "initial_claims",
        "A191RL1Q225SBEA": "gdp_growth",
        "INDPRO": "industrial_prod",
        "UMCSENT": "consumer_sentiment",
        "BAMLC0A4CBBB": "credit_spread",
        "HOUST": "housing_starts",
    }

    print(f"Fetching FRED history from {start_date} to {end_date}...")

    # 1) Fetch full history for each series
    history_series = {}
    for fred_id, label in series_map.items():
        print(f"  -> {label} ({fred_id})")
        obs = fetch_series_history(fred_id, start_date, api_key)
        history_series[label] = obs
        print(f"     {len(obs)} observations loaded")

    # 2) Prepare cursors for each series so we can "carry forward" last value
    cursors = {}
    for label, obs in history_series.items():
        cursors[label] = {"obs": obs, "idx": 0, "current": None}

    def get_value(label, date):
        c = cursors[label]
        obs = c["obs"]
        idx = c["idx"]
        current = c["current"]

        # Advance cursor while obs date <= target date
        while idx < len(obs) and obs[idx][0] <= date:
            current = obs[idx][1]
            idx += 1

        c["idx"] = idx
        c["current"] = current
        return current  # may be None if no data yet

    # 3) Loop over each calendar day and compute risk
    results = []
    d = start_date
    while d <= end_date:
        vals = {label: get_value(label, d) for label in cursors.keys()}

        risk, yc = compute_risk(
            yield_10y=vals["yield_10y"],
            yield_2y=vals["yield_2y"],
            unemployment=vals["unemployment"],
            initial_claims=vals["initial_claims"],
            gdp_growth=vals["gdp_growth"],
            industrial_prod=vals["industrial_prod"],
            consumer_sentiment=vals["consumer_sentiment"],
            credit_spread=vals["credit_spread"],
            housing_starts=vals["housing_starts"],
        )

        results.append({
            "date": d.strftime("%Y-%m-%d"),
            "risk": risk,
        })
        d += timedelta(days=1)

    return results


def main():
    if not API_KEY:
        raise SystemExit("ERROR: FRED_API_KEY environment variable is not set")

    today = datetime.today().date()
    start_date = today - timedelta(days=730)
    end_date = today  # inclusive

    history = build_daily_history(start_date, end_date, API_KEY)
    print(f"Built {len(history)} daily risk records")

    with open("risk_history.json", "w") as f:
        json.dump(history, f, indent=2)

    print("Saved risk_history.json with REAL FRED-based history")


if __name__ == "__main__":
    main()
