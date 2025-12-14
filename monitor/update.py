import json
import os
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
from gdeltdoc import GdeltDoc, Filters

DEFAULT_TICKERS = ["NTSK", "KLAR", "FIG", "NAVN"]

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def pct_change(open_price: float, last_price: float) -> float:
    if open_price is None or last_price is None or open_price == 0:
        return None
    return (last_price - open_price) / open_price * 100.0

def fetch_intraday(ticker: str) -> dict:
    # 5m intraday do dia
    df = yf.download(
        tickers=ticker,
        period="1d",
        interval="5m",
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )

    if df is None or df.empty:
        return {"ticker": ticker, "error": "Sem dados intraday (ticker pode não existir na fonte)."}
    # yfinance retorna índice datetime
    df = df.dropna()
    if df.empty:
        return {"ticker": ticker, "error": "Sem candles válidos após limpeza."}

    open_day = float(df["Open"].iloc[0])
    last = float(df["Close"].iloc[-1])
    ts_last = df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc).isoformat()

    p = pct_change(open_day, last)
    return {
        "ticker": ticker,
        "open": open_day,
        "last": last,
        "pct": p,
        "last_timestamp_utc": ts_last,
    }

def fetch_news_gdelt(query: str, hours_back: int = 48, max_n: int = 5):
    # GDELT Doc API via gdeltdoc (sem key). Retorna colunas como url/title/seendate/domain... :contentReference[oaicite:5]{index=5}
    gd = GdeltDoc()

    f = Filters(
        keyword=query,
        start_date=(datetime.now(timezone.utc) - pd.Timedelta(hours=hours_back)).strftime("%Y-%m-%d"),
        end_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    try:
        articles = gd.article_search(f)
        if articles is None or len(articles) == 0:
            return []
        # ordena por seendate desc se tiver
        if "seendate" in articles.columns:
            articles = articles.sort_values("seendate", ascending=False)
        rows = []
        for _, r in articles.head(max_n).iterrows():
            rows.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "seendate": str(r.get("seendate", "")),
                "domain": r.get("domain", ""),
            })
        return rows
    except Exception as e:
        return [{"error": f"Falha ao buscar notícias no GDELT: {e}"}]

def main():
    tickers_env = os.getenv("TICKERS", "")
    tickers = [t.strip().upper() for t in tickers_env.split(",") if t.strip()] or DEFAULT_TICKERS

    # portfolio weights (igualitário por padrão)
    weights_env = os.getenv("WEIGHTS", "")
    weights = None
    if weights_env:
        try:
            w = [float(x.strip()) for x in weights_env.split(",")]
            if len(w) == len(tickers) and sum(w) > 0:
                s = sum(w)
                weights = [x / s for x in w]
        except:
            weights = None

    items = []
    for t in tickers:
        items.append(fetch_intraday(t))

    # calcula retorno do portfolio (média simples se não tiver weights)
    valid_pcts = [it["pct"] for it in items if it.get("pct") is not None]
    if weights and len(valid_pcts) == len(tickers):
        port_pct = sum(weights[i] * items[i]["pct"] for i in range(len(tickers)))
    else:
        port_pct = (sum(valid_pcts) / len(valid_pcts)) if valid_pcts else None

    # notícias só quando abs(pct) >= 5
    for it in items:
        p = it.get("pct")
        if p is not None and abs(p) >= 5.0:
            # query simples: ticker (você pode trocar por nome completo depois)
            it["news"] = fetch_news_gdelt(it["ticker"], hours_back=72, max_n=5)
        else:
            it["news"] = []

    out = {
        "generated_at_utc": now_utc_iso(),
        "tickers": tickers,
        "portfolio_pct": port_pct,
        "threshold_pct": 5.0,
        "items": items,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
