"""Top-level utilities and configuration for Binance signal analysis and Pine artifact generation."""

import html
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


FUTURES_BASE_URL = "https://fapi.binance.com"
SPOT_BASE_URL = "https://api.binance.com"
INTERVAL = "4h"
CANDLE_MS = 4 * 60 * 60 * 1000
REQUEST_TIMEOUT = 20
MIN_PREANALYSIS_PROGRESS = 0.20
MIN_PREANALYSIS_SCORE = 62
MIN_FULLANALYSIS_SCORE = 68
MIN_CLOSE_EXTREME_STRENGTH = 0.52
MAX_REASON_LOG_LINES = 120
_print_lock = threading.Lock()
_log_lock = threading.Lock()
LOG_FILE = None
try:
    LOG_FILE = Path.home() / "Desktop/scanner.log"
except Exception:
    LOG_FILE = None

PINE_OUTPUT_DIR = Path(__file__).resolve().parent / "pine-scripts"
PINE_GITHUB_RAW_BASE = os.getenv(
    "SCANNER_PINE_GITHUB_RAW_BASE",
    "https://raw.githubusercontent.com/cryptoscanner-lab/pine-scripts/main",
).rstrip("/")


def _now_it_str() -> str:
    return datetime.now(tz=timezone(timedelta(hours=2))).strftime("[%d/%m/%y %H:%M]")


def _emit(message: str) -> None:
    line = f"{_now_it_str()} {message}"
    with _print_lock:
        print(line, flush=True)
    if LOG_FILE:
        with _log_lock:
            try:
                with LOG_FILE.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except Exception:
                pass


def _build_ssl_context() -> ssl.SSLContext:
    insecure = os.getenv("SCANNER_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    if insecure:
        return ssl._create_unverified_context()
    cafile = os.getenv("SSL_CERT_FILE", "").strip()
    capath = os.getenv("SSL_CERT_DIR", "").strip()
    if cafile or capath:
        return ssl.create_default_context(cafile=cafile or None, capath=capath or None)
    return ssl.create_default_context()


def _json_get(url: str, params: dict = None, retries: int = 3):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    last_exc = None
    ssl_context = _build_ssl_context()
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoScanner2/2.0", "Accept": "application/json"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ssl_context) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.4 * attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Richiesta Binance fallita senza eccezione")


def _base_url_for_market(market: str) -> str:
    return FUTURES_BASE_URL if market == "futures" else SPOT_BASE_URL


def _exchange_info_path_for_market(market: str) -> str:
    return "/fapi/v1/exchangeInfo" if market == "futures" else "/api/v3/exchangeInfo"


def _klines_path_for_market(market: str) -> str:
    return "/fapi/v1/klines" if market == "futures" else "/api/v3/klines"


def _iter_tradable_symbols(market: str) -> list:
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Richiesta Binance fallita senza eccezione")

def _base_url_for_market(market: str) -> str:
    return FUTURES_BASE_URL if market == "futures" else SPOT_BASE_URL

def _exchange_info_path_for_market(market: str) -> str:
    return "/fapi/v1/exchangeInfo" if market == "futures" else "/api/v3/exchangeInfo"

def _klines_path_for_market(market: str) -> str:
    return "/fapi/v1/klines" if market == "futures" else "/api/v3/klines"

def _iter_tradable_symbols(market: str) -> list:
    url = _base_url_for_market(market) + _exchange_info_path_for_market(market)
    payload = _json_get(url)
    symbols = []
    for item in payload.get("symbols", []):
        if market == "futures":
            if item.get("status") != "TRADING":
                continue
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
        else:
            if item.get("status") != "TRADING":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            if not item.get("isSpotTradingAllowed", True):
                continue
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            continue
        if symbol.endswith(("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        symbols.append(symbol)
    symbols.sort()
    return symbols

def _get_klines_for_setup(market: str, symbol: str, first_impulse_at):
    start_dt = first_impulse_at.astimezone(timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = start_ms + (3 * CANDLE_MS) - 1
    url = _base_url_for_market(market) + _klines_path_for_market(market)
    payload = _json_get(
        url,
        {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 3,
        },
    )
    if not isinstance(payload, list):
        raise RuntimeError("Risposta klines non valida")
    return payload

def _to_float(value):
    return float(value)

def _safe_ratio(num, den):
    if abs(den) <= 1e-12:
        return 0.0
    return num / den

def _clip(value, low, high):
    return max(low, min(high, value))

def _analyze_candle(kline):
    open_price = _to_float(kline[1])
    high_price = _to_float(kline[2])
    low_price = _to_float(kline[3])
    close_price = _to_float(kline[4])
    body = abs(close_price - open_price)
    upper_spike = max(0.0, high_price - max(open_price, close_price))
    lower_spike = max(0.0, min(open_price, close_price) - low_price)
    total_spike = upper_spike + lower_spike
    impulse = body + total_spike
    total_range = max(0.0, high_price - low_price)
    direction = 0
    if close_price > open_price:
        direction = 1
    elif close_price < open_price:
        direction = -1
    pct_change = 0.0
    if open_price != 0:
        pct_change = ((close_price - open_price) / open_price) * 100.0
    if direction > 0 and total_range > 0:
        close_extreme_strength = _safe_ratio(close_price - low_price, total_range)
    elif direction < 0 and total_range > 0:
        close_extreme_strength = _safe_ratio(high_price - close_price, total_range)
    else:
        close_extreme_strength = 0.0
    return {
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "body": body,
        "upper_spike": upper_spike,
        "lower_spike": lower_spike,
        "total_spike": total_spike,
        "impulse": impulse,
        "range": total_range,
        "direction": float(direction),
        "pct_change": pct_change,
        "close_extreme_strength": close_extreme_strength,
    }

def _same_direction(c1, c2, c3):
    d1 = int(c1["direction"])
    d2 = int(c2["direction"])
    d3 = int(c3["direction"])
    return d1 != 0 and d1 == d2 == d3

def _third_candle_progress(first_impulse_at, phase):
    third_open = first_impulse_at.astimezone(timezone(timedelta(hours=-4))) + timedelta(hours=8)
    third_close = third_open + timedelta(hours=4)
    now = datetime.now(tz=timezone(timedelta(hours=-4)))
    if phase == "fullanalysis" or now >= third_close:
        return 1.0
    if now <= third_open:
        return 0.0
    return _clip((now - third_open).total_seconds() / (4 * 60 * 60), 0.0, 1.0)

def _validate_triplet_rules(c1, c2, c3, phase, progress):
    eps = 1e-12
    if not _same_direction(c1, c2, c3):
        return "direzione incoerente"
    if c1["body"] <= eps or c2["body"] <= eps or c3["body"] <= eps:
        return "body nullo"
    if c1["total_spike"] >= c1["body"]:
        return "spike totale C1 >= body C1"
    if c2["total_spike"] >= c2["body"]:
        return "spike totale C2 >= body C2"
    if c3["total_spike"] >= c3["body"]:
        return "spike totale C3 >= body C3"
    if not (c1["body"] < c2["body"]):
        return "body C1 non minore di body C2"
    if not (c1["impulse"] < c2["impulse"]):
        return "impulso C1 non minore di impulso C2"
    if c2["upper_spike"] > c1["body"] or c2["lower_spike"] > c1["body"]:
        return "spike C2 oltre body C1"
    if c1["upper_spike"] > c2["body"] or c1["lower_spike"] > c2["body"]:
        return "spike C1 oltre body C2"
    if c3["upper_spike"] > c2["body"] or c3["lower_spike"] > c2["body"]:
        return "spike C3 oltre body C2"
    if phase == "preanalysis" and progress < MIN_PREANALYSIS_PROGRESS:
        return "terza candela troppo acerba"
    if not (c3["body"] > (c1["body"] + c2["body"])):
        return "body C3 non supera somma C1+C2"
    if not (c3["impulse"] > (c1["impulse"] + c2["impulse"])):
        return "impulso C3 non supera somma C1+C2"
    if c3["close_extreme_strength"] < MIN_CLOSE_EXTREME_STRENGTH:
        return "chiusura C3 poco vicina all'estremo"
    return None

def _compute_signal_metrics(c1, c2, c3, phase, progress):
    body_growth_12 = _safe_ratio(c2["body"], c1["body"])
    body_breakout = _safe_ratio(c3["body"], (c1["body"] + c2["body"]))
    impulse_growth_12 = _safe_ratio(c2["impulse"], c1["impulse"])
    impulse_breakout = _safe_ratio(c3["impulse"], (c1["impulse"] + c2["impulse"]))
    spike_cleanliness = 1.0 - _clip(_safe_ratio(c3["total_spike"], c3["body"]), 0.0, 1.0)
    close_strength = _clip(c3["close_extreme_strength"], 0.0, 1.0)
    phase_bonus = progress if phase == "preanalysis" else 1.0
    score = (
        _clip(body_growth_12 / 1.4, 0.0, 1.0) * 14.0
        + _clip(body_breakout / 1.1, 0.0, 1.0) * 24.0
        + _clip(impulse_growth_12 / 1.35, 0.0, 1.0) * 12.0
        + _clip(impulse_breakout / 1.12, 0.0, 1.0) * 26.0
        + spike_cleanliness * 12.0
        + close_strength * 8.0
        + _clip(phase_bonus, 0.0, 1.0) * 4.0
    )
    score = round(_clip(score, 0.0, 100.0), 2)
    direction_text = "LONG" if int(c3["direction"]) > 0 else "SHORT"
    return {
        "score": score,
        "direction_text": direction_text,
        "body_growth_12": body_growth_12,
        "body_breakout": body_breakout,
        "impulse_growth_12": impulse_growth_12,
        "impulse_breakout": impulse_breakout,
        "spike_cleanliness": spike_cleanliness,
        "close_strength": close_strength,
        "progress": progress,
    }

def _min_score_for_phase(phase):
    return MIN_FULLANALYSIS_SCORE if phase == "fullanalysis" else MIN_PREANALYSIS_SCORE

def _reason_for_missing_klines(klines, first_impulse_at):
    if not klines:
        return "klines non disponibili"
    if len(klines) < 3:
        return "tripletta non disponibile"
    expected_start_ms = int(first_impulse_at.astimezone(timezone.utc).timestamp() * 1000)
    for idx, kline in enumerate(klines[:3]):
        expected = expected_start_ms + idx * CANDLE_MS
        try:
            actual = int(kline[0])
        except Exception:
            return "pattern non calcolabile"
        if actual != expected:
            return "tripletta non disponibile"
    return "pattern non calcolabile"

def _format_good_symbol(symbol, c3, metrics):
    sign = "+" if c3["pct_change"] >= 0 else ""
    return (
        f"{symbol} | {metrics['direction_text']} | score {float(metrics['score']):.2f} | "
        f"var C3 {sign}{c3['pct_change']:.2f}% | "
        f"body3/somma12 {float(metrics['body_breakout']):.2f} | "
        f"imp3/somma12 {float(metrics['impulse_breakout']):.2f}"
    )


def _symbol_exchange_for_market(market):
        return "BINANCE" if market in {"spot", "futures"} else market.upper()


def _slugify_symbol(symbol):
        parts = []
        for char in str(symbol).strip().upper():
                if char.isalnum() or char in {"_", "-"}:
                        parts.append(char)
                else:
                        parts.append("_")
        slug = "".join(parts).strip("_")
        return slug or "SIGNAL"


def _tradingview_url(exchange, symbol):
    full_symbol = f"{exchange}:{symbol}"
    return f"https://www.tradingview.com/chart/?symbol={urllib.parse.quote(full_symbol, safe=':')}"


def _public_pine_url(pine_path):
    return f"{PINE_GITHUB_RAW_BASE}/{pine_path.name}"


def _setup_number_from_impulse(first_impulse_at):
    local_dt = first_impulse_at.astimezone(timezone(timedelta(hours=-4)))
    digits = f"{local_dt.day:02d}{local_dt.month:02d}{local_dt.year % 100:02d}"
    total = sum(int(ch) for ch in digits)
    while total > 9:
        total = sum(int(ch) for ch in str(total))
    return 9 if total == 0 else total


def _build_pine_artifact(market, phase, symbol, first_impulse_at, c1, c2, c3, metrics):
    exchange = _symbol_exchange_for_market(market)
    start_time = first_impulse_at.astimezone(timezone(timedelta(hours=-4))).isoformat()
    setup_number = _setup_number_from_impulse(first_impulse_at)
    be = c1["open"]
    impulso1 = c1["body"]
    impulso2 = c2["body"]
    impulso3 = c3["body"]
    spike1_up = c1["upper_spike"]
    spike2_low = c2["lower_spike"]
    spike2_up = c2["upper_spike"]
    spike3_low = c3["lower_spike"]

    pine_code = generate_pine_script(
        be,
        impulso1,
        impulso2,
        impulso3,
        spike1_up,
        spike2_low,
        spike2_up,
        spike3_low,
        start_time,
    )

    PINE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = first_impulse_at.astimezone(timezone(timedelta(hours=-4))).strftime("%Y%m%dT%H%M%S")
    base_name = f"{_slugify_symbol(symbol)}_{phase}_{stamp}"
    pine_path = PINE_OUTPUT_DIR / f"{base_name}.pine"
    html_path = PINE_OUTPUT_DIR / f"{base_name}.html"
    pine_path.write_text(pine_code, encoding="utf-8")

    tradingview_link = _tradingview_url(exchange, symbol)
    public_pine_link = _public_pine_url(pine_path)
    escaped_pine = html.escape(pine_code)
    html_text = f"""<!doctype html>
<html lang=\"it\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>{html.escape(symbol)} Pine</title>
    <style>
        :root {{
            color-scheme: dark;
            --bg0: #070b12;
            --bg1: #0d1220;
            --panel: rgba(17, 23, 35, 0.96);
            --panel-strong: #151d2c;
            --border: #26324a;
            --text: #f3f7ff;
            --muted: #9aa8bd;
            --accent: #57e08a;
            --accent-2: #8bd4ff;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            min-height: 100vh;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background:
                radial-gradient(circle at top, rgba(87, 224, 138, 0.16), transparent 24%),
                radial-gradient(circle at 85% 0%, rgba(139, 212, 255, 0.16), transparent 22%),
                linear-gradient(180deg, var(--bg1), var(--bg0));
            color: var(--text);
        }}
        .wrap {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
        .hero {{
            margin-bottom: 14px;
            padding: 14px 16px;
            border-radius: 16px;
            background: linear-gradient(135deg, rgba(87, 224, 138, .14), rgba(139, 212, 255, .12));
            border: 1px solid rgba(139, 212, 255, .18);
            color: #dfeaff;
            line-height: 1.45;
        }}
        .card {{
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 22px;
            padding: 20px;
            box-shadow: 0 16px 50px rgba(0, 0, 0, .38);
            backdrop-filter: blur(6px);
        }}
        h1 {{ margin: 0 0 10px; font-size: clamp(24px, 3vw, 34px); line-height: 1.1; }}
        .meta {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 10px 0 18px; color: var(--muted); font-size: 14px; }}
        .pill {{ padding: 6px 10px; border-radius: 999px; background: #20283a; border: 1px solid #2f3b52; }}
        .details {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; margin-bottom: 16px; }}
        .field {{
            background: var(--panel-strong);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 12px 14px;
            min-height: 72px;
        }}
        .field .label {{ color: var(--muted); font-size: 12px; display: block; margin-bottom: 6px; text-transform: uppercase; letter-spacing: .04em; }}
        .field .value {{ color: var(--text); font-weight: 700; word-break: break-word; line-height: 1.35; }}
        .field .value a {{ color: var(--accent-2); text-decoration: none; }}
        .field .value a:hover {{ text-decoration: underline; }}
        .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 6px 0 16px; align-items: center; }}
        a.button, button {{
            appearance: none;
            border: 0;
            border-radius: 12px;
            padding: 11px 16px;
            font-weight: 700;
            cursor: pointer;
            text-decoration: none;
        }}
        a.button {{ background: var(--accent); color: #07130b; }}
        button {{ background: #2b3448; color: var(--text); }}
        textarea {{
            width: 100%;
            min-height: 64vh;
            resize: vertical;
            border-radius: 16px;
            border: 1px solid #2b3448;
            background: #0b0f17;
            color: #e9eef7;
            padding: 16px;
            line-height: 1.5;
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            font-size: 13px;
            box-sizing: border-box;
        }}
        .small {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: space-between; margin-top: 10px; color: var(--muted); font-size: 12px; }}
        .note {{ margin-top: 12px; color: #c8d2e4; font-size: 12px; line-height: 1.45; }}
        .kbd {{
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            background: #20283a;
            border: 1px solid #31405a;
            border-bottom-width: 2px;
            padding: 1px 6px;
            border-radius: 6px;
            color: #eff4ff;
        }}
        @media (max-width: 640px) {{
            .wrap {{ padding: 14px; }}
            .card {{ padding: 16px; border-radius: 18px; }}
            textarea {{ min-height: 56vh; }}
        }}
    </style>
</head>
<body>
    <div class=\"wrap\">
        <div class=\"hero\">
            Pine e pagina HTML sono generati direttamente dal segnale. Se il file è pubblicato nel repository GitHub pubblico, il link in pagina è condivisibile.
        </div>
        <div class=\"card\">
            <h1>{html.escape(symbol)} - Pine pronto</h1>
            <div class=\"meta\">
                <span class=\"pill\">{html.escape(exchange)}</span>
                <span class=\"pill\">{html.escape(phase.upper())}</span>
                <span class=\"pill\">score {float(metrics['score']):.2f}</span>
                <span class=\"pill\">{html.escape(str(metrics['direction_text']))}</span>
            </div>
            <div class=\"details\">
                <div class=\"field\"><span class=\"label\">Coppia</span><span class=\"value\">{html.escape(symbol)}</span></div>
                <div class=\"field\"><span class=\"label\">Orario setup</span><span class=\"value\">{html.escape(start_time)}</span></div>
                <div class=\"field\"><span class=\"label\">Numero setup</span><span class=\"value\">{setup_number}</span></div>
                <div class=\"field\"><span class=\"label\">Direzione</span><span class=\"value\">{html.escape(str(metrics['direction_text']))}</span></div>
                <div class=\"field\"><span class=\"label\">TradingView</span><span class=\"value\"><a href=\"{html.escape(tradingview_link)}\" target=\"_blank\" rel=\"noopener\">Apri chart</a></span></div>
                <div class=\"field\"><span class=\"label\">Link pubblico Pine</span><span class=\"value\"><a href=\"{html.escape(public_pine_link)}\" target=\"_blank\" rel=\"noopener\">Apri file pubblico</a></span></div>
                <div class=\"field\"><span class=\"label\">File Pine locale</span><span class=\"value\">{html.escape(pine_path.name)}</span></div>
            </div>
            <div class=\"actions\">
                <a class=\"button\" href=\"{html.escape(tradingview_link)}\" target=\"_blank\" rel=\"noopener\">Apri TradingView</a>
                <button id=\"copyBtn\" type=\"button\" accesskey=\"c\" autofocus>Copia Pine</button>
            </div>
            <textarea id=\"pine\" readonly>{escaped_pine}</textarea>
            <div class=\"small\">
                <span>Scorciatoia: <span class=\"kbd\">Alt</span> + <span class=\"kbd\">Shift</span> + <span class=\"kbd\">C</span></span>
                <span>HTML: {html.escape(html_path.name)}</span>
            </div>
            <div class=\"note\">Nota: il link pubblico funziona solo se questi file sono davvero esposti in un repository GitHub pubblico. In locale resta pronto, ma non è accessibile dall'esterno.</div>
        </div>
    </div>
    <script>
        const pineField = document.getElementById('pine');
        const copyButton = document.getElementById('copyBtn');
        const copyPine = async () => {{
            pineField.focus();
            pineField.select();
            pineField.setSelectionRange(0, pineField.value.length);
            try {{
                await navigator.clipboard.writeText(pineField.value);
            }} catch (error) {{
                document.execCommand('copy');
            }}
        }};
        copyButton.addEventListener('click', copyPine);
        document.addEventListener('keydown', (event) => {{
            if ((event.altKey || event.metaKey || event.ctrlKey) && event.shiftKey && event.key.toLowerCase() === 'c') {{
                event.preventDefault();
                copyPine();
            }}
        }});
    </script>
</body>
</html>
"""
    html_path.write_text(html_text, encoding="utf-8")

    return {
        "exchange": exchange,
        "tradingview_url": tradingview_link,
        "pine_file": str(pine_path),
        "html_file": str(html_path),
        "pine_be": be,
        "pine_impulso1": impulso1,
        "pine_impulso2": impulso2,
        "pine_impulso3": impulso3,
        "pine_spike1_up": spike1_up,
        "pine_spike2_low": spike2_low,
        "pine_spike2_up": spike2_up,
        "pine_spike3_low": spike3_low,
        "pine_start_time": start_time,
        "pine_direction": str(metrics["direction_text"]),
    }

def _log_reason_summary(market, rejected_reasons, skipped_reasons):
    if LOG_FILE:
        with _log_lock:
            try:
                with LOG_FILE.open("a", encoding="utf-8") as fh:
                    if rejected_reasons:
                        fh.write(f"{_now_it_str()} Motivi scarto {market.upper()}:\n")
                        for reason, count in rejected_reasons.most_common():
                            fh.write(f" - {reason}: {count}\n")
                    if skipped_reasons:
                        fh.write(f"{_now_it_str()} Non analizzati {market.upper()}:\n")
                        for item in skipped_reasons[:MAX_REASON_LOG_LINES]:
                            fh.write(f" - {item}\n")
                        extra = max(0, len(skipped_reasons) - MAX_REASON_LOG_LINES)
                        if extra:
                            fh.write(f" - altri {extra} elementi non mostrati\n")
            except Exception:
                pass

def _analyze_symbol(market, phase, first_impulse_at, symbol):
    try:
        klines = _get_klines_for_setup(market, symbol, first_impulse_at)
    except urllib.error.HTTPError as exc:
        return ("skipped", symbol, f"errore Binance HTTP {exc.code}")
    except urllib.error.URLError as exc:
        return ("skipped", symbol, f"errore Binance rete: {exc.reason}")
    except Exception as exc:
        return ("skipped", symbol, f"errore Binance: {exc}")
    if len(klines) < 3:
        return ("skipped", symbol, _reason_for_missing_klines(klines, first_impulse_at))
    try:
        c1 = _analyze_candle(klines[0])
        c2 = _analyze_candle(klines[1])
        c3 = _analyze_candle(klines[2])
    except Exception:
        return ("skipped", symbol, "pattern non calcolabile")
    progress = _third_candle_progress(first_impulse_at, phase)
    failed_reason = _validate_triplet_rules(c1, c2, c3, phase, progress)
    if failed_reason:
        return ("rejected", symbol, failed_reason)
    metrics = _compute_signal_metrics(c1, c2, c3, phase, progress)
    if float(metrics["score"]) < _min_score_for_phase(phase):
        return ("rejected", symbol, f"score insufficiente ({float(metrics['score']):.2f})")
    artifact = _build_pine_artifact(market, phase, symbol, first_impulse_at, c1, c2, c3, metrics)
    return (
        "good",
        {
            "summary": _format_good_symbol(symbol, c3, metrics),
            "artifact": artifact,
        },
        None,
    )
def generate_pine_script(be, impulso1, impulso2, impulso3, spike1_up, spike2_low, spike2_up, spike3_low, start_time):
    entry_c1 = be - impulso1
    entry_c2 = be - (impulso1 + impulso2 - spike1_up - spike2_low)
    entry_c3 = be - (impulso1 + impulso2 + impulso3 - spike1_up - spike2_low - spike2_up - spike3_low)
    return f"""
//@version=5
indicator(\"Gann Signal\", overlay=true)

be = {be}
impulso1 = {impulso1}
impulso2 = {impulso2}
impulso3 = {impulso3}
spike1_up = {spike1_up}
spike2_low = {spike2_low}
spike2_up = {spike2_up}
spike3_low = {spike3_low}
start_time_str = \"{start_time}\"

entry_c1 = {entry_c1}
entry_c2 = {entry_c2}
entry_c3 = {entry_c3}

// --- CALCOLO BARRA DI INIZIO ---
start_time_ts = timestamp(start_time_str)
var int start_bar = na
if time >= start_time_ts and na(start_bar)
    start_bar := bar_index

// --- DISEGNA LE LINEE ---
if not na(start_bar) and bar_index >= start_bar
    line.new(start_bar, be, bar_index, be, color=color.gray, width=1, extend=extend.right)
    line.new(start_bar, entry_c1, bar_index, entry_c1, color=color.blue, width=2, extend=extend.right)
    line.new(start_bar, entry_c2, bar_index, entry_c2, color=color.orange, width=2, extend=extend.right)
    line.new(start_bar, entry_c3, bar_index, entry_c3, color=color.red, width=2, extend=extend.right)
"""


# --- Funzione reale per la scansione di tutte le coppie Binance ---
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import urllib.error

NY_TZ = __import__('zoneinfo').ZoneInfo("America/New_York")
UTC = __import__('datetime').timezone.utc
MAX_WORKERS = 8
PROGRESS_EVERY = 90

def scan_market_symbols(market, phase, setup):
    first_impulse_at = setup.first_impulse_at
    if first_impulse_at.tzinfo is None:
        first_impulse_at = first_impulse_at.replace(tzinfo=NY_TZ)

    third_open = first_impulse_at + timedelta(hours=8)
    third_close = third_open + timedelta(hours=4)
    if phase == "fullanalysis" and __import__('datetime').datetime.now(tz=NY_TZ) < third_close.astimezone(NY_TZ):
        raise RuntimeError("Analisi completa richiesta ma terza candela non ancora chiusa")

    symbols = _iter_tradable_symbols(market)
    total = len(symbols)
    good_symbols = []
    signal_payloads = []
    rejected = 0
    skipped = 0
    rejected_reasons = Counter()
    non_analyzed_reasons = []

    _emit(
        f"Scan {market.upper()}: simboli rilevati {total} | "
        f"fase {phase.upper()} | primo impulso {first_impulse_at.astimezone(NY_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )

    processed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_analyze_symbol, market, phase, first_impulse_at, symbol): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            processed += 1
            status, payload, reason = future.result()
            if status == "good":
                good_symbols.append(payload["summary"])
                signal_payloads.append(payload["artifact"])
            elif status == "rejected":
                rejected += 1
                if reason:
                    rejected_reasons[reason] += 1
            else:
                skipped += 1
                non_analyzed_reasons.append(f"{payload}: {reason}")

            if processed % PROGRESS_EVERY == 0 or processed == total:
                _emit(
                    f"Avanzamento {market.upper()}: {processed}/{total} | "
                    f"buoni {len(good_symbols)} | scartati {rejected} | non analizzati {skipped}"
                )

    good_symbols.sort()
    _log_reason_summary(market, rejected_reasons, non_analyzed_reasons)

    if rejected_reasons:
        top_reason, top_count = rejected_reasons.most_common(1)[0]
        _emit(f"Motivo scarto prevalente {market.upper()}: {top_reason} ({top_count})")

    return {
        "processed_now": total,
        "good_now": len(good_symbols),
        "rejected_now": rejected,
        "skipped_now": skipped,
        "residual_to_complete": skipped,
        "good_symbols_now": good_symbols,
        "signal_payloads_now": signal_payloads,
        "totals_good": len(good_symbols),
        "totals_rejected": rejected,
        "totals_skipped": skipped,
    }
