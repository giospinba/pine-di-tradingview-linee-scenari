#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import ssl
import threading
import time
import requests
import urllib3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BASE_DIR = Path.home() / "Desktop"
LOG_FILE = BASE_DIR / "scanner_upbit.log"

NY_TZ = ZoneInfo("America/New_York")
IT_TZ = ZoneInfo("Europe/Rome")
UTC = timezone.utc

UPBIT_REGIONS = [r.strip().lower() for r in (os.getenv("UPBIT_REGIONS", "sg,id,th") or "sg,id,th").split(",") if r.strip()]
if not UPBIT_REGIONS:
    UPBIT_REGIONS = ["sg", "id", "th"]
UPBIT_MARKETS_PATH = "/v1/market/all"
UPBIT_240M_CANDLES_PATH = "/v1/candles/minutes/240"
REQUEST_TIMEOUT = 20
MAX_WORKERS = int(os.getenv("SCANNER_MAX_WORKERS", "8") or "8")
PROGRESS_EVERY = int(os.getenv("SCANNER_PROGRESS_EVERY", "90") or "90")
MIN_PREANALYSIS_PROGRESS = float(os.getenv("SCANNER_MIN_PREANALYSIS_PROGRESS", "0.20") or "0.20")
MIN_PREANALYSIS_SCORE = float(os.getenv("SCANNER_MIN_PREANALYSIS_SCORE", "62") or "62")
MIN_FULLANALYSIS_SCORE = float(os.getenv("SCANNER_MIN_FULLANALYSIS_SCORE", "68") or "68")
MIN_CLOSE_EXTREME_STRENGTH = float(os.getenv("SCANNER_MIN_CLOSE_EXTREME_STRENGTH", "0.52") or "0.52")
MAX_REASON_LOG_LINES = int(os.getenv("SCANNER_MAX_REASON_LOG_LINES", "120") or "120")

_print_lock = threading.Lock()
_log_lock = threading.Lock()


def _now_it_str() -> str:
    return datetime.now(tz=IT_TZ).strftime("[%d/%m/%y %H:%M]")


def _emit(message: str) -> None:
    line = f"{_now_it_str()} {message}"
    with _print_lock:
        print(line, flush=True)
    with _log_lock:
        try:
            with LOG_FILE.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except Exception:
            pass


def _disable_insecure_request_warnings() -> None:
    insecure = os.getenv("SCANNER_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    if insecure:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _build_ssl_context() -> ssl.SSLContext:
    insecure = os.getenv("SCANNER_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    if insecure:
        return ssl._create_unverified_context()
    cafile = os.getenv("SSL_CERT_FILE", "").strip()
    capath = os.getenv("SSL_CERT_DIR", "").strip()
    if cafile or capath:
        return ssl.create_default_context(cafile=cafile or None, capath=capath or None)
    return ssl.create_default_context()


def _base_url_for_region(region: str) -> str:
    return f"https://{region}-api.upbit.com"


def _json_get(region: str, path: str, params: dict[str, Any] | None = None, retries: int = 3) -> Any:
    url = _base_url_for_region(region) + path

    verify: bool | str = True
    insecure = os.getenv("SCANNER_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    if insecure:
        verify = False
    else:
        cafile = os.getenv("SSL_CERT_FILE", "").strip()
        if cafile:
            verify = cafile

    last_exc: Exception | None = None
    headers = {
        "User-Agent": "CryptoScanner2-Upbit/1.0",
        "Accept": "application/json",
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT, verify=verify)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.4 * attempt)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Richiesta Upbit fallita senza eccezione")

def _iter_tradable_symbols() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for region in UPBIT_REGIONS:
        payload = _json_get(region, UPBIT_MARKETS_PATH, {"isDetails": "false"})
        for item in payload:
            market = str(item.get("market", "")).strip()
            if not market:
                continue
            key = (region, market)
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "region": region,
                    "market": market,
                    "display": f"{region.upper()}:{market}",
                }
            )
    items.sort(key=lambda x: (x["region"], x["market"]))
    return items


def _to_upbit_to_value(first_impulse_at: datetime) -> str:
    third_close_utc = first_impulse_at.astimezone(UTC) + timedelta(hours=12)
    return third_close_utc.strftime("%Y-%m-%dT%H:%M:%S")


def _get_klines_for_setup(region: str, symbol: str, first_impulse_at: datetime) -> list[dict[str, Any]]:
    payload = _json_get(
        region,
        UPBIT_240M_CANDLES_PATH,
        {
            "market": symbol,
            "to": _to_upbit_to_value(first_impulse_at),
            "count": 3,
        },
    )
    if not isinstance(payload, list):
        raise RuntimeError("Risposta candles Upbit non valida")
    candles = list(reversed(payload))
    return candles


def _to_float(value: Any) -> float:
    return float(value)


def _safe_ratio(num: float, den: float) -> float:
    if abs(den) <= 1e-12:
        return 0.0
    return num / den


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _analyze_candle(candle: dict[str, Any]) -> dict[str, float]:
    open_price = _to_float(candle["opening_price"])
    high_price = _to_float(candle["high_price"])
    low_price = _to_float(candle["low_price"])
    close_price = _to_float(candle["trade_price"])

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


def _invalid_impulse_reason(candle: dict[str, float]) -> str | None:
    eps = 1e-12
    if int(candle["direction"]) == 0 or candle["body"] <= eps:
        return "body nullo"
    if candle["upper_spike"] <= eps and candle["lower_spike"] <= eps:
        return "impulso senza spike"
    if candle["total_spike"] >= candle["body"]:
        return "spike totale non minore del body"
    return None


def _same_direction(c1: dict[str, float], c2: dict[str, float], c3: dict[str, float]) -> bool:
    d1 = int(c1["direction"])
    d2 = int(c2["direction"])
    d3 = int(c3["direction"])
    return d1 != 0 and d1 == d2 == d3


def _third_candle_progress(first_impulse_at: datetime, phase: str) -> float:
    third_open = first_impulse_at.astimezone(NY_TZ) + timedelta(hours=8)
    third_close = third_open + timedelta(hours=4)
    now = datetime.now(tz=NY_TZ)
    if phase == "fullanalysis" or now >= third_close:
        return 1.0
    if now <= third_open:
        return 0.0
    return _clip((now - third_open).total_seconds() / (4 * 60 * 60), 0.0, 1.0)


def _validate_triplet_rules(c1: dict[str, float], c2: dict[str, float], c3: dict[str, float], phase: str, progress: float) -> str | None:
    if not _same_direction(c1, c2, c3):
        return "direzione incoerente"

    invalid1 = _invalid_impulse_reason(c1)
    if invalid1:
        return f"impulso 1 non valido: {invalid1}"
    invalid2 = _invalid_impulse_reason(c2)
    if invalid2:
        return f"impulso 2 non valido: {invalid2}"
    invalid3 = _invalid_impulse_reason(c3)
    if invalid3:
        return f"impulso 3 non valido: {invalid3}"

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


def _compute_signal_metrics(c1: dict[str, float], c2: dict[str, float], c3: dict[str, float], phase: str, progress: float) -> dict[str, float | str]:
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
        "body_breakout": body_breakout,
        "impulse_breakout": impulse_breakout,
    }


def _min_score_for_phase(phase: str) -> float:
    return MIN_FULLANALYSIS_SCORE if phase == "fullanalysis" else MIN_PREANALYSIS_SCORE


def _fmt5(value: float) -> str:
    return f"{value:.5f}"


def _format_good_symbol_group(market: str, items: list[dict[str, Any]]) -> str:
    regions = sorted({str(item["region"]).upper() for item in items})
    first = items[0]
    c1 = first["c1"]
    c2 = first["c2"]
    c3 = first["c3"]
    metrics = first["metrics"]
    total_conformation = c1["impulse"] + c2["impulse"] + c3["impulse"]

    lines = [
        f"📍 {market} | regioni: {', '.join(regions)}",
        f"Direzione: {metrics['direction_text']}",
        f"Variazione 3° impulso: {c3['pct_change']:.5f}%",
        "",
        "Impulso 1",
        f"- Body: {_fmt5(c1['body'])}",
        f"- Spike alta: {_fmt5(c1['upper_spike'])}",
        f"- Spike bassa: {_fmt5(c1['lower_spike'])}",
        f"- Impulso totale: {_fmt5(c1['impulse'])}",
        "",
        "Impulso 2",
        f"- Body: {_fmt5(c2['body'])}",
        f"- Spike alta: {_fmt5(c2['upper_spike'])}",
        f"- Spike bassa: {_fmt5(c2['lower_spike'])}",
        f"- Impulso totale: {_fmt5(c2['impulse'])}",
        "",
        "Impulso 3",
        f"- Body: {_fmt5(c3['body'])}",
        f"- Spike alta: {_fmt5(c3['upper_spike'])}",
        f"- Spike bassa: {_fmt5(c3['lower_spike'])}",
        f"- Impulso totale: {_fmt5(c3['impulse'])}",
        "",
        f"Conformazione totale: {_fmt5(total_conformation)}",
        f"Conformazione body: {float(metrics['body_breakout']):.5f}",
        f"Conformazione impulso: {float(metrics['impulse_breakout']):.5f}",
    ]
    return "\n".join(lines)


def _log_reason_summary(rejected_reasons: Counter[str], skipped_reasons: list[str]) -> None:
    with _log_lock:
        try:
            with LOG_FILE.open("a", encoding="utf-8") as fh:
                if rejected_reasons:
                    fh.write(f"{_now_it_str()} Motivi scarto UPBIT:\n")
                    for reason, count in rejected_reasons.most_common():
                        fh.write(f" - {reason}: {count}\n")
                if skipped_reasons:
                    fh.write(f"{_now_it_str()} Non analizzati UPBIT:\n")
                    for item in skipped_reasons[:MAX_REASON_LOG_LINES]:
                        fh.write(f" - {item}\n")
                    extra = max(0, len(skipped_reasons) - MAX_REASON_LOG_LINES)
                    if extra:
                        fh.write(f" - altri {extra} elementi non mostrati\n")
        except Exception:
            pass


def _analyze_symbol(phase: str, first_impulse_at: datetime, item: dict[str, str]) -> tuple[str, Any, str | None]:
    region = item["region"]
    symbol = item["market"]
    display_symbol = item["display"]
    try:
        candles = _get_klines_for_setup(region, symbol, first_impulse_at)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "?"
        return ("skipped", display_symbol, f"errore Upbit HTTP {status_code}")
    except requests.RequestException as exc:
        return ("skipped", display_symbol, f"errore Upbit rete: {exc}")
    except Exception as exc:
        return ("skipped", display_symbol, f"errore Upbit: {exc}")

    if len(candles) < 3:
        return ("skipped", symbol, "tripletta non disponibile")

    try:
        c1 = _analyze_candle(candles[0])
        c2 = _analyze_candle(candles[1])
        c3 = _analyze_candle(candles[2])
    except Exception:
        return ("skipped", symbol, "pattern non calcolabile")

    progress = _third_candle_progress(first_impulse_at, phase)
    failed_reason = _validate_triplet_rules(c1, c2, c3, phase, progress)
    if failed_reason:
        return ("rejected", symbol, failed_reason)

    metrics = _compute_signal_metrics(c1, c2, c3, phase, progress)
    if float(metrics["score"]) < _min_score_for_phase(phase):
        return ("rejected", symbol, f"score insufficiente ({float(metrics['score']):.2f})")

    return (
        "good",
        {
            "region": region,
            "market": symbol,
            "display_symbol": display_symbol,
            "c1": c1,
            "c2": c2,
            "c3": c3,
            "metrics": metrics,
        },
        None,
    )


def scan_market_symbols(_market, phase, setup) -> dict[str, Any]:
    first_impulse_at = setup.first_impulse_at
    if first_impulse_at.tzinfo is None:
        first_impulse_at = first_impulse_at.replace(tzinfo=NY_TZ)

    symbols = _iter_tradable_symbols()
    total = len(symbols)
    good_payloads: list[dict[str, Any]] = []
    rejected = 0
    skipped = 0
    rejected_reasons: Counter[str] = Counter()
    non_analyzed_reasons: list[str] = []

    _emit(
        f"Scan UPBIT: pairs rilevate {total} | fase {phase.upper()} | "
        f"1 impulso {first_impulse_at.astimezone(NY_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')} | regions {','.join(r.upper() for r in UPBIT_REGIONS)}"
    )

    processed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_analyze_symbol, phase, first_impulse_at, item): item["display"]
            for item in symbols
        }
        for future in as_completed(futures):
            processed += 1
            status, payload, reason = future.result()
            if status == "good":
                good_payloads.append(payload)
            elif status == "rejected":
                rejected += 1
                if reason:
                    rejected_reasons[reason] += 1
            else:
                skipped += 1
                non_analyzed_reasons.append(f"{payload}: {reason}")
            if processed % PROGRESS_EVERY == 0 or processed == total:
                _emit(
                    f"Avanzamento UPBIT: {processed}/{total} | buoni {len(good_payloads)} | scartati {rejected} | non analizzati {skipped}"
                )

    grouped_goods: dict[str, list[dict[str, Any]]] = {}
    for payload in good_payloads:
        grouped_goods.setdefault(payload["market"], []).append(payload)

    good_symbols = [_format_good_symbol_group(market, grouped_goods[market]) for market in sorted(grouped_goods)]
    _log_reason_summary(rejected_reasons, non_analyzed_reasons)

    if rejected_reasons:
        top_reason, top_count = rejected_reasons.most_common(1)[0]
        _emit(f"Motivo scarto prevalente UPBIT: {top_reason} ({top_count})")

    return {
        "processed_now": total,
        "good_now": len(good_symbols),
        "rejected_now": rejected,
        "skipped_now": skipped,
        "residual_to_complete": skipped,
        "good_symbols_now": good_symbols,
        "totals_good": len(good_symbols),
        "totals_rejected": rejected,
        "totals_skipped": skipped,
    }
