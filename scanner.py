#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import shutil
import signal
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import ssl
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo


BASE_DIR = Path.home() / "Desktop"
SCRIPT_DIR = Path(__file__).resolve().parent
SYSTEM_DIR = BASE_DIR / "File di sistema Scanner Gann"
BACKUP_DIR = BASE_DIR / "scanner_backup_state"
NY_TZ = ZoneInfo("America/New_York")
IT_TZ = ZoneInfo("Europe/Rome")
UTC = timezone.utc
TELEGRAM_BOT_TOKEN_FALLBACK = "8601689604:AAH-J-rcxRDr4jK73tw145sHywH_F2fXa5k"
TELEGRAM_CHAT_ID_FALLBACK = "-1003723118597"
STATE_SCHEMA_VERSION = 4
LOCK_STALE_SECONDS = 4 * 60 * 60
MAX_COMPLETED_RUNS_PER_MARKET = 120
ENGINE_MODULE_ENV = "CRYPTO_SCANNER_ENGINE_MODULE"
ENGINE_FILE_ENV = "CRYPTO_SCANNER_ENGINE_FILE"
ENGINE_FILE_CANDIDATES = (
    SCRIPT_DIR / "scanner_engine.py",
    SYSTEM_DIR / "scanner_engine.py",
    BASE_DIR / "scanner_engine.py",
)
AUTO_FULLANALYSIS_WINDOW_MINUTES = int(os.getenv("SCANNER_AUTO_FULL_WINDOW_MINUTES", "10") or "10")
AUTO_CANDLE_HOURS = (0, 4, 8, 12, 16, 20)
AUTO_PREANALYSIS_WINDOW_MINUTES = int(os.getenv("SCANNER_AUTO_PRE_WINDOW_MINUTES", "15") or "15")

MARKET_CONFIG = {
    "futures": {
        "state_file": BASE_DIR / "scanner_state_futures.json",
        "lock_file": BASE_DIR / "scanner_futures.lock",
        "label": "FUTURES",
    },
    "spot": {
        "state_file": BASE_DIR / "scanner_state_spot.json",
        "lock_file": BASE_DIR / "scanner_spot.lock",
        "label": "SPOT",
    },
}

PHASE_LABELS = {
    "preanalysis": "PREANALISI = terza candela non ancora chiusa",
    "prescan": "PRESCAN = scansione manuale forzata disponibile in qualsiasi momento",
    "fullanalysis": "ANALISI COMPLETA = terza candela chiusa",
}

PHASE_TITLES = {
    "preanalysis": "PREANALISI",
    "prescan": "PRESCAN",
    "fullanalysis": "ANALISI COMPLETA",
}


@dataclass
class SetupContext:
    tripletta: str
    first_impulse_at: datetime
    setup_number: int

    def normalized(self) -> "SetupContext":
        dt = self.first_impulse_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return SetupContext(
            tripletta=str(self.tripletta).strip(),
            first_impulse_at=dt,
            setup_number=int(self.setup_number),
        )

    @property
    def setup_family(self) -> str:
        return setup_family_for_number(self.setup_number)

    @property
    def setup_key(self) -> str:
        dt = self.normalized().first_impulse_at.astimezone(NY_TZ)
        return f"{self.tripletta}|{dt.strftime('%Y-%m-%dT%H:%M:%S%z')}|{self.setup_number}"


@dataclass
class ScanResult:
    processed_now: int = 0
    good_now: int = 0
    rejected_now: int = 0
    skipped_now: int = 0
    residual_to_complete: int = 0
    good_symbols_now: list[str] = field(default_factory=list)
    signal_payloads_now: list[dict[str, Any]] = field(default_factory=list)
    totals_good: int = 0
    totals_rejected: int = 0
    totals_skipped: int = 0
    error_text: str | None = None

    def ensure_totals(self) -> None:
        if self.totals_good < self.good_now:
            self.totals_good = self.good_now
        if self.totals_rejected < self.rejected_now:
            self.totals_rejected = self.rejected_now
        if self.totals_skipped < self.skipped_now:
            self.totals_skipped = self.skipped_now


@dataclass
class AutoResolvedRun:
    phase: str
    setup: SetupContext
    source: str


# -----------------------------
# Time / formatting
# -----------------------------


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)



def now_it() -> datetime:
    return datetime.now(tz=IT_TZ)



def fmt_dt(dt: datetime, tz: ZoneInfo) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(tz).strftime("%d/%m/%y %H:%M:%S %Z")



def fmt_header_timestamp(dt: datetime | None = None) -> str:
    dt = dt or now_it()
    return dt.astimezone(IT_TZ).strftime("[%d/%m/%y %H:%M]")



def build_start_message(market: str, phase: str, setup: SetupContext) -> str:
    return f"INIZIO {PHASE_TITLES[phase]} {MARKET_CONFIG[market]['label']} BINANCE"



def build_end_message(market: str, phase: str, setup: SetupContext, result: ScanResult) -> str:
    setup = setup.normalized()
    result.ensure_totals()
    lines = [
        f"FINE {PHASE_TITLES[phase]} {MARKET_CONFIG[market]['label']} BINANCE",
        f"🕒 New York: {fmt_dt(now_ny(), NY_TZ)}",
        f"🕒 Italia: {fmt_dt(now_it(), IT_TZ)}",
        f"🎯 Candele in analisi: {setup.tripletta}",
        f"📅 Data primo impulso: {fmt_dt(setup.first_impulse_at, NY_TZ)}",
        f"🔢 Numero setup: {setup.setup_number}",
        f"📊 Lavorati ora: {result.processed_now}",
        f"✅ Buoni: {result.good_now}",
        f"❌ Scartati ora: {result.rejected_now}",
        f"⏸️ Non analizzati ora: {result.skipped_now}",
                f"🔁 Residui da completare: {result.residual_to_complete}",
    ]
    if result.error_text:
        lines.append(f"⚠️ Errore: {result.error_text}")
    if result.good_symbols_now:
        lines.append("✅ Setup superato:")
        lines.extend(result.good_symbols_now)
    else:
        lines.append("✅ Setup superato: nessuno")
    if result.signal_payloads_now:
        lines.append(f"📄 Pine/HTML generati: {len(result.signal_payloads_now)}")
    return "\n".join(lines)



def emit_console(message: str) -> None:
    print(f"{fmt_header_timestamp()} {message}", flush=True)


# -----------------------------
# Telegram
# -----------------------------


def _build_ssl_context() -> ssl.SSLContext | None:
    insecure = (os.getenv("SCANNER_INSECURE_SSL", "") or "").strip().lower()
    if insecure in {"1", "true", "yes", "on"}:
        return ssl._create_unverified_context()
    return None


def send_telegram_message(message: str) -> bool:
    bot_token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or TELEGRAM_BOT_TOKEN_FALLBACK).strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID", "") or TELEGRAM_CHAT_ID_FALLBACK).strip()
    if not bot_token or not chat_id:
        return False

    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    req = urllib.request.Request(url, data=payload, method="POST")
    try:
        ssl_context = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=20, context=ssl_context) as resp:
            return 200 <= resp.status < 300
    except urllib.error.URLError as exc:
        emit_console(f"Telegram non inviato: {exc}")
        return False



def notify(message: str) -> None:
    emit_console(message)
    send_telegram_message(message)


# -----------------------------
# State helpers
# -----------------------------


def default_state(market: str) -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "market": market,
        "completed_runs": {},
        "updated_at": None,
    }



def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)



def backup_corrupt_or_old_state(path: Path) -> None:
    if not path.exists():
        return
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUP_DIR / f"{path.stem}_{ts}{path.suffix}"
    shutil.move(str(path), str(backup))



def load_state(market: str) -> dict[str, Any]:
    path = MARKET_CONFIG[market]["state_file"]
    if not path.exists():
        return default_state(market)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        backup_corrupt_or_old_state(path)
        clean = default_state(market)
        atomic_write_json(path, clean)
        return clean

    if data.get("schema_version") != STATE_SCHEMA_VERSION or data.get("market") != market:
        backup_corrupt_or_old_state(path)
        clean = default_state(market)
        atomic_write_json(path, clean)
        return clean

    if "completed_runs" not in data or not isinstance(data["completed_runs"], dict):
        data = default_state(market)
        atomic_write_json(path, data)
        return data

    return data



def prune_completed_runs(state: dict[str, Any], limit: int = MAX_COMPLETED_RUNS_PER_MARKET) -> None:
    completed_runs = state.get("completed_runs", {})
    if not isinstance(completed_runs, dict) or len(completed_runs) <= limit:
        return

    sortable: list[tuple[str, str]] = []
    for setup_key, bucket in completed_runs.items():
        latest = ""
        if isinstance(bucket, dict):
            for phase_data in bucket.values():
                if isinstance(phase_data, dict):
                    latest = max(latest, str(phase_data.get("completed_at", "")))
        sortable.append((setup_key, latest))

    sortable.sort(key=lambda item: item[1])
    to_remove = max(0, len(sortable) - limit)
    for setup_key, _ in sortable[:to_remove]:
        completed_runs.pop(setup_key, None)



def save_state(market: str, data: dict[str, Any]) -> None:
    prune_completed_runs(data)
    data["updated_at"] = datetime.now(tz=UTC).isoformat()
    atomic_write_json(MARKET_CONFIG[market]["state_file"], data)



def is_phase_already_completed(state: dict[str, Any], setup_key: str, phase: str) -> bool:
    return bool(state.get("completed_runs", {}).get(setup_key, {}).get(phase, {}).get("done"))



def mark_phase_completed(state: dict[str, Any], setup_key: str, phase: str, result: ScanResult) -> None:
    run_bucket = state.setdefault("completed_runs", {}).setdefault(setup_key, {})
    run_bucket[phase] = {
        "done": True,
        "completed_at": datetime.now(tz=UTC).isoformat(),
        "result": asdict(result),
    }


# -----------------------------
# Lock helpers
# -----------------------------


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True



def _read_lock_metadata(lock_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return None



def _clear_stale_lock_if_needed(lock_path: Path) -> None:
    if not lock_path.exists():
        return

    stale = False
    meta = _read_lock_metadata(lock_path)
    if meta and isinstance(meta, dict):
        pid = int(meta.get("pid", 0) or 0)
        created_ts = float(meta.get("created_ts", 0) or 0)
        too_old = created_ts and (time.time() - created_ts > LOCK_STALE_SECONDS)
        dead_pid = pid and not _pid_is_alive(pid)
        stale = bool(too_old or dead_pid)
    else:
        age = time.time() - lock_path.stat().st_mtime
        stale = age > LOCK_STALE_SECONDS

    if stale:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass


@contextmanager
def market_lock(market: str):
    lock_path = MARKET_CONFIG[market]["lock_file"]
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    _clear_stale_lock_if_needed(lock_path)

    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd: int | None = None
    try:
        fd = os.open(str(lock_path), flags)
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "created_ts": time.time(),
                "created_at": datetime.now(tz=UTC).isoformat(),
                "market": market,
            },
            ensure_ascii=False,
        )
        os.write(fd, payload.encode("utf-8"))
        os.close(fd)
        fd = None
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass


# -----------------------------
# Engine loader
# -----------------------------


def _scan_result_from_any(value: Any) -> ScanResult:
    if isinstance(value, ScanResult):
        value.ensure_totals()
        return value
    if isinstance(value, dict):
        filtered = {
            key: value.get(key)
            for key in ScanResult.__dataclass_fields__.keys()
            if key in value
        }
        result = ScanResult(**filtered)
        result.ensure_totals()
        return result
    raise TypeError("Il motore reale deve restituire ScanResult oppure dict compatibile")



def _load_module_from_path(module_path: Path):
    spec = importlib.util.spec_from_file_location("crypto_scanner_engine_dynamic", str(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Impossibile caricare il modulo: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module



def resolve_external_runner() -> Callable[[str, str, SetupContext], ScanResult] | None:
    module_name = os.getenv(ENGINE_MODULE_ENV, "").strip()
    engine_file = os.getenv(ENGINE_FILE_ENV, "").strip()

    module = None
    origin = None

    if module_name:
        module = importlib.import_module(module_name)
        origin = f"modulo {module_name}"
    elif engine_file:
        path = Path(engine_file).expanduser()
        if path.exists():
            module = _load_module_from_path(path)
            origin = str(path)
    else:
        for candidate in ENGINE_FILE_CANDIDATES:
            if candidate.exists():
                module = _load_module_from_path(candidate)
                origin = str(candidate)
                break

    if module is None:
        return None

    runner = getattr(module, "scan_market_symbols", None)
    if not callable(runner):
        raise RuntimeError(
            f"Motore esterno trovato in {origin}, ma manca una funzione callable scan_market_symbols"
        )

    def _wrapped_runner(market: str, phase: str, setup: SetupContext) -> ScanResult:
        raw = runner(market, phase, setup)
        return _scan_result_from_any(raw)

    return _wrapped_runner


# -----------------------------
# Dynamic setup resolver
# -----------------------------


def floor_to_4h(dt: datetime) -> datetime:
    dt = dt.astimezone(NY_TZ)
    floored_hour = (dt.hour // 4) * 4
    return dt.replace(hour=floored_hour, minute=0, second=0, microsecond=0)



def next_4h_boundary(dt: datetime) -> datetime:
    return floor_to_4h(dt) + timedelta(hours=4)



def current_4h_window_seconds_left(dt: datetime) -> float:
    dt = dt.astimezone(NY_TZ)
    return (next_4h_boundary(dt) - dt).total_seconds()



def date_setup_number(dt: datetime) -> int:
    local_dt = dt.astimezone(NY_TZ)
    digits = f"{local_dt.day:02d}{local_dt.month:02d}{local_dt.year % 100:02d}"
    total = sum(int(ch) for ch in digits)
    while total > 9:
        total = sum(int(ch) for ch in str(total))
    return 9 if total == 0 else total


def setup_family_for_number(setup_number: int) -> str:
    normalized = ((int(setup_number) - 1) % 9) + 1
    if normalized in {1, 5, 7}:
        return "1/5/7"
    if normalized in {2, 4, 8}:
        return "2/4/8"
    if normalized in {3, 6, 9}:
        return "3/6/9"
    return "ALTRO"



def candle_number_from_open(candle_open: datetime) -> int:
    candle_open = candle_open.astimezone(NY_TZ)
    return (candle_open.hour // 4) + 1



def candle_open_to_setup(third_candle_open: datetime) -> SetupContext:
    third_candle_open = third_candle_open.astimezone(NY_TZ).replace(minute=0, second=0, microsecond=0)
    if third_candle_open.hour not in AUTO_CANDLE_HOURS:
        raise ValueError(f"Orario candela 4H non valido per setup dinamico: {third_candle_open.hour:02d}:00 NY")

    third_number = candle_number_from_open(third_candle_open)
    second_number = ((third_number - 2) % 6) + 1
    first_number = ((third_number - 3) % 6) + 1
    tripletta = f"{first_number},{second_number},{third_number}"
    first_impulse_at = third_candle_open - timedelta(hours=8)

    return SetupContext(
        tripletta=tripletta,
        first_impulse_at=first_impulse_at,
        setup_number=date_setup_number(first_impulse_at),
    ).normalized()



def resolve_current_candle_setup(now_dt: datetime | None = None) -> SetupContext:
    now_dt = (now_dt or now_ny()).astimezone(NY_TZ)
    current_candle_open = floor_to_4h(now_dt)
    return candle_open_to_setup(current_candle_open)



def resolve_latest_closed_candle_setup(now_dt: datetime | None = None) -> SetupContext:
    now_dt = (now_dt or now_ny()).astimezone(NY_TZ)
    latest_closed_candle_open = floor_to_4h(now_dt) - timedelta(hours=4)
    return candle_open_to_setup(latest_closed_candle_open)



def is_within_preanalysis_window(now_dt: datetime | None = None) -> bool:
    now_dt = (now_dt or now_ny()).astimezone(NY_TZ)
    seconds_left = current_4h_window_seconds_left(now_dt)
    return 0 < seconds_left <= max(60, AUTO_PREANALYSIS_WINDOW_MINUTES * 60)



def resolve_auto_run(now_dt: datetime | None = None) -> AutoResolvedRun | None:
    now_dt = (now_dt or now_ny()).astimezone(NY_TZ)
    boundary = floor_to_4h(now_dt)
    seconds_from_boundary = (now_dt - boundary).total_seconds()
    full_window_seconds = max(60, AUTO_FULLANALYSIS_WINDOW_MINUTES * 60)

    if 0 <= seconds_from_boundary < full_window_seconds:
        return AutoResolvedRun(
            phase="fullanalysis",
            setup=resolve_latest_closed_candle_setup(now_dt),
            source="chiusura candela 4H",
        )

    if is_within_preanalysis_window(now_dt):
        return AutoResolvedRun(
            phase="preanalysis",
            setup=resolve_current_candle_setup(now_dt),
            source="ultima finestra prima della chiusura 4H",
        )

    return None



def resolve_requested_run(
    phase: str,
    tripletta: str | None,
    first_impulse_at: datetime | None,
    setup_number: int | None,
    third_candle_close_at: datetime | None,
) -> AutoResolvedRun | None:
    has_manual_setup = bool(tripletta and first_impulse_at is not None and setup_number is not None)
    if has_manual_setup:
        setup = SetupContext(
            tripletta=tripletta or "",
            first_impulse_at=first_impulse_at or now_ny(),
            setup_number=int(setup_number or 0),
        ).normalized()
        resolved_phase = phase
        if phase == "auto":
            if third_candle_close_at is None:
                raise SystemExit("Con --phase auto e setup manuale devi passare --third-candle-close-at")
            resolved_phase = auto_phase_from_now(third_candle_close_at)
        return AutoResolvedRun(phase=resolved_phase, setup=setup, source="parametri manuali")

    if any(value is not None for value in (tripletta, first_impulse_at, setup_number, third_candle_close_at)):
        raise SystemExit(
            "Per il setup manuale devi passare insieme --tripletta, --first-impulse-at e --setup-number"
        )

    if phase == "auto":
        return resolve_auto_run()
    if phase == "preanalysis":
        if not is_within_preanalysis_window():
            raise SystemExit("Nessuna tripletta in PREANALISI attiva in questo orario di New York")
        return AutoResolvedRun(
            phase="preanalysis",
            setup=resolve_current_candle_setup(),
            source="preanalysis dinamica",
        )
    if phase == "prescan":
        return AutoResolvedRun(
            phase="prescan",
            setup=resolve_current_candle_setup(),
            source="prescan dinamico",
        )
    if phase == "fullanalysis":
        return AutoResolvedRun(
            phase="fullanalysis",
            setup=resolve_latest_closed_candle_setup(),
            source="fullanalysis dinamica",
        )
    raise SystemExit(f"Fase non supportata: {phase}")


# -----------------------------
# Phase / scan orchestration
# -----------------------------


def auto_phase_from_now(third_candle_close_at: datetime) -> str:
    if third_candle_close_at.tzinfo is None:
        third_candle_close_at = third_candle_close_at.replace(tzinfo=NY_TZ)
    return "fullanalysis" if now_ny() >= third_candle_close_at.astimezone(NY_TZ) else "preanalysis"


def phase_for_runner(phase: str) -> str:
    return "preanalysis" if phase == "prescan" else phase


def scan_market_symbols(market: str, phase: str, setup: SetupContext) -> ScanResult:
    external_runner = resolve_external_runner()
    if external_runner is not None:
        return external_runner(market, phase_for_runner(phase), setup)

    raise RuntimeError(
        "Motore Binance reale non ancora integrato. "
        "Questo scanner.py gestisce formato output, Telegram, lock e stato, "
        "ma per la scansione reale devi aggiungere ~/Desktop/scanner_engine.py "
        "oppure impostare CRYPTO_SCANNER_ENGINE_MODULE / CRYPTO_SCANNER_ENGINE_FILE."
    )



def run_market(
    market: str,
    phase: str,
    setup: SetupContext,
    runner: Callable[[str, str, SetupContext], ScanResult] = scan_market_symbols,
) -> int:
    market = market.lower()
    phase = phase.lower()
    if market not in MARKET_CONFIG:
        raise ValueError(f"Mercato non supportato: {market}")
    if phase not in PHASE_LABELS:
        raise ValueError(f"Fase non supportata: {phase}")

    setup = setup.normalized()
    state = load_state(market)

    persist_phase = phase != "prescan"

    if persist_phase and is_phase_already_completed(state, setup.setup_key, phase):
        emit_console(
            f"Skip {MARKET_CONFIG[market]['label']} {PHASE_TITLES[phase]}: già completato per setup {setup.setup_key}"
        )
        return 0

    try:
        with market_lock(market):
            notify(build_start_message(market, phase, setup))
            result = ScanResult()
            exit_code = 0
            try:
                result = runner(market, phase, setup)
                result.ensure_totals()
                if persist_phase:
                    mark_phase_completed(state, setup.setup_key, phase, result)
                    save_state(market, state)
            except Exception as exc:
                result.error_text = str(exc)
                exit_code = 1
            notify(build_end_message(market, phase, setup, result))
            return exit_code
    except FileExistsError:
        emit_console(
            f"Skip {MARKET_CONFIG[market]['label']}: lock attivo, evito overlap e nessun recovery loop"
        )
        return 0


# -----------------------------
# CLI
# -----------------------------


def parse_dt(value: str, default_tz: ZoneInfo = NY_TZ) -> datetime:
    value = value.strip()
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Datetime non valida: {value}. Usa ISO, es. 2026-04-02T04:00:00-04:00"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt



def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Crypto Scanner 2 scanner dinamico")
    p.add_argument("--market", choices=["spot", "futures"], required=True)
    p.add_argument(
        "--phase",
        choices=["preanalysis", "prescan", "fullanalysis", "auto"],
        default="auto",
    )
    p.add_argument("--tripletta", help="Es. 2,3,4")
    p.add_argument(
        "--first-impulse-at",
        type=parse_dt,
        help="ISO datetime, es. 2026-04-02T04:00:00-04:00",
    )
    p.add_argument("--setup-number", type=int)
    p.add_argument(
        "--third-candle-close-at",
        type=parse_dt,
        help="Necessario solo con --phase auto e setup manuale",
    )
    p.add_argument(
        "--reset-market-state",
        action="store_true",
        help="Resetta solo lo stato del mercato selezionato prima del run",
    )
    return p



def reset_market_state(market: str) -> None:
    state_file = MARKET_CONFIG[market]["state_file"]
    lock_file = MARKET_CONFIG[market]["lock_file"]
    if state_file.exists():
        backup_corrupt_or_old_state(state_file)
    lock_file.unlink(missing_ok=True)
    save_state(market, default_state(market))



def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    args = build_arg_parser().parse_args(argv)

    if args.reset_market_state:
        reset_market_state(args.market)

    resolved = resolve_requested_run(
        phase=args.phase,
        tripletta=args.tripletta,
        first_impulse_at=args.first_impulse_at,
        setup_number=args.setup_number,
        third_candle_close_at=args.third_candle_close_at,
    )
    if resolved is None:
        emit_console("Nessun setup attivo in questo orario: nessuna scansione avviata")
        return 0

    emit_console(
        f"Setup dinamico selezionato | mercato: {args.market.upper()} | fase: {resolved.phase.upper()} | "
        f"candele: {resolved.setup.tripletta} | primo impulso NY: {fmt_dt(resolved.setup.first_impulse_at, NY_TZ)} | "
        f"numero setup: {resolved.setup.setup_number}"
    )
    return run_market(args.market, resolved.phase, resolved.setup)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    raise SystemExit(main())
