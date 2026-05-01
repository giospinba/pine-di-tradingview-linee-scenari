#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import signal
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

BASE_DIR = Path.home() / "Desktop"
NY_TZ = ZoneInfo("America/New_York")
IT_TZ = ZoneInfo("Europe/Rome")
UTC = timezone.utc
STATE_FILE = BASE_DIR / "scanner_state_upbit.json"
LOCK_FILE = BASE_DIR / "scanner_upbit.lock"
LOG_FILE = BASE_DIR / "scanner_upbit.log"
LOCK_STALE_SECONDS = 4 * 60 * 60
ENGINE_FILE = Path(__file__).resolve().parent / "scanner_engine_upbit.py"
AUTO_FULLANALYSIS_WINDOW_MINUTES = int(os.getenv("SCANNER_AUTO_FULL_WINDOW_MINUTES", "10") or "10")
VALID_THIRD_OPEN_HOURS = {8, 12, 16, 20}
TELEGRAM_BOT_TOKEN_FALLBACK = ""
TELEGRAM_CHAT_ID_FALLBACK = ""

PHASE_TITLES = {
    "prescan": "PRESCAN",
    "preanalysis": "PREANALISI",
    "fullanalysis": "ANALISI COMPLETA",
}
PHASE_LABELS = {
    "prescan": "PRESCAN = scansione manuale forzata disponibile in qualsiasi momento",
    "preanalysis": "PREANALISI = terza candela non ancora chiusa",
    "fullanalysis": "ANALISI COMPLETA = terza candela chiusa",
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
        return SetupContext(self.tripletta.strip(), dt, int(self.setup_number))

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
    totals_good: int = 0
    totals_rejected: int = 0
    totals_skipped: int = 0
    error_text: str | None = None


@dataclass
class AutoResolvedRun:
    phase: str
    setup: SetupContext
    source: str


def _emit(msg: str) -> None:
    line = f"[{datetime.now(tz=IT_TZ).strftime('%d/%m/%y %H:%M')}] {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def _build_ssl_context() -> ssl.SSLContext:
    insecure = os.getenv("SCANNER_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    if insecure:
        return ssl._create_unverified_context()
    return ssl.create_default_context()


def send_telegram_message(message: str) -> bool:
    bot_token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or TELEGRAM_BOT_TOKEN_FALLBACK).strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID", "") or TELEGRAM_CHAT_ID_FALLBACK).strip()
    if not bot_token or not chat_id:
        return False
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": message, "disable_web_page_preview": "true"}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{bot_token}/sendMessage", data=payload, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20, context=_build_ssl_context()) as resp:
            return 200 <= resp.status < 300
    except urllib.error.URLError as exc:
        _emit(f"Telegram non inviato: {exc}")
        return False


def notify(message: str) -> None:
    _emit(message)
    send_telegram_message(message)


def floor_to_4h(dt: datetime) -> datetime:
    dt = dt.astimezone(NY_TZ)
    return dt.replace(hour=(dt.hour // 4) * 4, minute=0, second=0, microsecond=0)


def date_setup_number(dt: datetime) -> int:
    local_dt = dt.astimezone(NY_TZ)
    digits = f"{local_dt.day:02d}{local_dt.month:02d}{local_dt.year % 100:02d}"
    total = sum(int(ch) for ch in digits)
    while total > 9:
        total = sum(int(ch) for ch in str(total))
    return 9 if total == 0 else total


def third_open_to_setup(third_open: datetime) -> SetupContext | None:
    third_open = third_open.astimezone(NY_TZ).replace(minute=0, second=0, microsecond=0)
    if third_open.hour not in VALID_THIRD_OPEN_HOURS:
        return None
    third_candle_number = (third_open.hour // 4) + 1
    first_candle_number = third_candle_number - 2
    second_candle_number = third_candle_number - 1
    if first_candle_number <= 0:
        first_candle_number += 6
    if second_candle_number <= 0:
        second_candle_number += 6
    tripletta = f"{first_candle_number},{second_candle_number},{third_candle_number}"
    first_impulse_at = third_open - timedelta(hours=8)
    return SetupContext(tripletta, first_impulse_at, date_setup_number(first_impulse_at)).normalized()


def resolve_auto_run(now_dt: datetime | None = None) -> AutoResolvedRun | None:
    now_dt = (now_dt or datetime.now(tz=NY_TZ)).astimezone(NY_TZ)
    boundary = floor_to_4h(now_dt)
    seconds_from_boundary = (now_dt - boundary).total_seconds()
    full_window_seconds = max(60, AUTO_FULLANALYSIS_WINDOW_MINUTES * 60)
    if 0 <= seconds_from_boundary < full_window_seconds:
        just_closed_third_open = boundary - timedelta(hours=4)
        setup = third_open_to_setup(just_closed_third_open)
        if setup:
            return AutoResolvedRun("fullanalysis", setup, "chiusura candela 4H")
    current_third_open = boundary
    setup = third_open_to_setup(current_third_open)
    if setup:
        return AutoResolvedRun("preanalysis", setup, "terza candela attiva")
    return None



def resolve_forced_prescan_setup(now_dt: datetime | None = None) -> SetupContext:
    now_dt = (now_dt or datetime.now(tz=NY_TZ)).astimezone(NY_TZ)
    third_open = now_dt.replace(
        hour=(now_dt.hour // 4) * 4,
        minute=0,
        second=0,
        microsecond=0,
    )
    first_impulse_at = third_open - timedelta(hours=8)
    first_no = (first_impulse_at.hour // 4) + 1
    second_no = (((first_impulse_at + timedelta(hours=4)).hour) // 4) + 1
    third_no = ((third_open.hour // 4) + 1)
    tripletta = f"{first_no},{second_no},{third_no}"
    return SetupContext(tripletta, first_impulse_at, date_setup_number(first_impulse_at)).normalized()


def resolve_latest_fullanalysis_setup(now_dt: datetime | None = None) -> SetupContext | None:
    now_dt = (now_dt or datetime.now(tz=NY_TZ)).astimezone(NY_TZ)
    boundary = floor_to_4h(now_dt)
    for step_back in range(0, 7):
        candidate_boundary = boundary - timedelta(hours=4 * step_back)
        setup = third_open_to_setup(candidate_boundary - timedelta(hours=4))
        if setup:
            return setup
    return None


def resolve_requested_run(phase: str) -> AutoResolvedRun | None:
    if phase == "auto":
        return resolve_auto_run()
    if phase == "prescan":
        setup = resolve_forced_prescan_setup()
        return AutoResolvedRun("prescan", setup, "prescan dinamica")
    if phase == "preanalysis":
        setup = third_open_to_setup(floor_to_4h(datetime.now(tz=NY_TZ)))
        if setup is None:
            raise SystemExit("Nessuna tripletta attiva in questo orario di New York")
        return AutoResolvedRun("preanalysis", setup, "preanalysis dinamica")
    if phase == "fullanalysis":
        setup = resolve_latest_fullanalysis_setup()
        if setup is None:
            return None
        return AutoResolvedRun("fullanalysis", setup, "fullanalysis dinamica")
    raise SystemExit(f"Fase non supportata: {phase}")


def _load_engine_runner() -> Callable[[str, str, SetupContext], ScanResult]:
    spec = importlib.util.spec_from_file_location("scanner_engine_upbit_dynamic", str(ENGINE_FILE))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Impossibile caricare il motore Upbit: {ENGINE_FILE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    runner = getattr(module, "scan_market_symbols", None)
    if not callable(runner):
        raise RuntimeError("scanner_engine_upbit.py non espone scan_market_symbols")

    def _wrapped(_market: str, phase: str, setup: SetupContext) -> ScanResult:
        raw = runner("upbit", phase, setup)
        if isinstance(raw, ScanResult):
            return raw
        if isinstance(raw, dict):
            return ScanResult(**{k: raw.get(k) for k in ScanResult.__dataclass_fields__.keys() if k in raw})
        raise TypeError("Il motore Upbit deve restituire dict o ScanResult")

    return _wrapped


def _default_state() -> dict[str, Any]:
    return {"completed_runs": {}, "updated_at": None}


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return _default_state()
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()


def _save_state(data: dict[str, Any]) -> None:
    data["updated_at"] = datetime.now(tz=UTC).isoformat()
    fd, temp_path = tempfile.mkstemp(prefix=STATE_FILE.name + ".", dir=str(STATE_FILE.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_path, STATE_FILE)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def _is_phase_already_completed(state: dict[str, Any], setup_key: str, phase: str) -> bool:
    return bool(state.get("completed_runs", {}).get(setup_key, {}).get(phase, {}).get("done"))


def _mark_phase_completed(state: dict[str, Any], setup_key: str, phase: str, result: ScanResult) -> None:
    state.setdefault("completed_runs", {}).setdefault(setup_key, {})[phase] = {
        "done": True,
        "completed_at": datetime.now(tz=UTC).isoformat(),
        "result": asdict(result),
    }


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


def _clear_stale_lock() -> None:
    if not LOCK_FILE.exists():
        return
    try:
        meta = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
    except Exception:
        if time.time() - LOCK_FILE.stat().st_mtime > LOCK_STALE_SECONDS:
            LOCK_FILE.unlink(missing_ok=True)
        return
    pid = int(meta.get("pid", 0) or 0)
    created_ts = float(meta.get("created_ts", 0) or 0)
    if (created_ts and time.time() - created_ts > LOCK_STALE_SECONDS) or (pid and not _pid_is_alive(pid)):
        LOCK_FILE.unlink(missing_ok=True)


@contextmanager
def market_lock():
    _clear_stale_lock()
    fd = None
    try:
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, json.dumps({"pid": os.getpid(), "created_ts": time.time()}, ensure_ascii=False).encode("utf-8"))
        os.close(fd)
        fd = None
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        LOCK_FILE.unlink(missing_ok=True)


def build_start_message(phase: str, setup: SetupContext) -> str:
    return (
        f"INIZIO {PHASE_TITLES[phase]} UPBIT\n"
        f"🕒 NY: {datetime.now(tz=NY_TZ).strftime('%d/%m/%y %H:%M:%S EDT/EST')}\n"
        f"🕒 Italia: {datetime.now(tz=IT_TZ).strftime('%d/%m/%y %H:%M:%S %Z')}\n"
        f"🎯 Candele in analisi: {setup.tripletta}\n"
        f"📅 1 impulso: {setup.first_impulse_at.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M %Z')}\n"
        f"🔢 Numero setup: {setup.setup_number}\n"
        f"⚙️ Modalità: {PHASE_LABELS[phase]}"
    )


def build_end_message(phase: str, setup: SetupContext, result: ScanResult) -> str:
    lines = [
        f"{PHASE_TITLES[phase]} UPBIT",
        f"🕒 NY: {datetime.now(tz=NY_TZ).strftime('%d/%m/%y %H:%M:%S EDT/EST')}",
        f"🕒 Italia: {datetime.now(tz=IT_TZ).strftime('%d/%m/%y %H:%M:%S %Z')}",
        f"🎯 Candele in analisi: {setup.tripletta}",
        f"📅 1 impulso: {setup.first_impulse_at.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M %Z')}",
        f"🔢 Numero setup: {setup.setup_number}",
        f"📊 Lavorati ora: {result.processed_now}",
        f"✅ Buoni: {result.good_now}",
        f"❌ Scartati ora: {result.rejected_now}",
        f"⏸️ Non analizzati ora: {result.skipped_now}",
        f"🔁 Residui da completare: {result.residual_to_complete}",
    ]
    if result.error_text:
        lines.append(f"⚠️ Errore: {result.error_text}")
    lines.append("✅ Setup superato:" if result.good_symbols_now else "✅ Setup superato: nessuno")
    if result.good_symbols_now:
        lines.extend(result.good_symbols_now)
    return "\n".join(lines)



def build_skip_message(phase: str, setup: SetupContext) -> str:
    now_ny = datetime.now(tz=NY_TZ)
    now_it = now_ny.astimezone(IT_TZ)
    return (
        f"{PHASE_TITLES[phase]} UPBIT\n"
        f"🕒 NY: {now_ny.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M:%S %Z')}\n"
        f"🕒 Italia: {now_it.strftime('%d/%m/%y %H:%M:%S %Z')}\n"
        f"🎯 Candele in analisi: {setup.tripletta}\n"
        f"📅 1 impulso: {setup.first_impulse_at.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M %Z')}\n"
        f"🔢 Numero setup: {setup.setup_number}\n"
        f"⚠️ Stato: già completato"
    )

def run_market(phase: str, setup: SetupContext) -> int:
    state = _load_state()
    if _is_phase_already_completed(state, setup.setup_key, phase):
        _emit(f"Skip UPBIT {PHASE_TITLES[phase]}: già completato per setup {setup.setup_key}")
        try:
            notify(build_skip_message(phase, setup))
        except Exception:
            pass
        return 0
    runner = _load_engine_runner()
    try:
        with market_lock():
            notify(build_start_message(phase, setup))
            result = ScanResult()
            code = 0
            try:
                result = runner("upbit", phase, setup)
                _mark_phase_completed(state, setup.setup_key, phase, result)
                _save_state(state)
            except Exception as exc:
                result.error_text = str(exc)
                code = 1
            notify(build_end_message(phase, setup, result))
            return code
    except FileExistsError:
        _emit("Skip UPBIT: lock attivo, evito overlap")
        return 0


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Crypto Scanner 2 Upbit")
    p.add_argument("--phase", choices=["prescan", "preanalysis", "fullanalysis", "auto"], default="auto")
    p.add_argument("--reset-state", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv or sys.argv[1:])
    if args.reset_state:
        STATE_FILE.unlink(missing_ok=True)
        LOCK_FILE.unlink(missing_ok=True)

    if args.phase == "prescan":
        setup = resolve_forced_prescan_setup()
        _emit(
            f"Setup dinamico selezionato | mercato: UPBIT | fase: PRESCAN | candele: {setup.tripletta} | "
            f"1 impulso NY: {setup.first_impulse_at.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M %Z')} | numero setup: {setup.setup_number}"
        )
        return run_market("prescan", setup)

    resolved = resolve_requested_run(args.phase)
    if resolved is None:
        _emit("Nessun setup attivo in questo orario: nessuna scansione avviata")
        return 0

    _emit(
        f"Setup dinamico selezionato | mercato: UPBIT | fase: {resolved.phase.upper()} | candele: {resolved.setup.tripletta} | "
        f"1 impulso NY: {resolved.setup.first_impulse_at.astimezone(NY_TZ).strftime('%d/%m/%y %H:%M %Z')} | numero setup: {resolved.setup.setup_number}"
    )
    return run_market(resolved.phase, resolved.setup)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    raise SystemExit(main())
