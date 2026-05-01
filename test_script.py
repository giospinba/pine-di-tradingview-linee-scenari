import sys
import os
import glob
from scanner_engine import generate_pine_script

def mock_generate_pine_script(be, impulso1, impulso2, impulso3, spike1_up, spike2_low, spike2_up, spike3_low, start_time):
    return f"""
//@version=5
indicator("Gann Signal SWARMSUSDT", overlay=true)
be = {be}
impulso1 = {impulso1}
impulso2 = {impulso2}
impulso3 = {impulso3}
spike1_up = {spike1_up}
spike2_low = {spike2_low}
spike2_up = {spike2_up}
spike3_low = {spike3_low}
start_time_str = "{start_time}"

entry_c1 = {be - impulso1}
entry_c2 = {be - (impulso1 + impulso2 - spike1_up - spike2_low)}
entry_c3 = {be - (impulso1 + impulso2 + impulso3 - spike1_up - spike2_low - spike2_up - spike3_low)}

// --- CALCOLO BARRA DI INIZIO ---
start_time = timestamp("{start_time}")
"""

print(mock_generate_pine_script(be=0.025, impulso1=0.001, impulso2=0.002, impulso3=0.003, spike1_up=0.0001, spike2_low=0.0002, spike2_up=0.0003, spike3_low=0.0004, start_time='2026-04-27T08:00:00-04:00'))

# Esempio: lista di segnali
signals = [
    {
        "symbol": "BTCUSDT",
        "exchange": "BINANCE",
        "be": 0.025,
        "impulso1": 0.001,
        "impulso2": 0.002,
        "impulso3": 0.003,
        "spike1_up": 0.0001,
        "spike2_low": 0.0002,
        "spike2_up": 0.0003,
        "spike3_low": 0.0004,
        "start_time": "2026-04-27T08:00:00-04:00"
    },
    {
        "symbol": "ETHUSDT",
        "exchange": "BINANCE",
        "be": 0.015,
        "impulso1": 0.0008,
        "impulso2": 0.0015,
        "impulso3": 0.0022,
        "spike1_up": 0.00005,
        "spike2_low": 0.0001,
        "spike2_up": 0.00012,
        "spike3_low": 0.00013,
        "start_time": "2026-04-27T08:00:00-04:00"
    }
]

# 1. Cancella tutti i vecchi Pine Script
for f in glob.glob("pine-scripts/*_pine_script.txt"):
    os.remove(f)

# 2. Genera, salva e prepara commit per tutti i segnali
pine_links = []
for sig in signals:
    pine_code = generate_pine_script(
        sig["be"], sig["impulso1"], sig["impulso2"], sig["impulso3"],
        sig["spike1_up"], sig["spike2_low"], sig["spike2_up"], sig["spike3_low"], sig["start_time"]
    )
    pine_path = f"pine-scripts/{sig['symbol']}_pine_script.txt"
    with open(pine_path, "w") as f:
        f.write(pine_code)
    os.system(f"git add {pine_path}")
    pine_github_link = f"https://github.com/cryptoscanner-lab/pine-scripts/blob/main/{sig['symbol']}_pine_script.txt"
    pine_links.append((sig["symbol"], pine_github_link, sig["exchange"]))

os.system("git commit -m 'Aggiorna Pine Script batch'")
os.system("git push")

# 3. Messaggio Telegram con tutti i link
telegram_message = "\n".join([
    f"🚀 Segnale Gann su {symbol} ({exchange})\n<a href=\"{link}\">📄 Pine Script</a>" for symbol, link, exchange in pine_links
])
print(telegram_message)
