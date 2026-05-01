# Pine di TradingView: Linee Scenari

Repository dello scanner che genera segnali, Pine Script e HTML di analisi in modo dinamico.

## Contenuto

- `scanner.py`: orchestratore principale.
- `scanner_engine.py`: logica di analisi e generazione artefatti.
- `scanner_upbit.py`: flusso dedicato a Upbit.
- `pine-scripts/`: output generati per Pine Script e pagina HTML.

## Flusso

Quando arriva un segnale, il motore genera automaticamente gli artefatti Pine e HTML nella cartella `pine-scripts/`.
Il Pine è compilato in modo dinamico a partire dal segnale e dai dati disponibili al momento.

## Repository pubblico

https://github.com/giospinba/pine-di-tradingview-linee-scenari