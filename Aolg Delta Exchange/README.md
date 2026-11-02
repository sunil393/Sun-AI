# Delta Signal Desk

Local Python and browser workspace for continuously streaming Delta futures data, calculating configurable EMA/RSI signals, and submitting confirmed bracket orders.

## Install and start

1. Install Python 3.10 or newer.
2. In this folder, run `python -m pip install -r requirements.txt`.
3. Run `python app.py` and open `http://127.0.0.1:8765`.
4. Configure the watchlist and strategy, then select **Start scan**. The server loads candle history once and then consumes Delta's public WebSocket feed. There is no market scan polling interval.

## Configure

Select exchange, futures symbols, timeframe, EMA periods, RSI period and thresholds, position size, stop-loss percent, and target percent. BUY/SELL is the EMA direction filtered by RSI thresholds; other readings show NEUTRAL. This simple rule is educational and is not a profitable-strategy claim.

## Orders and kill switch

Connect a Delta API key from the in-app credentials panel. The key is held in server memory only for the current process and is cleared when the server exits; it is not written to disk or browser storage. Connecting enables confirmed manual orders. `ENABLE_LIVE_TRADING=true` can also arm an environment-configured server. A manual order requires browser confirmation and uses the Tradehull futures bracket helper. The Close all button requires a separate confirmation and calls the package's close-all function, which attempts to cancel open/bracket orders and close positions across the connected Delta India account.

Auto-trade is off by default. Enable it in the strategy panel and confirm again when starting the scan; this authorizes the scanner to submit bracket orders on a new directional signal, without asking for each order. Tradehull order placement and close-all currently apply to Delta India only. Use a dedicated API key, a strict IP allowlist, minimum size, and demo account while evaluating the setup.

PowerShell example (use newly generated credentials; never commit secrets):

```powershell
 $env:DELTA_API_KEY = "your-new-key"
 $env:DELTA_API_SECRET = "your-new-secret"
 $env:ENABLE_LIVE_TRADING = "false"
 $env:MAX_ORDER_SIZE = "1"
 python app.py
```

## References

- [Tradehull Delta Exchange package](https://pypi.org/project/Tradehull-Delta-Exchange/)
- [Delta Exchange Python REST client README](https://github.com/delta-exchange/python-rest-client/blob/master/README.md)
- [Official Delta Exchange API docs](https://docs.delta.exchange/)

The workspace binds to `127.0.0.1`. API secrets are not sent to the browser. Futures trading can result in rapid losses; signals and bracket orders do not guarantee an exit price or limit losses.
