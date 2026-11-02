"""Local Delta futures signal desk. WebSocket feed; live orders disabled by default."""
from __future__ import annotations
import hashlib, hmac, json, os, threading, time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", "8765"))
MARKETS = {
    "india": ("https://api.india.delta.exchange", "wss://public-socket.india.delta.exchange"),
    "global": ("https://api.delta.exchange", "wss://public-socket.india.delta.exchange"),
    "testnet": ("https://cdn-ind.testnet.deltaex.org", "wss://socket-ind-pub.testnet.deltaex.org"),
}
RESOLUTIONS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w"}
SESSION = {"running": False, "config": {}, "rows": {}, "logs": deque(maxlen=300), "orders": [], "ws": None, "thread": None, "last_auto": {}, "credentials": None}
LOCK = threading.RLock()
STOP = threading.Event()


def log(message, level="info"):
    with LOCK:
        SESSION["logs"].appendleft({"time": time.strftime("%H:%M:%S"), "level": level, "message": str(message)[:300]})


def record_order(symbol, side, size, signal_price, target, stop, status):
    with LOCK:
        SESSION["orders"].insert(0,{"time":time.strftime("%H:%M:%S"),"symbol":str(symbol or "—"),"side":str(side or "—").upper(),
            "size":size or "—","signal_price":signal_price or 0,"target":target or 0,"stop":stop or 0,"status":str(status)[:100]})


def live_orders_enabled():
    # Explicit in-app credential connection arms manual orders; env flag remains an alternative.
    return os.environ.get("ENABLE_LIVE_TRADING") == "true" or bool(SESSION.get("credentials"))


def api_request(base, path, params=None, method="GET", payload=None, auth=False):
    query = urlencode(params or {})
    request_path = path + ("?" + query if query else "")
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) if payload is not None else ""
    headers = {"Accept": "application/json", "User-Agent": "local-delta-signal-desk/1.0"}
    if body: headers["Content-Type"] = "application/json"
    if auth:
        key, secret = os.environ.get("DELTA_API_KEY", ""), os.environ.get("DELTA_API_SECRET", "")
        if not key or not secret: raise ValueError("Set DELTA_API_KEY and DELTA_API_SECRET in the server environment.")
        ts = str(int(time.time()))
        sig = hmac.new(secret.encode(), (method + ts + request_path + body).encode(), hashlib.sha256).hexdigest()
        headers.update({"api-key": key, "timestamp": ts, "signature": sig})
    req = Request(base + request_path, data=body.encode() if body else None, headers=headers, method=method)
    try:
        with urlopen(req, timeout=12) as response: data = json.loads(response.read().decode())
    except (HTTPError, URLError, TimeoutError) as exc:
        detail = exc.read().decode(errors="replace") if isinstance(exc, HTTPError) else str(exc)
        raise RuntimeError(f"Delta API request failed: {detail[:250]}") from exc
    if isinstance(data, dict) and data.get("success") is False: raise RuntimeError(str(data.get("error", "Delta API error"))[:250])
    return data


def ema(values, period):
    alpha, out = 2 / (period + 1), values[0]
    for value in values[1:]: out = alpha * value + (1 - alpha) * out
    return out


def rsi(values, period):
    if len(values) <= period: return 50.0
    diffs = [values[i] - values[i-1] for i in range(1, len(values))]
    gains = sum(max(x, 0) for x in diffs[-period:]) / period
    losses = sum(max(-x, 0) for x in diffs[-period:]) / period
    return 100.0 if losses == 0 else 100 - 100 / (1 + gains / losses)


def make_signal(symbol, candles, config, last_price=None):
    candles = candles[-500:]
    closes = [float(c.get("close", c.get("c", 0))) for c in candles if c.get("close", c.get("c")) is not None]
    if len(closes) < max(int(config["slow_ema"]) + 2, int(config["rsi_period"]) + 2):
        raise ValueError(f"{symbol}: waiting for enough candles")
    fast, slow = ema(closes[-max(100, int(config["slow_ema"]) * 3):], int(config["fast_ema"])), ema(closes[-max(100, int(config["slow_ema"]) * 3):], int(config["slow_ema"]))
    momentum = rsi(closes, int(config["rsi_period"]))
    candle = candles[-1]
    op = float(candle.get("open", candle.get("o", closes[-1])))
    close = float(last_price if last_price is not None else closes[-1])
    trend = "UPTREND" if fast > slow else "DOWNTREND" if fast < slow else "SIDEWAYS"
    if fast > slow and momentum < float(config["rsi_overbought"]): state = "BUY"
    elif fast < slow and momentum > float(config["rsi_oversold"]): state = "SELL"
    else: state = "NEUTRAL"
    return {"symbol": symbol, "price": close, "ema_fast": fast, "ema_slow": slow, "rsi": momentum,
            "trend": trend, "candle": "BULLISH" if close >= op else "BEARISH", "momentum": "STRONG" if momentum >= 60 or momentum <= 40 else "MIXED",
            "signal": state, "resolution": config["resolution"], "timestamp": int(candle.get("time", candle.get("t", time.time())))}


def initial_candles(base, symbol, resolution):
    seconds = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,"4h":14400,"6h":21600,"12h":43200,"1d":86400,"1w":604800}[resolution]
    end = int(time.time())
    data = api_request(base, "/v2/history/candles", {"resolution":resolution,"symbol":symbol,"start":end-seconds*160,"end":end})
    return data.get("result", [])


def run_stream(config):
    try:
        import websocket
        base, ws_url = MARKETS[config["market"]]
        cache = {s: initial_candles(base, s, config["resolution"]) for s in config["symbols"]}
        ready = {}
        for symbol, candles in cache.items():
            try: ready[symbol] = make_signal(symbol, candles, config)
            except Exception as exc: log(exc, "warn")
        with LOCK: SESSION["rows"].update(ready)
        # When the user explicitly enables Auto-Trade and confirms Start, act on
        # each current directional signal once instead of waiting for a crossover.
        if config.get("auto_trade"):
            for symbol, row in ready.items(): maybe_auto_trade(symbol, row, config)
        def on_open(ws):
            channels = [{"name": "candlesticks_" + config["resolution"], "symbols": config["symbols"]},
                        {"name": "ticker", "symbols": config["symbols"]},
                        {"name": "trades", "symbols": config["symbols"]}]
            ws.send(json.dumps({"type":"subscribe","payload":{"channels":channels}}))
            log("Live WebSocket connected; subscribed to candles and tickers.")
        def on_message(ws, raw):
            try:
                msg = json.loads(raw)
                data = msg.get("data", msg)
                if not isinstance(data, dict): return
                symbol = str(data.get("symbol", data.get("sy", data.get("s", "")))).upper()
                if symbol not in cache: return
                kind = str(data.get("type", msg.get("type", ""))).lower()
                if "ticker" in kind or "mark_price" in kind or "trade" in kind:
                    px = data.get("close", data.get("last_price", data.get("price", data.get("p", data.get("c")))))
                    if px is not None and symbol in SESSION["rows"]:
                        price = float(px)
                        if cache[symbol]:
                            cache[symbol][-1]["close"] = price
                            cache[symbol][-1]["high"] = max(float(cache[symbol][-1].get("high", price) or price), price)
                            cache[symbol][-1]["low"] = min(float(cache[symbol][-1].get("low", price) or price), price)
                            old = SESSION["rows"].get(symbol, {}).get("signal")
                            row = make_signal(symbol, cache[symbol], config, last_price=price)
                            with LOCK: SESSION["rows"][symbol] = row
                            if row["signal"] != old:
                                log(f"{symbol}: {old or 'READY'} → {row['signal']} ({price})")
                                maybe_auto_trade(symbol, row, config)
                        else: SESSION["rows"][symbol]["price"] = price
                elif any(k in data for k in ("close", "c")):
                    raw_time = int(data.get("time", data.get("t", data.get("ts", time.time()))))
                    if raw_time > 10**12: raw_time //= 1_000_000
                    interval = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,"4h":14400,"6h":21600,"12h":43200,"1d":86400,"1w":604800}[config["resolution"]]
                    candle_time = raw_time - raw_time % interval
                    candle = {"time":candle_time,"open":data.get("open",data.get("o")),
                              "high":data.get("high",data.get("h")),"low":data.get("low",data.get("l")),
                              "close":data.get("close",data.get("c")),"volume":data.get("volume",data.get("v",0))}
                    stamp = candle["time"]
                    if cache[symbol] and str(cache[symbol][-1].get("time", cache[symbol][-1].get("t"))) == str(stamp): cache[symbol][-1] = candle
                    else: cache[symbol].append(candle)
                    old = SESSION["rows"].get(symbol, {}).get("signal")
                    row = make_signal(symbol, cache[symbol], config)
                    with LOCK: SESSION["rows"][symbol] = row
                    if row["signal"] != old:
                        log(f"{symbol}: {old or 'READY'} → {row['signal']} ({row['price']})")
                        maybe_auto_trade(symbol, row, config)
            except Exception as exc: log(f"Stream update: {exc}", "warn")
        def on_error(ws, error): log(f"WebSocket: {error}", "warn")
        def on_close(ws, code, reason): log("Market stream disconnected; reconnecting…", "warn")
        while not STOP.is_set():
            ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            with LOCK: SESSION["ws"] = ws
            ws.run_forever(ping_interval=20, ping_timeout=10)
            if not STOP.is_set(): STOP.wait(2)
    except Exception as exc:
        log(f"Scanner stopped: {exc}", "error")
    finally:
        with LOCK:
            SESSION["running"] = False
            SESSION["ws"] = None
        log("Scanner stopped.")


def tradehull_client():
    creds = SESSION.get("credentials")
    key = (creds or {}).get("key") or os.environ.get("DELTA_API_KEY", "")
    secret = (creds or {}).get("secret") or os.environ.get("DELTA_API_SECRET", "")
    if not key or not secret:
        raise ValueError("Set replacement API credentials in the local environment.")
    import Tradehull_Delta_Exchange as TDX
    return TDX.login(api_key=key, api_secret=secret)


def bracket_order(payload):
    if not live_orders_enabled(): raise PermissionError("Connect API credentials or enable ENABLE_LIVE_TRADING in the server environment.")
    if payload.get("confirm") is not True: raise ValueError("Order confirmation is required.")
    if payload.get("market") != "india": raise ValueError("Tradehull bracket orders are enabled for Delta India only.")
    symbol, side = str(payload.get("symbol", "")).upper(), payload.get("side")
    size, stop, target = int(payload.get("size", 0)), float(payload.get("stop_loss", 0)), float(payload.get("target", 0))
    max_size = int(os.environ.get("MAX_ORDER_SIZE", "1"))
    if not symbol or side not in {"buy", "sell"} or size < 1 or size > max_size: raise ValueError(f"Order size must be between 1 and {max_size}.")
    ticker = api_request(MARKETS["india"][0], "/v2/tickers/" + symbol).get("result", {})
    price = float(ticker.get("close", ticker.get("last_price", 0)))
    if price <= 0: raise ValueError("Could not get a valid last price for bracket validation.")
    if (side == "buy" and not (stop < price < target)) or (side == "sell" and not (target < price < stop)):
        raise ValueError(f"Invalid bracket around current price {price}.")
    client = tradehull_client()
    try:
        result = client.place_bracket_order(symbol=symbol, side=side, quantity=size, entry_price=None,
                   target_price=target, sl_price=stop, sl_limit_price=stop, target_limit_price=target)
        record_order(symbol,side,size,price,target,stop,"SUBMITTED")
        log(f"LIVE {side.upper()} bracket {symbol} size={size}; stop={stop}; target={target}", "order")
        return result
    finally:
        close = getattr(client, "close", None)
        if close: close()


def maybe_auto_trade(symbol, row, config):
    if not config.get("auto_trade") or not live_orders_enabled(): return
    stamp = row.get("timestamp")
    key = f"{symbol}:{stamp}"
    if row["signal"] not in {"BUY", "SELL"} or SESSION["last_auto"].get(symbol) == key: return
    with LOCK: SESSION["last_auto"][symbol] = key
    try:
        client = tradehull_client()
        try: positions = client.get_open_positions() or []
        finally:
            close = getattr(client, "close", None)
            if close: close()
        already_open = next((p for p in positions if str(p.get("symbol",p.get("product_symbol",""))).upper()==symbol and float(p.get("size",0) or 0)!=0),None)
        if already_open:
            record_order(symbol,row["signal"],int(config["size"]),row["price"],0,0,"SKIPPED: position already open")
            log(f"Auto-trade skipped for {symbol}: an open position already exists.","warn")
            return
    except Exception as exc:
        side = "buy" if row["signal"] == "BUY" else "sell"
        px = row["price"]
        sp,tp = float(config["stop_loss_pct"])/100,float(config["target_pct"])/100
        stop,target = (px*(1-sp),px*(1+tp)) if side=="buy" else (px*(1+sp),px*(1-tp))
        record_order(symbol,side,int(config["size"]),px,target,stop,f"FAILED preflight: {exc}")
        log(f"Auto-trade preflight failed for {symbol}: {exc}","error")
        return
    side = "buy" if row["signal"] == "BUY" else "sell"
    px = row["price"]
    stop_pct, target_pct = float(config["stop_loss_pct"])/100, float(config["target_pct"])/100
    stop, target = (px*(1-stop_pct),px*(1+target_pct)) if side == "buy" else (px*(1+stop_pct),px*(1-target_pct))
    try:
        result = bracket_order({"confirm":True,"market":config["market"],"symbol":symbol,"side":side,"size":int(config["size"]),"stop_loss":stop,"target":target})
        log(f"Auto-trade accepted for {symbol}: {result}", "info")
    except Exception as exc:
        record_order(symbol,side,int(config["size"]),px,target,stop,f"FAILED: {exc}")
        log(f"Auto-trade failed for {symbol}: {exc}", "error")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): print("%s - %s" % (self.address_string(), fmt % args))
    def body(self): return json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
    def send_json(self, status, obj):
        body=json.dumps(obj).encode(); self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(body))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        parsed=urlparse(self.path); p=parsed.path
        if p == "/": p="/index.html"
        if p == "/api/status":
            with LOCK: state={"running":SESSION["running"],"rows":list(SESSION["rows"].values()),"logs":list(SESSION["logs"]),"orders":SESSION["orders"][:100],"config":SESSION["config"]}
            state.update({"live_enabled":live_orders_enabled(),"auto_enabled":live_orders_enabled(),"credentials_configured":bool(SESSION.get("credentials") or (os.environ.get("DELTA_API_KEY") and os.environ.get("DELTA_API_SECRET"))),"credentials_source":"manual session" if SESSION.get("credentials") else "environment","max_order_size":int(os.environ.get("MAX_ORDER_SIZE","1"))})
            return self.send_json(200,state)
        if p == "/api/positions":
            try:
                client=tradehull_client()
                try: positions=client.get_open_positions()
                finally:
                    close=getattr(client,"close",None)
                    if close: close()
                return self.send_json(200,{"positions":positions or []})
            except Exception as exc: return self.send_json(400,{"error":str(exc)[:250]})
        if p == "/api/events":
            self.send_response(200); self.send_header("Content-Type","text/event-stream"); self.send_header("Cache-Control","no-cache"); self.send_header("Connection","keep-alive"); self.end_headers()
            try:
                while True:
                    with LOCK: snapshot={"running":SESSION["running"],"rows":list(SESSION["rows"].values()),"logs":list(SESSION["logs"]),"orders":SESSION["orders"][:100],"config":SESSION["config"]}
                    self.wfile.write(("data: "+json.dumps(snapshot)+"\n\n").encode()); self.wfile.flush(); time.sleep(.5)
            except (BrokenPipeError, ConnectionResetError): pass
            return
        path=ROOT/"static"/p.lstrip("/")
        if path.parent==ROOT/"static" and path.is_file():
            ext=path.suffix; typ={".html":"text/html",".css":"text/css",".js":"text/javascript"}.get(ext,"application/octet-stream")+"; charset=utf-8"; body=path.read_bytes()
            self.send_response(200); self.send_header("Content-Type",typ); self.send_header("Content-Length",str(len(body))); self.send_header("X-Content-Type-Options","nosniff"); self.end_headers(); self.wfile.write(body); return
        self.send_json(404,{"error":"Not found"})
    def do_POST(self):
        p=urlparse(self.path).path
        try:
            data=self.body()
            if p == "/api/credentials":
                key=str(data.get("api_key","")).strip(); secret=str(data.get("api_secret","")).strip()
                if not key or not secret: raise ValueError("Enter both API key and API secret.")
                if len(key)>250 or len(secret)>500: raise ValueError("Credential value is too long.")
                import Tradehull_Delta_Exchange as TDX
                client=TDX.login(api_key=key,api_secret=secret)
                try: positions=client.get_open_positions()
                finally:
                    close=getattr(client,"close",None)
                    if close: close()
                with LOCK: SESSION["credentials"]={"key":key,"secret":secret}
                log("API credentials connected for this server session. Credentials are held in memory only.")
                return self.send_json(200,{"ok":True,"position_count":len(positions or [])})
            if p == "/api/start":
                market=data.get("market","india"); symbols=[s.strip().upper() for s in data.get("symbols",[]) if s.strip()]
                res=data.get("resolution","5m"); fast=int(data.get("fast_ema",9)); slow=int(data.get("slow_ema",21)); period=int(data.get("rsi_period",14))
                config={"market":market,"symbols":symbols,"resolution":res,"fast_ema":fast,"slow_ema":slow,"rsi_period":period,"rsi_overbought":float(data.get("rsi_overbought",70)),"rsi_oversold":float(data.get("rsi_oversold",30)),"stop_loss_pct":float(data.get("stop_loss_pct",1)),"target_pct":float(data.get("target_pct",2)),"size":int(data.get("size",1)),"auto_trade":bool(data.get("auto_trade",False))}
                if market not in MARKETS or res not in RESOLUTIONS or not symbols or len(symbols)>12 or any(len(s)>40 or not all(c.isalnum() or c in "-_" for c in s) for s in symbols): raise ValueError("Choose valid market, timeframe, and up to 12 futures symbols.")
                if fast<2 or slow<=fast or slow>200 or period<2 or period>100: raise ValueError("Use EMA fast ≥2, slow above fast and ≤200; RSI period 2–100.")
                if not 50<float(config["rsi_overbought"])<100 or not 0<float(config["rsi_oversold"])<50: raise ValueError("RSI overbought must exceed 50 and oversold must be below 50.")
                if not 1<=config["stop_loss_pct"]<=50 or not 1<=config["target_pct"]<=100 or config["size"]<1 or config["size"]>int(os.environ.get("MAX_ORDER_SIZE","1")): raise ValueError("Risk percentages or order size exceed configured limits.")
                if config["auto_trade"] and (not live_orders_enabled() or market!="india"): raise PermissionError("Auto-trading requires connected credentials or live opt-in and Delta India.")
                with LOCK:
                    old_thread=SESSION.get("thread") if SESSION["running"] else None
                    old_ws=SESSION.get("ws") if SESSION["running"] else None
                if old_thread:
                    STOP.set()
                    if old_ws: old_ws.close()
                    old_thread.join(timeout=8)
                    if old_thread.is_alive(): raise RuntimeError("Current scanner did not stop cleanly. Use Stop, wait for STOPPED, then Start again.")
                with LOCK:
                    SESSION["rows"]={}; SESSION["logs"]=deque(maxlen=300); SESSION["orders"]=[]; SESSION["last_auto"]={}; SESSION["config"]=config; SESSION["running"]=True
                STOP.clear(); log("New session started. Existing session log cleared.")
                thread=threading.Thread(target=run_stream,args=(config,),daemon=True); SESSION["thread"]=thread; thread.start()
                return self.send_json(200,{"ok":True})
            if p == "/api/stop":
                STOP.set()
                with LOCK: ws=SESSION.get("ws")
                if ws: ws.close()
                log("Stop requested."); return self.send_json(200,{"ok":True})
            if p == "/api/execute":
                result=bracket_order(data)
                return self.send_json(200,{"success":True,"result":result})
            if p == "/api/close-all":
                if not live_orders_enabled(): raise PermissionError("Connect credentials before closing positions.")
                if data.get("confirm") is not True: raise ValueError("Close-all confirmation is required.")
                if data.get("market")!="india": raise ValueError("Tradehull close-all currently supports Delta India only.")
                client=tradehull_client()
                try: result=client.close_all_orders()
                finally:
                    close=getattr(client,"close",None)
                    if close: close()
                log("CLOSE ALL requested: cancel orders and close positions.","order"); return self.send_json(200,{"success":True,"result":result})
            if p == "/api/close-position":
                if not live_orders_enabled(): raise PermissionError("Connect credentials before closing positions.")
                if data.get("confirm") is not True: raise ValueError("Position close confirmation is required.")
                symbol=str(data.get("symbol","")).upper()
                client=tradehull_client()
                try:
                    positions=client.get_open_positions() or []
                    position=next((x for x in positions if str(x.get("symbol",x.get("product_symbol",""))).upper()==symbol),None)
                    if not position: raise ValueError("No open position found for this symbol.")
                    result=client.close_position_market(symbol=symbol,product_id=int(position["product_id"]),size=abs(int(float(position["size"]))),side=str(position.get("side","buy")).lower())
                finally:
                    close=getattr(client,"close",None)
                    if close: close()
                log(f"Position exit requested for {symbol}.","order"); return self.send_json(200,{"success":True,"result":result})
            return self.send_json(404,{"error":"Not found"})
        except Exception as exc:
            if p=="/api/execute" and isinstance(locals().get("data"),dict):
                record_order(data.get("symbol"),data.get("side"),data.get("size"),data.get("signal_price"),data.get("target"),data.get("stop_loss"),f"FAILED: {exc}")
                log(f"Order failed: {exc}","error")
            return self.send_json(400,{"error":str(exc)[:300]})

if __name__ == "__main__":
    print(f"Delta Signal Desk: http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("127.0.0.1",PORT),Handler).serve_forever()
