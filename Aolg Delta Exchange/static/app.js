const $ = id => document.getElementById(id);
let lastRows = [], liveAllowed = false, autoAllowed = false, stream, lastOrderLog = '', suppressNextLogChime = false, positionsLoaded = false, sideOverride = false;
const fmt = n => Number(n).toLocaleString(undefined,{maximumFractionDigits:2});
const esc = s => { const e=document.createElement('span');e.textContent=String(s);return e.innerHTML; };

function config() {
  return {market:$('market').value, symbols:$('symbols').value.split(/[\s,]+/).map(x=>x.trim().toUpperCase()).filter(Boolean), resolution:$('resolution').value,
    fast_ema:$('fast-ema').value,slow_ema:$('slow-ema').value,rsi_period:$('rsi-period').value,rsi_overbought:$('rsi-overbought').value,rsi_oversold:$('rsi-oversold').value,
    stop_loss_pct:$('stop-loss').value,target_pct:$('target').value,size:$('size').value,auto_trade:$('auto-trade').checked};
}
async function post(path, body={}) {
  const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const j=await r.json();if(!r.ok)throw new Error(j.error||'Request failed');return j;
}
async function refreshStatus() {
  try { const s=await (await fetch('/api/status')).json();
    liveAllowed=s.live_enabled&&s.credentials_configured&&$('market').value==='india'; autoAllowed=liveAllowed&&s.auto_enabled;
    $('connection-note').textContent=s.credentials_configured?`API connected (${s.credentials_source}) · public feed active`:'Public stream available · connect API for positions and orders';
    $('credential-state').textContent=s.credentials_configured?'CONNECTED':'NOT CONNECTED';
    $('credential-state').className=s.credentials_configured?'chip':'chip neutral';
    $('mode-title').textContent=liveAllowed?'LIVE ORDERS ARMED':'PAPER MODE';$('mode-caption').textContent=liveAllowed?'Confirmation required for manual orders':'Live orders locked';
    $('mode-dot').style.background=liveAllowed?'#e7bb68':'';$('execution-chip').textContent=liveAllowed?'LIVE ARMED':'PAPER ONLY';
    $('live-lock-title').textContent=liveAllowed?'Live orders unlocked · manual confirmation required':'Connect API credentials to unlock orders';
    $('live-lock-hint').textContent=liveAllowed?'Manual orders require a final confirmation. Auto-trading stays off until you enable it in Strategy and confirm Start.':'Enter your replacement API key and secret in API Credentials, then select Connect. To automate, turn on Auto-Trade in Strategy and confirm Start. Current signals are acted on at session start and when their direction changes.';
    $('order-button').disabled=!liveAllowed;$('order-button').className=liveAllowed?'primary':'disabled';$('order-button').textContent=liveAllowed?'Place confirmed bracket order':'Live execution disabled';
    $('order-size').max=String(s.max_order_size||1);$('size').max=String(s.max_order_size||1);
    $('close-all').disabled=!liveAllowed;$('close-all').title=liveAllowed?'Close all positions':'Enable live trading and connect credentials first';
    if(s.running) $('session-status').textContent='RUNNING';
    paintRows(s.rows||[],s.config?.symbols||config().symbols);paintLogs(s.logs||[]);paintOrders(s.orders||[]);
    const knownOrder=s.logs?.find(x=>x.level==='order');lastOrderLog=knownOrder?knownOrder.time+knownOrder.message:'';
    $('auto-status').textContent=s.config?.auto_trade?'AUTO-TRADE ON':'AUTO-TRADE OFF';$('auto-status').className=s.config?.auto_trade?'chip auto-on':'chip neutral';if(s.config)$('auto-trade').checked=!!s.config.auto_trade;
    if(s.config) { $('stat-symbols').textContent=(s.config.symbols||[]).join(', ')||'—'; }
    if(s.credentials_configured&&!positionsLoaded) refreshPositions();
    if(s.running&&!stream) connectEvents();
  } catch(e) { $('connection-note').textContent='Local server unavailable'; }
}
function connectEvents(){
  if(stream)stream.close();stream=new EventSource('/api/events');
  stream.onmessage=e=>{const s=JSON.parse(e.data);$('session-status').textContent=s.running?'RUNNING':'STOPPED';$('status-dot').style.background=s.running?'var(--green)':'#66717c';$('stream-note').textContent=s.running?'Continuous WebSocket stream · no polling interval':'No stream active';paintRows(s.rows||[],s.config?.symbols||config().symbols);paintOrders(s.orders||[]);$('auto-status').textContent=s.config?.auto_trade?'AUTO-TRADE ON':'AUTO-TRADE OFF';$('auto-status').className=s.config?.auto_trade?'chip auto-on':'chip neutral';if(s.config)$('auto-trade').checked=!!s.config.auto_trade;const logs=s.logs||[];const order=logs.find(x=>x.level==='order');const orderKey=order?order.time+order.message:'';if(orderKey&&orderKey!==lastOrderLog){if(suppressNextLogChime)suppressNextLogChime=false;else chime();}lastOrderLog=orderKey;paintLogs(logs);if((s.rows||[]).length){$('stat-time').textContent=new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});}};
  stream.onerror=()=>{};
}
function paintRows(rows,symbols=[]){
  const bySymbol=new Map(rows.map(r=>[r.symbol,r]));const names=[...new Set([...(symbols||[]),...rows.map(r=>r.symbol)])];lastRows=rows;$('empty-state').style.display=rows.length?'none':'flex';
  $('results').innerHTML=names.length?names.map(symbol=>{const r=bySymbol.get(symbol);return r?`<tr><td>${esc(r.symbol)}</td><td>${fmt(r.price)}</td><td>${fmt(r.rsi)}</td><td>${fmt(r.ema_fast)}</td><td>${fmt(r.ema_slow)}</td><td>${esc(r.trend)}</td><td>${esc(r.candle)}</td><td>${esc(r.momentum)}</td><td><span class="bias ${String(r.signal).toLowerCase()}">${esc(r.signal)}</span></td></tr>`:`<tr><td>${esc(symbol)}</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td><span class="bias neutral">PENDING</span></td></tr>`;}).join(''):'<tr><td colspan="9" class="empty-cell">Add symbols to your watchlist.</td></tr>';
  const counts={BUY:0,SELL:0,NEUTRAL:0};rows.forEach(r=>counts[r.signal]=(counts[r.signal]||0)+1);$('buy-count').textContent=counts.BUY;$('sell-count').textContent=counts.SELL;$('neutral-count').textContent=counts.NEUTRAL;
  $('signal-chip').textContent=names.length?`${names.length} CONTRACT${names.length===1?'':'S'}`:'WAITING';
  const prev=$('order-symbol').value;const opts=rows.map(r=>`<option>${esc(r.symbol)}</option>`).join('');if(opts){$('order-symbol').innerHTML=opts;if(rows.some(r=>r.symbol===prev))$('order-symbol').value=prev;updateTicket();}
}
function paintOrders(orders){$('orders').innerHTML=orders.length?orders.map(o=>`<tr><td>${esc(o.time)}</td><td>${esc(o.symbol)}</td><td>${esc(o.side)}</td><td>${esc(o.size)}</td><td>${fmt(o.signal_price)}</td><td>${fmt(o.target)}</td><td>${fmt(o.stop)}</td><td>${esc(o.status)}</td></tr>`).join(''):'<tr><td colspan="8" class="empty-cell">No orders placed yet.</td></tr>';}
async function refreshPositions(){
  try{const response=await fetch('/api/positions');const data=await response.json();if(!response.ok)throw new Error(data.error||'Connect API to load positions.');const rows=data.positions||[];positionsLoaded=true;$('position-count').textContent=rows.length;$('positions').innerHTML=rows.length?rows.map(p=>`<tr><td>${esc(p.symbol||p.product_symbol||'—')}</td><td>${esc(p.side||'—')}</td><td>${esc(p.size)}</td><td>${fmt(p.entry_price||p.average_entry_price||0)}</td><td>${fmt(p.mark_price||p.mark||0)}</td><td>${fmt(p.unrealized_pnl||0)}</td><td><button class="danger exit-one" data-symbol="${esc(p.symbol||p.product_symbol||'')}">Exit</button></td></tr>`).join(''):'<tr><td colspan="7" class="empty-cell">No open positions.</td></tr>';}
  catch(e){$('position-count').textContent='—';$('positions').innerHTML=`<tr><td colspan="7" class="empty-cell">${esc(e.message)}</td></tr>`;}
}
async function connectCredentials(){
  const key=$('api-key').value.trim(),secret=$('api-secret').value.trim();$('credential-message').textContent='Connecting…';
  try{await post('/api/credentials',{api_key:key,api_secret:secret});$('api-key').value='';$('api-secret').value='';positionsLoaded=false;$('credential-message').textContent='Connected for this server run. Credentials were not saved.';await refreshStatus();await refreshPositions();}
  catch(e){$('credential-message').textContent=e.message;}
}
function saveWatchlist(){const symbols=config().symbols;if(!symbols.length||symbols.length>12){$('config-error').textContent='Add 1–12 symbols before saving the watchlist.';return;}localStorage.setItem('delta-watchlist',symbols.join('\n'));$('config-error').textContent='Watchlist saved in this browser.';$('stat-symbols').textContent=symbols.join(', ');paintRows(lastRows,symbols);}
function saveSettings(){const c=config();localStorage.setItem('delta-settings',JSON.stringify({...c,symbols:undefined,auto_trade:false}));$('config-error').textContent='Order settings saved in this browser. API credentials are never saved.';}
function paintLogs(logs){$('log-count').textContent=`${logs.length} event${logs.length===1?'':'s'} · newest first`;$('log-list').innerHTML=logs.length?logs.slice(0,60).map(x=>`<div class="log-row ${esc(x.level)}"><time>${esc(x.time)}</time><span>${esc(x.message)}</span></div>`).join(''):'<div class="log-row muted">No session events yet.</div>';}
async function start(){
  $('config-error').textContent='';const c=config();
  if(c.auto_trade&&!autoAllowed){$('config-error').textContent='Connect your API credentials before enabling auto-trade.';return;}
  if(c.auto_trade&&!confirm('Enable auto-trading for this scanner session? Directional signals can submit real market-entry bracket orders without asking again for each order. Confirm only if you intend this.'))return;
  try{await post('/api/start',c);connectEvents();$('stat-symbols').textContent=c.symbols.join(', ');$('session-status').textContent='STARTING';}
  catch(e){$('config-error').textContent=e.message;}
}
async function stop(){try{await post('/api/stop');$('session-status').textContent='STOPPING';}catch(e){$('config-error').textContent=e.message;}}
function updateTicket(){
  const r=lastRows.find(x=>x.symbol===$('order-symbol').value);if(!r)return;
  if(!sideOverride&&['BUY','SELL'].includes(r.signal))$('order-side').value=r.signal==='SELL'?'sell':'buy';
  const side=$('order-side').value,sl=Number($('stop-loss').value)/100,tp=Number($('target').value)/100,p=Number(r.price);
  $('order-stop').value=(side==='buy'?p*(1-sl):p*(1+sl)).toPrecision(8).replace(/\.?0+$/,'');
  $('order-target').value=(side==='buy'?p*(1+tp):p*(1-tp)).toPrecision(8).replace(/\.?0+$/,'');
}
function chime(){try{const c=new(window.AudioContext||window.webkitAudioContext)();[880,1174].forEach((f,i)=>{const o=c.createOscillator(),g=c.createGain();o.frequency.value=f;o.type='sine';g.gain.setValueAtTime(.0001,c.currentTime+i*.16);g.gain.exponentialRampToValueAtTime(.22,c.currentTime+i*.16+.02);g.gain.exponentialRampToValueAtTime(.0001,c.currentTime+i*.16+.28);o.connect(g);g.connect(c.destination);o.start(c.currentTime+i*.16);o.stop(c.currentTime+i*.16+.3);});setTimeout(()=>c.close(),1000);}catch{}}
async function execute(){
  const symbol=$('order-symbol').value,side=$('order-side').value,size=Number($('order-size').value),stop=Number($('order-stop').value),target=Number($('order-target').value),row=lastRows.find(x=>x.symbol===symbol),price=Number(row?.price);
  if(!row||!price||!stop||!target)return $('order-message').textContent='Start the scan and set both bracket prices first.';
  if((side==='buy'&&!(stop<price&&price<target))||(side==='sell'&&!(target<price&&price<stop)))return $('order-message').textContent=`Bracket prices do not surround the latest price ${fmt(price)} for this side.`;
  if(!confirm(`Confirm LIVE ${side.toUpperCase()} ${size} ${symbol} at market with stop ${fmt(stop)} and target ${fmt(target)}?`))return;
  $('order-message').textContent='Submitting bracket order…';
  try{await post('/api/execute',{confirm:true,market:$('market').value,symbol,side,size,signal_price:price,stop_loss:stop,target});$('order-message').textContent='Bracket order accepted by the Tradehull client.';suppressNextLogChime=true;chime();positionsLoaded=false;refreshStatus();}
  catch(e){$('order-message').textContent=e.message;}
}
async function closeAll(){
  if(!liveAllowed)return $('order-message').textContent='Live execution is disabled.';
  if(!confirm('Close all open positions and cancel orders for the connected Delta India account? This affects every symbol.'))return;
  try{await post('/api/close-all',{confirm:true,market:$('market').value});$('order-message').textContent='Close-all request sent. Check exchange positions and orders.';suppressNextLogChime=true;chime();positionsLoaded=false;refreshStatus();}
  catch(e){$('order-message').textContent=e.message;}
}
$('start-button').addEventListener('click',start);$('stop-button').addEventListener('click',stop);$('order-button').addEventListener('click',execute);$('close-all').addEventListener('click',closeAll);
$('connect-button').addEventListener('click',connectCredentials);$('save-watchlist').addEventListener('click',saveWatchlist);$('save-settings').addEventListener('click',saveSettings);$('refresh-positions').addEventListener('click',refreshPositions);
document.addEventListener('click',async e=>{const button=e.target.closest('.exit-one');if(!button)return;const symbol=button.dataset.symbol;if(!confirm(`Close the entire ${symbol} position with a reduce-only market order?`))return;try{await post('/api/close-position',{confirm:true,symbol});chime();positionsLoaded=false;await refreshPositions();await refreshStatus();}catch(err){$('order-message').textContent=err.message;}});
['stop-loss','target'].forEach(id=>$(id).addEventListener('change',updateTicket));
$('order-symbol').addEventListener('change',()=>{sideOverride=false;updateTicket();});$('order-side').addEventListener('change',()=>{sideOverride=true;updateTicket();});
$('auto-trade').addEventListener('change',()=>{$('auto-status').textContent=$('auto-trade').checked?'AUTO-TRADE ON':'AUTO-TRADE OFF';$('auto-status').className=$('auto-trade').checked?'chip auto-on':'chip neutral';});
$('symbols').addEventListener('input',()=>{$('stat-symbols').textContent=config().symbols.join(', ');});$('size').addEventListener('input',()=>{$('order-size').value=$('size').value;});
 $('market').addEventListener('change',refreshStatus);
try{const saved=localStorage.getItem('delta-watchlist');if(saved)$('symbols').value=saved;const savedSettings=JSON.parse(localStorage.getItem('delta-settings')||'null');if(savedSettings){$('resolution').value=savedSettings.resolution||'5m';$('fast-ema').value=savedSettings.fast_ema||9;$('slow-ema').value=savedSettings.slow_ema||21;$('rsi-period').value=savedSettings.rsi_period||14;$('rsi-overbought').value=savedSettings.rsi_overbought||70;$('rsi-oversold').value=savedSettings.rsi_oversold||30;$('size').value=savedSettings.size||1;$('order-size').value=savedSettings.size||1;$('stop-loss').value=savedSettings.stop_loss_pct||1;$('target').value=savedSettings.target_pct||2;}}catch{}
paintRows([],config().symbols);
refreshStatus();
