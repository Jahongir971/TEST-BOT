// Telegram Test Bot (Single File)
// Required packages:
//   npm i node-telegram-bot-api mongodb
// Run:
//   node index.js

const TelegramBot = require("node-telegram-bot-api");
const { MongoClient, ObjectId } = require("mongodb");
const dns = require("dns");

if (dns.setDefaultResultOrder) {
  dns.setDefaultResultOrder("ipv4first");
}

// =====================
// CONFIG (no .env)
// =====================
const CONFIG = {
  BOT_TOKEN: "8644020661:AAG8j5sc2dboVqF0jr7OswtOhiYOzVovvFo",
  MONGODB_URI:
    "mongodb://jtoshtemirov263_db_user:testbot@ac-flqgcrw-shard-00-00.tfu3blb.mongodb.net:27017,ac-flqgcrw-shard-00-01.tfu3blb.mongodb.net:27017,ac-flqgcrw-shard-00-02.tfu3blb.mongodb.net:27017/?replicaSet=atlas-e2qlyu-shard-0&ssl=true&authSource=admin",
  ADMIN_IDS: [7074605949, 990389210], // <-- Replace with real admin Telegram IDs
  DEFAULT_CODE_LENGTH: 8,
  PAGE_SIZE_USERS: 6,
  PAGE_SIZE_TESTS: 6,
  PAGE_SIZE_RESULTS: 8,
  DUP_CLICK_MS: 700,
  CHAT_HISTORY_LIMIT: 1000000,
  TEST_TIME_LIMIT_SEC: 1800, // 30 minutes
};

if (!CONFIG.BOT_TOKEN || CONFIG.BOT_TOKEN.startsWith("PASTE_")) {
  console.error("BOT_TOKEN is missing. Please set it in CONFIG.");
  process.exit(1);
}

const bot = new TelegramBot(CONFIG.BOT_TOKEN, {
  polling: false,
  request: { family: 4, timeout: 30000 },
});
const mongo = new MongoClient(CONFIG.MONGODB_URI);

const _sendMessage = bot.sendMessage.bind(bot);
bot.sendMessage = async (chatId, text, options) => {
  const res = await _sendMessage(chatId, text, options);
  try {
    await trackBotMessage(chatId, res?.message_id);
  } catch (err) {
    // ignore tracking errors
  }
  return res;
};

const _sendPhoto = bot.sendPhoto.bind(bot);
bot.sendPhoto = async (chatId, photo, options) => {
  const res = await _sendPhoto(chatId, photo, options);
  try {
    await trackBotMessage(chatId, res?.message_id);
  } catch (err) {
    // ignore tracking errors
  }
  return res;
};

let db, usersCol, testsCol, resultsCol, settingsCol, sessionsCol, logsCol;
let dbReady = false;

bot.on("polling_error", (err) => {
  if (err.code === "EFATAL") {
    console.error("Fatal polling error:", err.message);
    setTimeout(() => {
      console.log("Attempting to restart polling...");
      bot.startPolling().catch((restartErr) => {
        console.error("Polling restart failed:", restartErr);
      });
    }, 5000);
  } else {
    console.error("Polling error:", err);
  }
});

bot.on("error", (err) => {
  console.error("Bot error:", err);
});

const defaultSettings = {
  _id: "main",
  botName: "Test Bot",
  welcomeText:
    "Assalomu alaykum, {name}. Botga xush kelibsiz. Ishlash uchun testni tanlang.",
  codeLength: CONFIG.DEFAULT_CODE_LENGTH,
  registrationEnabled: true,
  testEnabled: true,
  adminLogChannelId: null,
  testTimeLimitEnabled: false,
};

// =====================
// Helpers
// =====================
const now = () => new Date();

function isAdmin(id) {
  return CONFIG.ADMIN_IDS.includes(Number(id));
}

function esc(str = "") {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function formatDate(d) {
  if (!d) return "-";
  return new Date(d).toLocaleString("uz-UZ");
}

function humanDuration(seconds) {
  const s = Math.max(0, Math.floor(seconds));
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}m ${r}s`;
}

function elapsedSeconds(sessionData) {
  if (!sessionData?.startedAt) return 0;
  return Math.floor(
    (Date.now() - new Date(sessionData.startedAt).getTime()) / 1000,
  );
}

function scoreLabel(pct) {
  if (pct >= 90) return "A'lo";
  if (pct >= 75) return "Yaxshi";
  if (pct >= 55) return "Qoniqarli";
  return "Yomon";
}

function kb(rows) {
  return { reply_markup: { inline_keyboard: rows } };
}

async function sendOrEdit(chatId, messageId, text, options = {}) {
  const params = {
    parse_mode: "HTML",
    disable_web_page_preview: true,
    protect_content: true,
    ...options,
  };
  if (messageId) {
    try {
      await bot.editMessageText(text, {
        chat_id: chatId,
        message_id: messageId,
        ...params,
      });
      return { edited: true };
    } catch (err) {
      // fallthrough to sendMessage
    }
  }
  await bot.sendMessage(chatId, text, params);
  return { edited: false };
}

async function clearInlineKeyboard(chatId, messageId) {
  if (!messageId) return;
  try {
    await bot.editMessageReplyMarkup(
      { inline_keyboard: [] },
      { chat_id: chatId, message_id: messageId },
    );
  } catch (err) {
    // ignore
  }
}

async function safeAnswerCallback(q, text) {
  try {
    await bot.answerCallbackQuery(q.id, text ? { text } : {});
  } catch (err) {
    // ignore
  }
}

function isPrivateChat(chat) {
  return chat?.type === "private";
}

async function ensurePrivateChat(q, userId) {
  if (isPrivateChat(q?.message?.chat)) return true;
  const warn =
    "Bu amal uchun bot bilan shaxsiy (private) chatda ishlang.";
  await safeAnswerCallback(q, warn);
  try {
    await bot.sendMessage(userId, warn, { protect_content: true });
  } catch (err) {
    // ignore
  }
  return false;
}

function genCode(len) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let out = "";
  for (let i = 0; i < len; i++) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

async function uniqueCode(len) {
  for (let i = 0; i < 10; i++) {
    const code = genCode(len);
    const exists = await usersCol.findOne({ oneTimeCode: code });
    if (!exists) return code;
  }
  throw new Error("Unique code generation failed");
}

async function getSettings() {
  if (!settingsCol) return defaultSettings;
  const s = await settingsCol.findOne({ _id: "main" });
  return s || defaultSettings;
}

async function ensureSettings() {
  await settingsCol.updateOne(
    { _id: "main" },
    { $setOnInsert: defaultSettings },
    { upsert: true },
  );
}

async function getSession(userId) {
  if (!sessionsCol)
    return {
      userId: Number(userId),
      state: null,
      data: {},
      lastCallbackAt: 0,
      lastMessageId: null,
      updatedAt: now(),
    };
  let s = await sessionsCol.findOne({ userId: Number(userId) });
  if (!s) {
    s = {
      userId: Number(userId),
      state: null,
      data: {},
      lastCallbackAt: 0,
      lastMessageId: null,
      updatedAt: now(),
    };
    await sessionsCol.insertOne(s);
  }
  return s;
}

async function setSession(userId, patch) {
  if (!sessionsCol) return;
  await sessionsCol.updateOne(
    { userId: Number(userId) },
    { $set: { ...patch, updatedAt: now() } },
    { upsert: true },
  );
}

async function clearSession(userId) {
  if (!sessionsCol) return;
  await sessionsCol.updateOne(
    { userId: Number(userId) },
    { $set: { state: null, data: {}, updatedAt: now() } },
    { upsert: true },
  );
}

async function wipeUserSession(userId) {
  if (!sessionsCol) return;
  if (userId === null || userId === undefined) return;
  await sessionsCol.deleteOne({ userId: Number(userId) });
}

async function recomputeTestsStats(testIds) {
  if (!testsCol || !resultsCol) return;
  if (!Array.isArray(testIds) || testIds.length === 0) return;

  const unique = [];
  const seen = new Set();
  for (const id of testIds) {
    const key = String(id);
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(id);
  }
  if (!unique.length) return;

  const stats = await resultsCol
    .aggregate([
      { $match: { testId: { $in: unique } } },
      {
        $group: {
          _id: "$testId",
          total: { $sum: 1 },
          avg: { $avg: "$percentage" },
          lastUsedAt: { $max: "$createdAt" },
        },
      },
    ])
    .toArray();

  const byId = new Map(stats.map((s) => [String(s._id), s]));
  const ops = unique.map((id) => {
    const s = byId.get(String(id));
    return {
      updateOne: {
        filter: { _id: id },
        update: {
          $set: {
            totalAttempts: s ? s.total : 0,
            averageScore: s ? s.avg : 0,
            lastUsedAt: s ? s.lastUsedAt : null,
          },
        },
      },
    };
  });
  if (ops.length) {
    await testsCol.bulkWrite(ops, { ordered: false });
  }
}

async function wipeUserHistory(userDoc) {
  if (!userDoc || !usersCol) return;
  let testIds = [];
  if (resultsCol) {
    testIds = await resultsCol.distinct("testId", { userId: userDoc._id });
    await resultsCol.deleteMany({ userId: userDoc._id });
  }
  await usersCol.updateOne(
    { _id: userDoc._id },
    {
      $set: {
        totalTestsWorked: 0,
        averageScore: 0,
        lastResult: null,
        lastActiveAt: null,
        joinedAt: null,
        results: [],
      },
    },
  );
  if (userDoc.telegramId) {
    await purgeChatHistory(userDoc.telegramId);
    await wipeUserSession(userDoc.telegramId);
  }
  await recomputeTestsStats(testIds);
}

async function trackMessageId(userId, field, messageId) {
  if (!sessionsCol) return;
  const uid = Number(userId);
  if (!Number.isFinite(uid) || uid <= 0) return;
  if (!Number.isFinite(messageId)) return;
  await sessionsCol.updateOne(
    { userId: uid },
    {
      $push: {
        [field]: { $each: [messageId], $slice: -CONFIG.CHAT_HISTORY_LIMIT },
      },
      $setOnInsert: {
        userId: uid,
        state: null,
        data: {},
        lastCallbackAt: 0,
        lastMessageId: null,
        updatedAt: now(),
      },
    },
    { upsert: true },
  );
}

async function trackBotMessage(chatId, messageId) {
  await trackMessageId(chatId, "botMsgIds", messageId);
}

async function trackUserMessage(userId, messageId) {
  await trackMessageId(userId, "userMsgIds", messageId);
}

async function purgeChatHistory(userId) {
  if (!sessionsCol) return;
  const uid = Number(userId);
  if (!Number.isFinite(uid) || uid <= 0) return;
  const s = await sessionsCol.findOne({ userId: uid });
  const botIds = Array.isArray(s?.botMsgIds) ? s.botMsgIds : [];
  const userIds = Array.isArray(s?.userMsgIds) ? s.userMsgIds : [];
  const ids = [...botIds, ...userIds];
  if (!ids.length) return;

  const uniqueIds = Array.from(new Set(ids));
  for (const mid of uniqueIds) {
    try {
      await bot.deleteMessage(uid, mid);
    } catch (err) {
      // ignore (Telegram may refuse old messages)
    }
  }

  await sessionsCol.updateOne(
    { userId: uid },
    { $set: { botMsgIds: [], userMsgIds: [] } },
  );
}

async function logAdminAction(adminId, action, meta = {}) {
  const settings = await getSettings();
  const log = { adminId: Number(adminId), action, meta, createdAt: now() };
  await logsCol.insertOne(log);
  if (settings.adminLogChannelId) {
    const text = `🧾 <b>Admin log</b>\nAdmin: <code>${adminId}</code>\nAction: <b>${esc(action)}</b>\nTime: ${formatDate(log.createdAt)}`;
    try {
      await bot.sendMessage(settings.adminLogChannelId, text, {
        parse_mode: "HTML",
        protect_content: true,
      });
    } catch (err) {
      // ignore
    }
  }
}

// =====================
// parseQuestionBlock
// Preserves the original question text exactly as the admin typed it.
// Supports bilingual (two-block) questions.
// =====================
function parseQuestionBlock(text) {
  const optionRegex = /^([A-Da-d])\s*[\)\.\:-]\s*(.+)$/;

  // Split into raw lines preserving original wording (only trim trailing spaces)
  const rawLines = text.split(/\r?\n/).map((l) => l.trimEnd());

  // Parse into blocks. Each block = question lines + A/B/C/D options.
  const blocks = [];
  let qLines = [];
  let opts = { A: null, B: null, C: null, D: null };
  let inOpts = false;

  const saveBlock = () => {
    if (qLines.length && opts.A && opts.B && opts.C && opts.D) {
      blocks.push({ qLines: [...qLines], opts: { ...opts } });
    }
    qLines = [];
    opts = { A: null, B: null, C: null, D: null };
    inOpts = false;
  };

  for (const line of rawLines) {
    const trimmed = line.trim();
    if (!trimmed) {
      // blank line between blocks
      if (inOpts && opts.A && opts.B && opts.C && opts.D) saveBlock();
      continue;
    }
    const m = trimmed.match(optionRegex);
    if (m) {
      inOpts = true;
      const letter = m[1].toUpperCase();
      if (!opts[letter]) opts[letter] = m[2].trim();
    } else {
      // non-option line
      if (inOpts && opts.A && opts.B && opts.C && opts.D) saveBlock();
      qLines.push(trimmed);
    }
  }
  // save last block
  saveBlock();

  if (!blocks.length) return null;

  if (blocks.length === 1) {
    const b = blocks[0];
    return {
      question: b.qLines.join("\n"),
      options: { A: b.opts.A, B: b.opts.B, C: b.opts.C, D: b.opts.D },
    };
  }

  // Two language blocks: merge questions with blank line between,
  // options on separate lines (English on top, Uzbek below)
  const main = blocks[0];
  const alt = blocks[1];
  return {
    question: main.qLines.join("\n") + "\n\n" + alt.qLines.join("\n"),
    options: {
      A: main.opts.A + " / " + alt.opts.A,
      B: main.opts.B + " / " + alt.opts.B,
      C: main.opts.C + " / " + alt.opts.C,
      D: main.opts.D + " / " + alt.opts.D,
    },
  };
}

async function updateLastActive(userId) {
  if (!usersCol) return;
  await usersCol.updateOne(
    { telegramId: Number(userId) },
    { $set: { lastActiveAt: now() } },
  );
}

// =====================
// Renderers
// =====================
function adminMenu(name) {
  const text = `Assalomu Aleykum, <b>${esc(name)}</b>. Botning admin paneliga xush kelibsiz.`;
  const rows = [
    [{ text: "📊 Statistika", callback_data: "a:stats" }],
    [{ text: "🏆 Eng yaxshi natijalar", callback_data: "a:top" }],
    [
      { text: "👥 Foydalanuvchilar", callback_data: "a:users:page:1" },
      { text: "📝 Testlar", callback_data: "a:tests:page:1" },
    ],
    [{ text: "📨 Xabar yuborish", callback_data: "a:msg" }],
    [
      { text: "⚙️ Sozlamalar", callback_data: "a:settings" },
      { text: "🔄 Yangilash", callback_data: "a:menu" },
    ],
  ];
  return { text, keyboard: kb(rows) };
}

// =====================
// Admin: Stats
// =====================
async function renderStats() {
  const totalUsers = await usersCol.countDocuments({});
  const registeredUsers = await usersCol.countDocuments({ codeUsed: true });
  const bannedUsers = await usersCol.countDocuments({ isBanned: true });
  const notUsed = await usersCol.countDocuments({ codeUsed: false });
  const totalTests = await testsCol.countDocuments({});

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const activeToday = await usersCol.countDocuments({
    lastActiveAt: { $gte: today },
  });
  const testsToday = await resultsCol.countDocuments({
    createdAt: { $gte: today },
  });

  const last7 = new Date();
  last7.setDate(last7.getDate() - 7);
  const active7 = await usersCol.countDocuments({
    lastActiveAt: { $gte: last7 },
  });

  const mostTest = await testsCol
    .find({})
    .sort({ totalAttempts: -1 })
    .limit(1)
    .toArray();
  const topTestName = mostTest[0]?.title || "-";

  const last5Users = await usersCol
    .find({})
    .sort({ createdByAdminAt: -1 })
    .limit(5)
    .toArray();
  const last5 = last5Users.length
    ? last5Users
        .map(
          (u) =>
            `• ${esc(u.fullName)} (${u.codeUsed ? "Aktiv" : "Kod berildi"})`,
        )
        .join("\n")
    : "—";

  const text = [
    "📊 <b>Statistika</b>",
    "",
    `• Jami userlar: <b>${totalUsers}</b>`,
    `• Kod bilan roʻyxatdan oʻtganlar: <b>${registeredUsers}</b>`,
    `• Ban qilinganlar: <b>${bannedUsers}</b>`,
    `• Kod ishlatilmagan: <b>${notUsed}</b>`,
    `• Jami testlar: <b>${totalTests}</b>`,
    `• Bugungi aktiv userlar: <b>${activeToday}</b>`,
    `• Bugun test ishlaganlar: <b>${testsToday}</b>`,
    `• Oxirgi 7 kunlik aktivlik: <b>${active7}</b>`,
    `• Eng ko'p ishlangan test: <b>${esc(topTestName)}</b>`,
    "",
    "<b>Oxirgi 5 user</b>:",
    last5,
  ].join("\n");

  const rows = [
    [{ text: "⬅️ Orqaga", callback_data: "a:menu" }],
    [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
    [{ text: "🔄 Yangilash", callback_data: "a:stats" }],
  ];
  return { text, keyboard: kb(rows) };
}

async function renderAdminTop() {
  const topUsers = await usersCol
    .find({ codeUsed: true })
    .sort({ averageScore: -1 })
    .limit(10)
    .toArray();

  const topTests = await testsCol
    .find({})
    .sort({ averageScore: -1 })
    .limit(5)
    .toArray();

  const topResults = await resultsCol
    .find({})
    .sort({ percentage: -1, createdAt: -1 })
    .limit(10)
    .toArray();

  let text = "🏆 <b>Eng yaxshi natijalar</b>\n\n";
  text += "<b>Top 10 user</b>\n";
  if (!topUsers.length) {
    text += "—\n";
  } else {
    topUsers.forEach((u, i) => {
      text += `${i + 1}. ${esc(u.fullName)} — <b>${(u.averageScore || 0).toFixed(1)}%</b>\n`;
    });
  }

  text += "\n<b>Top testlar</b>\n";
  if (!topTests.length) {
    text += "—\n";
  } else {
    topTests.forEach((t, i) => {
      text += `${i + 1}. ${esc(t.title)} — <b>${(t.averageScore || 0).toFixed(1)}%</b>\n`;
    });
  }

  text += "\n<b>Top natijalar</b>\n";
  if (!topResults.length) {
    text += "—\n";
  } else {
    for (let i = 0; i < topResults.length; i++) {
      const r = topResults[i];
      const user = await usersCol.findOne({ _id: r.userId });
      const test = await testsCol.findOne({ _id: r.testId });
      text += `${i + 1}. ${esc(user?.fullName || "User")} — ${esc(test?.title || "Test")} — <b>${r.percentage.toFixed(1)}%</b>\n`;
    }
  }

  const rows = [
    [{ text: "⬅️ Orqaga", callback_data: "a:menu" }],
    [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
    [{ text: "🔄 Yangilash", callback_data: "a:top" }],
  ];
  return { text, keyboard: kb(rows) };
}

// =====================
// Admin: Users list
// =====================
async function renderUsersList(page = 1, query = "") {
  const filter = query ? { fullName: { $regex: query, $options: "i" } } : {};
  const total = await usersCol.countDocuments(filter);
  const pages = Math.max(1, Math.ceil(total / CONFIG.PAGE_SIZE_USERS));
  const p = Math.min(Math.max(page, 1), pages);

  const users = await usersCol
    .find(filter)
    .sort({ createdByAdminAt: -1 })
    .skip((p - 1) * CONFIG.PAGE_SIZE_USERS)
    .limit(CONFIG.PAGE_SIZE_USERS)
    .toArray();

  let text = "👥 <b>Foydalanuvchilar</b>\n";
  text += query ? `Qidiruv: <code>${esc(query)}</code>\n` : "";
  text += `Sahifa: <b>${p}/${pages}</b>\n`;

  if (!users.length) {
    text += "\nHozircha foydalanuvchilar yo'q.";
  }

  const rows = [];
  for (const u of users) {
    const status = u.isBanned ? "🚫" : u.codeUsed ? "✅" : "🕓";
    rows.push([
      {
        text: `${status} ${u.fullName}`,
        callback_data: `a:users:view:${u._id}`,
      },
    ]);
  }

  const navRow = [];
  const q = query ? encodeURIComponent(query) : "";
  if (p > 1)
    navRow.push({
      text: "⬅️",
      callback_data: `a:users:page:${p - 1}${q ? `:q:${q}` : ""}`,
    });
  if (p < pages)
    navRow.push({
      text: "➡️",
      callback_data: `a:users:page:${p + 1}${q ? `:q:${q}` : ""}`,
    });
  if (navRow.length) rows.push(navRow);

  rows.push([
    { text: "➕ Foydalanuvchi qo'shish", callback_data: "a:users:add" },
  ]);
  rows.push([{ text: "🔍 Qidirish", callback_data: "a:users:search" }]);
  rows.push([{ text: "⬅️ Orqaga", callback_data: "a:menu" }]);

  return { text, keyboard: kb(rows) };
}

async function renderUserDetails(userId) {
  const u = await usersCol.findOne({ _id: new ObjectId(userId) });
  if (!u) {
    return {
      text: "Foydalanuvchi topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }]]),
    };
  }
  const status = u.isBanned
    ? "Ban"
    : u.codeUsed
      ? "Aktiv"
      : "Kod ishlatilmagan";

  const text = [
    "👤 <b>Foydalanuvchi maʼlumoti</b>",
    "",
    `• Ismi: <b>${esc(u.fullName)}</b>`,
    `• Username: ${u.username ? `@${esc(u.username)}` : "-"}`,
    `• Telegram ID: ${u.telegramId || "-"}`,
    `• Kod: <code>${u.oneTimeCode || "-"}</code>`,
    `• Holati: <b>${status}</b>`,
    `• Join date: ${formatDate(u.joinedAt)}`,
    `• Qachon qo'shilgan: ${formatDate(u.createdByAdminAt)}`,
    `• Oxirgi faolligi: ${formatDate(u.lastActiveAt)}`,
    `• Nechta test ishlagan: <b>${u.totalTestsWorked || 0}</b>`,
    `• Oxirgi natijasi: ${u.lastResult || "-"}`,
    `• O'rtacha foizi: ${u.averageScore ? u.averageScore.toFixed(1) + "%" : "-"}`,
  ].join("\n");

  const banBtn = u.isBanned
    ? { text: "✅ Unban", callback_data: `a:users:unban:${u._id}` }
    : { text: "🚫 Ban", callback_data: `a:users:ban:${u._id}` };

  const rows = [
    [banBtn, { text: "🔁 Kod", callback_data: `a:users:code:${u._id}` }],
    [
      { text: "🗑 O'chirish", callback_data: `a:users:del:${u._id}` },
      { text: "📊 Natijalar", callback_data: `a:users:results:${u._id}` },
    ],
    [{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }],
    [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
  ];
  return { text, keyboard: kb(rows) };
}

// =====================
// Admin: Tests list
// =====================
async function renderTestsList(page = 1) {
  const total = await testsCol.countDocuments({});
  const pages = Math.max(1, Math.ceil(total / CONFIG.PAGE_SIZE_TESTS));
  const p = Math.min(Math.max(page, 1), pages);

  const tests = await testsCol
    .find({})
    .sort({ createdAt: -1 })
    .skip((p - 1) * CONFIG.PAGE_SIZE_TESTS)
    .limit(CONFIG.PAGE_SIZE_TESTS)
    .toArray();

  let text = "📝 <b>Testlar</b>\n";
  text += `Sahifa: <b>${p}/${pages}</b>\n`;

  if (!tests.length) {
    text += "\nHozircha testlar mavjud emas.";
  }

  const rows = [];
  for (const t of tests) {
    rows.push([
      { text: `📄 ${t.title}`, callback_data: `a:tests:view:${t._id}` },
    ]);
  }

  const navRow = [];
  if (p > 1)
    navRow.push({ text: "⬅️", callback_data: `a:tests:page:${p - 1}` });
  if (p < pages)
    navRow.push({ text: "➡️", callback_data: `a:tests:page:${p + 1}` });
  if (navRow.length) rows.push(navRow);

  rows.push([{ text: "➕ Test qo'shish", callback_data: "a:tests:add" }]);
  rows.push([{ text: "⬅️ Orqaga", callback_data: "a:menu" }]);
  rows.push([{ text: "🏠 Bosh menyu", callback_data: "a:menu" }]);

  return { text, keyboard: kb(rows) };
}

async function renderTestDetails(testId) {
  const t = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!t) {
    return {
      text: "Test topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
    };
  }

  const text = [
    "🧪 <b>Test maʼlumoti</b>",
    "",
    `• Test nomi: <b>${esc(t.title)}</b>`,
    `• Savollar soni: <b>${t.questionCount}</b>`,
    `• Izoh: ${t.description ? esc(t.description) : "-"}`,
    `• Yaratilgan sana: ${formatDate(t.createdAt)}`,
    `• Necha marta ishlangan: <b>${t.totalAttempts || 0}</b>`,
    `• O'rtacha natija: ${t.averageScore ? t.averageScore.toFixed(1) + "%" : "-"}`,
    `• Oxirgi ishlatilgan vaqt: ${formatDate(t.lastUsedAt)}`,
  ].join("\n");

  const rows = [
    [{ text: "🗑 Testni o'chirish", callback_data: `a:tests:del:${t._id}` }],
    [
      {
        text: "📄 Savollarni ko'rish",
        callback_data: `a:tests:questions:${t._id}`,
      },
    ],
    [
      {
        text: "Savollarni boshqarish",
        callback_data: `a:tests:q:manage:${t._id}:page:1`,
      },
    ],
    [{ text: "🏆 Leaderboard", callback_data: `a:tests:leader:${t._id}` }],
    [{ text: "✏️ Tahrirlash", callback_data: `a:tests:edit:${t._id}` }],
    [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
    [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
  ];

  return { text, keyboard: kb(rows) };
}

async function renderTestQuestions(testId, page = 1) {
  const t = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!t) {
    return {
      text: "Test topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
    };
  }

  const pageSize = 3;
  const pages = Math.max(1, Math.ceil(t.questions.length / pageSize));
  const p = Math.min(Math.max(page, 1), pages);
  const start = (p - 1) * pageSize;
  const slice = t.questions.slice(start, start + pageSize);

  let text = `📄 <b>${esc(t.title)}</b> savollari\nSahifa: <b>${p}/${pages}</b>\n\n`;
  slice.forEach((q, idx) => {
    text += `${start + idx + 1}) ${esc(q.question)}\n`;
    text += `A) ${esc(q.options.A)}\nB) ${esc(q.options.B)}\nC) ${esc(q.options.C)}\nD) ${esc(q.options.D)}\n`;
    text += `To'g'ri: <b>${q.correctAnswer}</b>\n\n`;
  });

  const rows = [];
  const navRow = [];
  if (p > 1)
    navRow.push({
      text: "⬅️",
      callback_data: `a:tests:questions:${t._id}:page:${p - 1}`,
    });
  if (p < pages)
    navRow.push({
      text: "➡️",
      callback_data: `a:tests:questions:${t._id}:page:${p + 1}`,
    });
  if (navRow.length) rows.push(navRow);
  rows.push([{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${t._id}` }]);
  rows.push([{ text: "🏠 Bosh menyu", callback_data: "a:menu" }]);

  return { text, keyboard: kb(rows) };
}

async function renderTestQuestionsManage(testId, page = 1) {
  const t = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!t) {
    return {
      text: "Test topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
    };
  }

  const pageSize = 3;
  const pages = Math.max(1, Math.ceil(t.questions.length / pageSize));
  const p = Math.min(Math.max(page, 1), pages);
  const start = (p - 1) * pageSize;
  const slice = t.questions.slice(start, start + pageSize);

  let text = `Savollarni boshqarish: <b>${esc(t.title)}</b>\nSahifa: <b>${p}/${pages}</b>\n\n`;
  slice.forEach((q, idx) => {
    const n = start + idx + 1;
    text += `${n}) ${esc(q.question)}\n`;
    text += `A) ${esc(q.options.A)}\nB) ${esc(q.options.B)}\nC) ${esc(q.options.C)}\nD) ${esc(q.options.D)}\n`;
    text += `To'g'ri: <b>${q.correctAnswer}</b>\n`;
    text += `Rasm: <b>${q.imageFileId ? "bor" : "yo'q"}</b>\n\n`;
  });

  const rows = [];
  for (let i = 0; i < slice.length; i++) {
    const n = start + i + 1;
    rows.push([
      {
        text: `Edit ${n}`,
        callback_data: `a:tests:q:edit:${t._id}:${start + i}`,
      },
      {
        text: `Delete ${n}`,
        callback_data: `a:tests:q:del:${t._id}:${start + i}`,
      },
    ]);
    rows.push([
      {
        text: `Answer ${n}`,
        callback_data: `a:tests:q:answer:${t._id}:${start + i}`,
      },
      {
        text: `Image ${n}`,
        callback_data: `a:tests:q:image:${t._id}:${start + i}`,
      },
    ]);
  }

  const navRow = [];
  if (p > 1)
    navRow.push({
      text: "⬅️",
      callback_data: `a:tests:q:manage:${t._id}:page:${p - 1}`,
    });
  if (p < pages)
    navRow.push({
      text: "➡️",
      callback_data: `a:tests:q:manage:${t._id}:page:${p + 1}`,
    });
  if (navRow.length) rows.push(navRow);

  rows.push([
    { text: "Add question", callback_data: `a:tests:q:add:${t._id}` },
  ]);
  rows.push([{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${t._id}` }]);
  rows.push([{ text: "🏠 Bosh menyu", callback_data: "a:menu" }]);

  return { text, keyboard: kb(rows) };
}

async function renderTestLeaderboard(testId) {
  const t = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!t) {
    return {
      text: "Test topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
    };
  }

  const results = await resultsCol
    .find({ testId: t._id })
    .sort({ percentage: -1, createdAt: -1 })
    .limit(10)
    .toArray();

  let text = `🏆 <b>${esc(t.title)}</b> — Leaderboard\n\n`;
  if (!results.length) {
    text += "Hali natija yo'q.";
  } else {
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      const u = await usersCol.findOne({ _id: r.userId });
      text += `${i + 1}. ${esc(u?.fullName || "User")} — <b>${r.percentage.toFixed(1)}%</b> (${r.correctCount}/${r.totalCount})\n`;
    }
  }

  const rows = [
    [{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${t._id}` }],
    [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
  ];
  return { text, keyboard: kb(rows) };
}

// =====================
// User views
// =====================
async function renderUserMenu(userId) {
  const settings = await getSettings();
  const tests = await testsCol.find({}).sort({ createdAt: -1 }).toArray();
  const u = await usersCol.findOne({ telegramId: Number(userId) });

  const name = u?.fullName || "Foydalanuvchi";
  const text = settings.welcomeText.replace("{name}", esc(name));

  const rows = [];
  if (!settings.testEnabled) {
    rows.push([
      { text: "⛔ Testlar vaqtincha o'chirilgan", callback_data: "u:noop" },
    ]);
  } else if (!tests.length) {
    rows.push([{ text: "Hozircha testlar yo'q", callback_data: "u:noop" }]);
  } else {
    for (const t of tests.slice(0, 10)) {
      rows.push([
        { text: `📝 ${t.title}`, callback_data: `u:test:view:${t._id}` },
      ]);
    }
  }
  rows.push([{ text: "📊 Mening natijalarim", callback_data: "u:results" }]);
  rows.push([{ text: "ℹ️ Yordam", callback_data: "u:help" }]);

  return { text, keyboard: kb(rows) };
}

async function renderUserTestInfo(testId) {
  const t = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!t) {
    return {
      text: "Test topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "u:menu" }]]),
    };
  }
  const text = [
    "📝 <b>Test maʼlumoti</b>",
    "",
    `• Test nomi: <b>${esc(t.title)}</b>`,
    `• Savollar soni: <b>${t.questionCount}</b>`,
    `• Izoh: ${t.description ? esc(t.description) : "-"}`,
  ].join("\n");
  const rows = [
    [{ text: "▶️ Boshlash", callback_data: `u:test:start:${t._id}` }],
    [{ text: "⬅️ Orqaga", callback_data: "u:menu" }],
    [{ text: "🏠 Bosh menyu", callback_data: "u:menu" }],
  ];
  return { text, keyboard: kb(rows) };
}

function buildQuestionView(test, index, sessionData, settings) {
  const q = test.questions[index];
  const total = test.questionCount;
  const elapsed = elapsedSeconds(sessionData);
  const limitInfo = settings?.testTimeLimitEnabled
    ? ` / ${humanDuration(CONFIG.TEST_TIME_LIMIT_SEC)}`
    : "";

  const MAX_TELEGRAM_MSG_LEN = 4096;

  const header = `🧩 <b>${esc(test.title)}</b>\n`;
  const progress = `Savol: <b>${index + 1}/${total}</b> | Vaqt: <b>${humanDuration(elapsed)}${limitInfo}</b>\n\n`;
  const questionText = `${index + 1}) ${esc(q.question)}\n\n`;
  const optionsText = `A) ${esc(q.options.A)}\nB) ${esc(q.options.B)}\nC) ${esc(q.options.C)}\nD) ${esc(q.options.D)}\n`;

  let fullText = header + progress + questionText + optionsText;

  const answered = sessionData.answers?.[index];
  let answerResultText = "";
  if (answered) {
    const correctText = q.options[answered.correctAnswer];
    const selectedText = q.options[answered.selected];
    answerResultText = `<b>\nTog'ri javob: <b>${answered.correctAnswer}</b>${correctText ? ` — ${esc(correctText)}</b>` : ""}\n`;
    answerResultText += `Sizning javobingiz: <b>${answered.selected}</b>${selectedText ? ` — ${esc(selectedText)}` : ""}\n`;
    fullText += answerResultText;
  }

  let mainText = fullText;
  let overflowText = null;

  if (fullText.length > MAX_TELEGRAM_MSG_LEN) {
    mainText = header + progress + questionText;
    let remaining = MAX_TELEGRAM_MSG_LEN - mainText.length;
    if (remaining > 100) {
      const optionsPart = optionsText.substring(0, remaining - 50);
      mainText += optionsPart;
      overflowText =
        optionsText.substring(optionsPart.length) + answerResultText;
    } else {
      overflowText = optionsText + answerResultText;
    }
  }

  const buttons = ["A", "B", "C", "D"].map((opt) => {
    let label = opt;
    if (answered) {
      if (answered.selected === opt && answered.correct) label = `✅ ${opt}`;
      if (answered.selected === opt && !answered.correct) label = `❌ ${opt}`;
      if (answered.correctAnswer === opt) label = `✅ ${opt}`;
    }
    return {
      text: label,
      callback_data: `u:test:ans:${test._id}:${index}:${opt}`,
    };
  });

  const rows = [[buttons[0], buttons[1], buttons[2], buttons[3]]];
  if (answered && index < total - 1) {
    rows.push([
      { text: "➡️ Keyingi", callback_data: `u:test:next:${test._id}:${index}` },
    ]);
  }
  rows.push([
    { text: "❌ Testni yakunlash", callback_data: `u:test:finish:${test._id}` },
  ]);

  return {
    text: mainText,
    keyboard: kb(rows),
    imageFileId: q.imageFileId,
    overflowText,
  };
}

async function sendQuestionWithImage(
  chatId,
  messageId,
  test,
  index,
  sessionData,
  settings,
  forceNewMessage = false,
) {
  const q = test.questions[index];
  const view = buildQuestionView(test, index, sessionData, settings);

  if (forceNewMessage && messageId) {
    await clearInlineKeyboard(chatId, messageId);
  }

  const shouldSendNew = forceNewMessage || !messageId;

  if (q.imageFileId && shouldSendNew) {
    try {
      await bot.sendPhoto(chatId, q.imageFileId, {
        caption: `🧩 <b>${esc(test.title)}</b>\nSavol: <b>${index + 1}/${test.questionCount}</b>`,
        parse_mode: "HTML",
        protect_content: true,
      });
      const answerText = `${index + 1}) ${esc(q.question)}\n\nA) ${esc(q.options.A)}\nB) ${esc(q.options.B)}\nC) ${esc(q.options.C)}\nD) ${esc(q.options.D)}\n`;
      await bot.sendMessage(chatId, answerText, {
        parse_mode: "HTML",
        protect_content: true,
        ...view.keyboard,
      });
    } catch (err) {
      await sendOrEdit(
        chatId,
        shouldSendNew ? null : messageId,
        view.text,
        view.keyboard,
      );
      if (view.overflowText) {
        await bot.sendMessage(chatId, view.overflowText, {
          parse_mode: "HTML",
          protect_content: true,
        });
      }
    }
  } else {
    await sendOrEdit(
      chatId,
      shouldSendNew ? null : messageId,
      view.text,
      view.keyboard,
    );
    if (view.overflowText) {
      await bot.sendMessage(chatId, view.overflowText, {
        parse_mode: "HTML",
        protect_content: true,
      });
    }
  }
}

async function renderUserResults(userId) {
  const u = await usersCol.findOne({ telegramId: Number(userId) });
  if (!u) {
    return {
      text: "Profil topilmadi.",
      keyboard: kb([[{ text: "⬅️ Orqaga", callback_data: "u:menu" }]]),
    };
  }
  const results = await resultsCol
    .find({ userId: u._id })
    .sort({ createdAt: -1 })
    .limit(10)
    .toArray();
  let text = "📊 <b>Mening natijalarim</b>\n\n";
  if (!results.length) {
    text += "Hali natijalar yo'q.";
  } else {
    for (const r of results) {
      const t = await testsCol.findOne({ _id: r.testId });
      text += `• ${esc(t?.title || "Test")} — <b>${r.percentage.toFixed(1)}%</b> (${r.correctCount}/${r.totalCount}) | ${formatDate(r.createdAt)}\n`;
    }
  }
  const rows = [
    [{ text: "⬅️ Orqaga", callback_data: "u:menu" }],
    [{ text: "🏠 Bosh menyu", callback_data: "u:menu" }],
  ];
  return { text, keyboard: kb(rows) };
}

async function finalizeTest(
  userId,
  testId,
  sessionData,
  chatId,
  msgId,
  reason,
) {
  const test = await testsCol.findOne({ _id: new ObjectId(testId) });
  if (!test) return;

  const answers = sessionData.answers || [];
  const correctCount = answers.filter((a) => a?.correct).length;
  const totalCount = test.questionCount;
  const percentage = (correctCount / totalCount) * 100;
  const durationSeconds = elapsedSeconds(sessionData);

  const userDoc = await usersCol.findOne({ telegramId: Number(userId) });
  if (!userDoc) return;

  const result = {
    userId: userDoc._id,
    testId: test._id,
    correctCount,
    totalCount,
    percentage,
    durationSeconds,
    createdAt: now(),
  };
  await resultsCol.insertOne(result);

  const newTotal = (userDoc.totalTestsWorked || 0) + 1;
  const newAvg =
    ((userDoc.averageScore || 0) * (newTotal - 1) + percentage) / newTotal;
  await usersCol.updateOne(
    { _id: userDoc._id },
    {
      $set: {
        totalTestsWorked: newTotal,
        averageScore: newAvg,
        lastResult: `${percentage.toFixed(1)}% (${correctCount}/${totalCount})`,
        lastActiveAt: now(),
      },
    },
  );

  const newTestTotal = (test.totalAttempts || 0) + 1;
  const newTestAvg =
    ((test.averageScore || 0) * (newTestTotal - 1) + percentage) / newTestTotal;
  await testsCol.updateOne(
    { _id: test._id },
    {
      $set: {
        totalAttempts: newTestTotal,
        averageScore: newTestAvg,
        lastUsedAt: now(),
      },
    },
  );

  await clearSession(userId);

  const header = reason === "time" ? "⏱️ Vaqt tugadi" : "🎉 Natija";
  const text = [
    `<b>${header}</b>`,
    "",
    `Ism: <b>${esc(userDoc.fullName)}</b>`,
    `Test: <b>${esc(test.title)}</b>`,
    `To'g'ri javoblar: <b>${correctCount}/${totalCount}</b>`,
    `Foiz: <b>${percentage.toFixed(1)}%</b>`,
    `Aniqlilik darajasi: <b>${scoreLabel(percentage)}</b>`,
    `Ketgan vaqt: <b>${humanDuration(durationSeconds)}</b>`,
    `Ishlangan sana: ${formatDate(result.createdAt)}`,
    `Natija holati: <b>${scoreLabel(percentage)}</b>`,
  ].join("\n");

  const rows = [
    [{ text: "🏠 Bosh menyu", callback_data: "u:menu" }],
    [{ text: "📊 Mening natijalarim", callback_data: "u:results" }],
  ];
  await clearInlineKeyboard(chatId, msgId);
  await sendOrEdit(chatId, null, text, kb(rows));
}

// =====================
// Core Actions
// =====================
async function showAdminMenu(chatId, messageId, name) {
  const view = adminMenu(name || "Admin");
  await sendOrEdit(chatId, messageId, view.text, view.keyboard);
}

async function showUserMenu(chatId, messageId, userId) {
  const view = await renderUserMenu(userId);
  await sendOrEdit(chatId, messageId, view.text, view.keyboard);
}

async function guardBan(msgOrQuery) {
  if (!usersCol) return false;
  const userId = msgOrQuery.from?.id || msgOrQuery.message?.chat?.id;
  const u = await usersCol.findOne({ telegramId: Number(userId) });
  if (u?.isBanned) {
    const text = "Siz ushbu botdan foydalanish huquqidan mahrum qilingansiz.";
    if (msgOrQuery.message) {
      await sendOrEdit(
        msgOrQuery.message.chat.id,
        msgOrQuery.message.message_id,
        text,
        kb([]),
      );
    } else {
      await bot.sendMessage(userId, text, { protect_content: true });
    }
    return true;
  }
  return false;
}

// =====================
// Message Handler
// =====================
bot.onText(/\/start/, async (msg) => {
  if (!dbReady) {
    await bot.sendMessage(msg.chat.id, "Bot initializing, please wait...", {
      protect_content: true,
    });
    return;
  }

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const admin = isAdmin(userId);

  if (!admin && (await guardBan({ from: msg.from }))) return;

  await setSession(userId, { lastMessageId: msg.message_id });
  if (admin) {
    await showAdminMenu(chatId, msg.message_id, msg.from.first_name || "Admin");
    return;
  }

  if (!usersCol) {
    await bot.sendMessage(chatId, "Bot qo'shilmoqda, biroz kuting...", {
      protect_content: true,
    });
    return;
  }

  const user = await usersCol.findOne({ telegramId: Number(userId) });
  if (user) {
    await updateLastActive(userId);
    await showUserMenu(chatId, msg.message_id, userId);
    return;
  }

  const settings = await getSettings();
  if (!settings.registrationEnabled) {
    await sendOrEdit(
      chatId,
      msg.message_id,
      "Ro'yxatdan o'tish vaqtincha o'chirilgan.",
      kb([]),
    );
    return;
  }

  await setSession(userId, { state: "USER_WAIT_CODE", data: {} });
  await sendOrEdit(
    chatId,
    msg.message_id,
    "Iltimos, o'quvchi kodini kiriting:",
    kb([]),
  );
});

bot.on("message", async (msg) => {
  if (!dbReady) {
    await bot.sendMessage(msg.chat.id, "Bot initializing...", {
      protect_content: true,
    });
    return;
  }

  const hasText = typeof msg.text === "string" && msg.text.length > 0;
  const hasCaption = typeof msg.caption === "string" && msg.caption.length > 0;
  const inputText = hasText ? msg.text : hasCaption ? msg.caption : "";
  const hasPhoto = Array.isArray(msg.photo) && msg.photo.length > 0;
  const hasImageDoc = !!(
    msg.document && (msg.document.mime_type || "").startsWith("image/")
  );

  if (hasText && msg.text.startsWith("/")) return;

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const admin = isAdmin(userId);

  await trackUserMessage(userId, msg.message_id);

  if (!admin && !inputText) return;

  if (!admin && (await guardBan({ from: msg.from }))) return;

  const session = await getSession(userId);

  // Admin flows
  if (admin) {
    switch (session.state) {
      case "ADMIN_ADD_USER_NAME": {
        const fullName = inputText.trim();
        if (fullName.length < 2) {
          await bot.sendMessage(
            chatId,
            "Ism-familya juda qisqa. Qayta kiriting.",
            { protect_content: true },
          );
          return;
        }
        const settings = await getSettings();
        const code = await uniqueCode(
          settings.codeLength || CONFIG.DEFAULT_CODE_LENGTH,
        );
        const userDoc = {
          fullName,
          telegramId: null,
          username: null,
          oneTimeCode: code,
          codeUsed: false,
          isBanned: false,
          createdByAdminAt: now(),
          joinedAt: null,
          lastActiveAt: null,
          totalTestsWorked: 0,
          averageScore: 0,
          results: [],
        };
        await usersCol.insertOne(userDoc);
        await logAdminAction(userId, "USER_ADD", { fullName });

        const text = [
          "✅ <b>Foydalanuvchi qo'shildi</b>",
          "",
          `Ism: <b>${esc(fullName)}</b>`,
          `Bir martalik kod: <code>${code}</code>`,
          "Holati: <b>Kod berildi, hali ishlatilmagan</b>",
        ].join("\n");

        const rows = [
          [{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }],
          [{ text: "👥 Foydalanuvchilar", callback_data: "a:users:page:1" }],
          [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
        ];
        await bot.sendMessage(chatId, text, {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        await clearSession(userId);
        return;
      }

      case "ADMIN_USERS_SEARCH": {
        const query = inputText.trim();
        await setSession(userId, { state: null, data: {} });
        const view = await renderUsersList(1, query);
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_ADD_TEST_TITLE": {
        const title = inputText.trim();
        if (title.length < 2) {
          await bot.sendMessage(
            chatId,
            "Test nomi juda qisqa. Qayta kiriting.",
            { protect_content: true },
          );
          return;
        }
        await setSession(userId, {
          state: "ADMIN_ADD_TEST_COUNT",
          data: { title, questions: [] },
        });
        const rows = [
          [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
          [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
        ];
        await bot.sendMessage(chatId, "Testlar sonini kiriting:", {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        return;
      }

      case "ADMIN_ADD_TEST_COUNT": {
        const count = Number(inputText.trim());
        if (!Number.isInteger(count) || count < 1 || count > 200) {
          await bot.sendMessage(
            chatId,
            "Testlar soni noto'g'ri. 1-200 oralig'ida kiriting.",
            { protect_content: true },
          );
          return;
        }
        const data = { ...(session.data || {}), count, currentIndex: 0 };
        await setSession(userId, { state: "ADMIN_ADD_TEST_Q", data });
        const rows = [
          [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
          [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
        ];
        await bot.sendMessage(
          chatId,
          "1-savolni quyidagi formatda kiriting:\n\nHello ___ name is Akmal\n\nA) My\nB) I\nC) He\nD) Have\n\nAgar rasmli savol bo'lsa, rasmni yuboring va captionga savol/variantlarni yozing.",
          { parse_mode: "HTML", protect_content: true, ...kb(rows) },
        );
        return;
      }

      case "ADMIN_ADD_TEST_Q": {
        const parsed = parseQuestionBlock(inputText);
        if (!parsed) {
          await bot.sendMessage(chatId, "Format noto'g'ri. Qayta kiriting.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        data.currentQuestion = parsed;

        const fileId = hasPhoto
          ? msg.photo[msg.photo.length - 1].file_id
          : hasImageDoc
            ? msg.document.file_id
            : null;

        if (fileId) {
          data.currentQuestion.imageFileId = fileId;
          await setSession(userId, { state: "ADMIN_ADD_TEST_A", data });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
            [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
          ];
          await bot.sendMessage(chatId, "Qaysi javob to'g'ri? (A/B/C/D)", {
            parse_mode: "HTML",
            protect_content: true,
            ...kb(rows),
          });
          return;
        }

        await setSession(userId, { state: "ADMIN_ADD_TEST_Q_IMAGE", data });
        const rows = [
          [{ text: "✅ Rasm yo'q", callback_data: "a:tests:q:skip-image" }],
          [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
          [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
        ];
        await bot.sendMessage(
          chatId,
          'Rasm yuklaysizmi? (Savolga mos rasm yoqsa "✅ Rasm yo\'q" bosing)',
          { parse_mode: "HTML", protect_content: true, ...kb(rows) },
        );
        return;
      }

      case "ADMIN_ADD_TEST_Q_IMAGE": {
        if (hasPhoto || hasImageDoc) {
          const fileId = hasPhoto
            ? msg.photo[msg.photo.length - 1].file_id
            : msg.document.file_id;
          const data = { ...(session.data || {}) };
          data.currentQuestion.imageFileId = fileId;
          await setSession(userId, { state: "ADMIN_ADD_TEST_A", data });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
            [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
          ];
          await bot.sendMessage(chatId, "Qaysi javob to'g'ri? (A/B/C/D)", {
            parse_mode: "HTML",
            protect_content: true,
            ...kb(rows),
          });
          return;
        }
        await bot.sendMessage(chatId, "Rasm yo'qlikda faqat rasmni yuboring.", {
          protect_content: true,
        });
        return;
      }

      case "ADMIN_ADD_TEST_A": {
        const ans = inputText.trim().toUpperCase();
        if (!["A", "B", "C", "D"].includes(ans)) {
          await bot.sendMessage(chatId, "Javob A/B/C/D bo'lishi kerak.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        const q = data.currentQuestion;
        q.correctAnswer = ans;
        data.questions = data.questions || [];
        data.questions.push(q);
        data.currentIndex = (data.currentIndex || 0) + 1;
        data.currentQuestion = null;

        if (data.currentIndex < data.count) {
          await setSession(userId, { state: "ADMIN_ADD_TEST_Q", data });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
            [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
          ];
          await bot.sendMessage(
            chatId,
            `${data.currentIndex + 1}-savolni kiriting:`,
            { parse_mode: "HTML", protect_content: true, ...kb(rows) },
          );
          return;
        }

        await setSession(userId, { state: "ADMIN_ADD_TEST_DESC", data });
        const rows = [
          [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
          [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
        ];
        await bot.sendMessage(chatId, "Test haqida izoh kiriting:", {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        return;
      }

      case "ADMIN_ADD_TEST_DESC": {
        const desc = inputText.trim();
        const data = { ...(session.data || {}) };
        data.description = desc;

        const summary = [
          "✅ <b>Test summary</b>",
          "",
          `Test nomi: <b>${esc(data.title)}</b>`,
          `Test soni: <b>${data.count}</b>`,
          "Variantlar soni: <b>4</b>",
          `Izoh: ${esc(desc)}`,
        ].join("\n");

        const rows = [
          [{ text: "💾 Saqlash", callback_data: "a:tests:save" }],
          [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
          [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
        ];

        await setSession(userId, { state: "ADMIN_ADD_TEST_CONFIRM", data });
        await bot.sendMessage(chatId, summary, {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        return;
      }

      case "ADMIN_EDIT_Q_BLOCK": {
        const parsed = parseQuestionBlock(inputText);
        if (!parsed) {
          await bot.sendMessage(chatId, "Format noto'g'ri. Qayta kiriting.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        const t = await testsCol.findOne({ _id: new ObjectId(data.testId) });
        if (!t || !t.questions?.[data.qIndex]) {
          await bot.sendMessage(chatId, "Savol topilmadi.", {
            protect_content: true,
          });
          await clearSession(userId);
          return;
        }
        const old = t.questions[data.qIndex];
        t.questions[data.qIndex] = {
          ...old,
          question: parsed.question,
          options: parsed.options,
        };
        await testsCol.updateOne(
          { _id: t._id },
          { $set: { questions: t.questions } },
        );
        await logAdminAction(userId, "TEST_Q_EDIT", {
          testId: data.testId,
          qIndex: data.qIndex,
        });
        await clearSession(userId);
        const view = await renderTestQuestionsManage(
          data.testId,
          Math.floor(data.qIndex / 3) + 1,
        );
        await bot.sendMessage(chatId, "Savol yangilandi.\n\n" + view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_EDIT_Q_ANSWER": {
        const ans = inputText.trim().toUpperCase();
        if (!["A", "B", "C", "D"].includes(ans)) {
          await bot.sendMessage(chatId, "Javob A/B/C/D bo'lishi kerak.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        const t = await testsCol.findOne({ _id: new ObjectId(data.testId) });
        if (!t || !t.questions?.[data.qIndex]) {
          await bot.sendMessage(chatId, "Savol topilmadi.", {
            protect_content: true,
          });
          await clearSession(userId);
          return;
        }
        t.questions[data.qIndex].correctAnswer = ans;
        await testsCol.updateOne(
          { _id: t._id },
          { $set: { questions: t.questions } },
        );
        await logAdminAction(userId, "TEST_Q_ANSWER_EDIT", {
          testId: data.testId,
          qIndex: data.qIndex,
        });
        await clearSession(userId);
        const view = await renderTestQuestionsManage(
          data.testId,
          Math.floor(data.qIndex / 3) + 1,
        );
        await bot.sendMessage(
          chatId,
          "To'g'ri javob yangilandi.\n\n" + view.text,
          { parse_mode: "HTML", protect_content: true, ...view.keyboard },
        );
        return;
      }

      case "ADMIN_EDIT_Q_IMAGE": {
        const data = { ...(session.data || {}) };
        const t = await testsCol.findOne({ _id: new ObjectId(data.testId) });
        if (!t || !t.questions?.[data.qIndex]) {
          await bot.sendMessage(chatId, "Savol topilmadi.", {
            protect_content: true,
          });
          await clearSession(userId);
          return;
        }
        if (hasPhoto || hasImageDoc) {
          const fileId = hasPhoto
            ? msg.photo[msg.photo.length - 1].file_id
            : msg.document.file_id;
          t.questions[data.qIndex].imageFileId = fileId;
          await testsCol.updateOne(
            { _id: t._id },
            { $set: { questions: t.questions } },
          );
          await logAdminAction(userId, "TEST_Q_IMAGE_EDIT", {
            testId: data.testId,
            qIndex: data.qIndex,
          });
          await clearSession(userId);
          const view = await renderTestQuestionsManage(
            data.testId,
            Math.floor(data.qIndex / 3) + 1,
          );
          await bot.sendMessage(chatId, "Rasm yangilandi.\n\n" + view.text, {
            parse_mode: "HTML",
            protect_content: true,
            ...view.keyboard,
          });
          return;
        }
        const cmd = inputText.trim().toUpperCase();
        if (cmd === "REMOVE" || cmd === "DELETE" || cmd === "0") {
          t.questions[data.qIndex].imageFileId = null;
          await testsCol.updateOne(
            { _id: t._id },
            { $set: { questions: t.questions } },
          );
          await logAdminAction(userId, "TEST_Q_IMAGE_REMOVE", {
            testId: data.testId,
            qIndex: data.qIndex,
          });
          await clearSession(userId);
          const view = await renderTestQuestionsManage(
            data.testId,
            Math.floor(data.qIndex / 3) + 1,
          );
          await bot.sendMessage(chatId, "Rasm o'chirildi.\n\n" + view.text, {
            parse_mode: "HTML",
            protect_content: true,
            ...view.keyboard,
          });
          return;
        }
        await bot.sendMessage(chatId, 'Rasm yuboring yoki "REMOVE" yozing.', {
          protect_content: true,
        });
        return;
      }

      case "ADMIN_ADD_EXISTING_Q_BLOCK": {
        const parsed = parseQuestionBlock(inputText);
        if (!parsed) {
          await bot.sendMessage(chatId, "Format noto'g'ri. Qayta kiriting.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        data.currentQuestion = parsed;

        const fileId = hasPhoto
          ? msg.photo[msg.photo.length - 1].file_id
          : hasImageDoc
            ? msg.document.file_id
            : null;

        if (fileId) {
          data.currentQuestion.imageFileId = fileId;
          await setSession(userId, {
            state: "ADMIN_ADD_EXISTING_Q_ANSWER",
            data,
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${data.testId}:page:1`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await bot.sendMessage(chatId, "Qaysi javob to'g'ri? (A/B/C/D)", {
            parse_mode: "HTML",
            protect_content: true,
            ...kb(rows),
          });
          return;
        }

        await setSession(userId, { state: "ADMIN_ADD_EXISTING_Q_IMAGE", data });
        const rows = [
          [
            {
              text: "✅ Rasm yo'q",
              callback_data: `a:tests:q:add:skip-image:${data.testId}`,
            },
          ],
          [
            {
              text: "⬅️ Orqaga",
              callback_data: `a:tests:q:manage:${data.testId}:page:1`,
            },
          ],
          [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
        ];
        await bot.sendMessage(
          chatId,
          "Rasm yuklaysizmi? (Rasm bo'lmasa \"✅ Rasm yo'q\" bosing)",
          { parse_mode: "HTML", protect_content: true, ...kb(rows) },
        );
        return;
      }

      case "ADMIN_ADD_EXISTING_Q_IMAGE": {
        const data = { ...(session.data || {}) };
        if (hasPhoto || hasImageDoc) {
          const fileId = hasPhoto
            ? msg.photo[msg.photo.length - 1].file_id
            : msg.document.file_id;
          data.currentQuestion.imageFileId = fileId;
          await setSession(userId, {
            state: "ADMIN_ADD_EXISTING_Q_ANSWER",
            data,
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${data.testId}:page:1`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await bot.sendMessage(chatId, "Qaysi javob to'g'ri? (A/B/C/D)", {
            parse_mode: "HTML",
            protect_content: true,
            ...kb(rows),
          });
          return;
        }
        const cmd = inputText.trim().toUpperCase();
        if (cmd === "SKIP" || cmd === "0") {
          await setSession(userId, {
            state: "ADMIN_ADD_EXISTING_Q_ANSWER",
            data,
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${data.testId}:page:1`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await bot.sendMessage(chatId, "Qaysi javob to'g'ri? (A/B/C/D)", {
            parse_mode: "HTML",
            protect_content: true,
            ...kb(rows),
          });
          return;
        }
        await bot.sendMessage(chatId, 'Rasm yuboring yoki "SKIP" yozing.', {
          protect_content: true,
        });
        return;
      }

      case "ADMIN_ADD_EXISTING_Q_ANSWER": {
        const ans = inputText.trim().toUpperCase();
        if (!["A", "B", "C", "D"].includes(ans)) {
          await bot.sendMessage(chatId, "Javob A/B/C/D bo'lishi kerak.", {
            protect_content: true,
          });
          return;
        }
        const data = { ...(session.data || {}) };
        const t = await testsCol.findOne({ _id: new ObjectId(data.testId) });
        if (!t) {
          await bot.sendMessage(chatId, "Test topilmadi.", {
            protect_content: true,
          });
          await clearSession(userId);
          return;
        }
        const q = data.currentQuestion || {};
        q.correctAnswer = ans;
        const questions = t.questions || [];
        questions.push(q);
        await testsCol.updateOne(
          { _id: t._id },
          { $set: { questions, questionCount: questions.length } },
        );
        await logAdminAction(userId, "TEST_Q_ADD", { testId: data.testId });
        await clearSession(userId);
        const view = await renderTestQuestionsManage(
          data.testId,
          Math.ceil(questions.length / 3),
        );
        await bot.sendMessage(chatId, "Savol qo'shildi.\n\n" + view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_MSG_SINGLE_TEXT": {
        const data = { ...(session.data || {}) };
        data.messageText = inputText.trim();
        await setSession(userId, { state: "ADMIN_MSG_SINGLE_CONFIRM", data });
        const rows = [
          [{ text: "✅ Ha", callback_data: "a:msg:send:single" }],
          [{ text: "❌ Yo'q", callback_data: "a:msg:cancel" }],
        ];
        await bot.sendMessage(chatId, "Xabar yuborilsinmi?", {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        return;
      }

      case "ADMIN_MSG_ALL_TEXT": {
        const data = { ...(session.data || {}) };
        data.messageText = inputText.trim();
        await setSession(userId, { state: "ADMIN_MSG_ALL_CONFIRM", data });
        const rows = [
          [{ text: "✅ Ha", callback_data: "a:msg:send:all" }],
          [{ text: "❌ Yo'q", callback_data: "a:msg:cancel" }],
        ];
        await bot.sendMessage(chatId, "Barchaga yuborilsinmi?", {
          parse_mode: "HTML",
          protect_content: true,
          ...kb(rows),
        });
        return;
      }

      case "ADMIN_SETTINGS_BOTNAME": {
        const name = inputText.trim();
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { botName: name } },
        );
        await logAdminAction(userId, "SETTINGS_BOTNAME", { name });
        await clearSession(userId);
        const view = await renderSettings();
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_SETTINGS_WELCOME": {
        const welcomeText = inputText.trim();
        await settingsCol.updateOne({ _id: "main" }, { $set: { welcomeText } });
        await logAdminAction(userId, "SETTINGS_WELCOME", { text: welcomeText });
        await clearSession(userId);
        const view = await renderSettings();
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_SETTINGS_CODELEN": {
        const len = Number(inputText.trim());
        if (!Number.isInteger(len) || len < 4 || len > 12) {
          await bot.sendMessage(
            chatId,
            "Kod uzunligi 4-12 oralig'ida bo'lishi kerak.",
            { protect_content: true },
          );
          return;
        }
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { codeLength: len } },
        );
        await logAdminAction(userId, "SETTINGS_CODELEN", { len });
        await clearSession(userId);
        const view = await renderSettings();
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_SETTINGS_LOGCHAN": {
        const id = inputText.trim();
        const num = id === "0" ? null : Number(id);
        if (id !== "0" && !Number.isInteger(num)) {
          await bot.sendMessage(
            chatId,
            "ID noto'g'ri. Masalan: -1001234567890 yoki 0",
            { protect_content: true },
          );
          return;
        }
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { adminLogChannelId: num } },
        );
        await logAdminAction(userId, "SETTINGS_LOGCHAN", { num });
        await clearSession(userId);
        const view = await renderSettings();
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_TEST_EDIT_TITLE": {
        const title = inputText.trim();
        const data = { ...(session.data || {}) };
        await testsCol.updateOne(
          { _id: new ObjectId(data.testId) },
          { $set: { title } },
        );
        await logAdminAction(userId, "TEST_EDIT_TITLE", {
          testId: data.testId,
        });
        await clearSession(userId);
        const view = await renderTestDetails(data.testId);
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      case "ADMIN_TEST_EDIT_DESC": {
        const desc = inputText.trim();
        const data = { ...(session.data || {}) };
        await testsCol.updateOne(
          { _id: new ObjectId(data.testId) },
          { $set: { description: desc } },
        );
        await logAdminAction(userId, "TEST_EDIT_DESC", { testId: data.testId });
        await clearSession(userId);
        const view = await renderTestDetails(data.testId);
        await bot.sendMessage(chatId, view.text, {
          parse_mode: "HTML",
          protect_content: true,
          ...view.keyboard,
        });
        return;
      }

      default:
        break;
    }
  }

  // User flows
  if (!admin) {
    if (session.state === "USER_WAIT_CODE") {
      const code = inputText.trim().toUpperCase();
      const settings = await getSettings();
      if (!settings.registrationEnabled) {
        await bot.sendMessage(
          chatId,
          "Ro'yxatdan o'tish vaqtincha o'chirilgan.",
          { protect_content: true },
        );
        return;
      }

      const u = await usersCol.findOne({ oneTimeCode: code });
      if (!u) {
        await bot.sendMessage(
          chatId,
          "Kod noto'g'ri yoki allaqachon ishlatilgan. Qayta urinib ko'ring.",
          { protect_content: true },
        );
        return;
      }
      if (u.isBanned) {
        await bot.sendMessage(
          chatId,
          "Siz ushbu botdan foydalanish huquqidan mahrum qilingansiz.",
          { protect_content: true },
        );
        return;
      }
      if (u.codeUsed) {
        await bot.sendMessage(
          chatId,
          "Kod noto'g'ri yoki allaqachon ishlatilgan. Qayta urinib ko'ring.",
          { protect_content: true },
        );
        return;
      }

      await usersCol.updateOne(
        { _id: u._id },
        {
          $set: {
            telegramId: Number(userId),
            username: msg.from.username || null,
            codeUsed: true,
            joinedAt: now(),
            lastActiveAt: now(),
          },
        },
      );

      await clearSession(userId);
      await showUserMenu(chatId, msg.message_id, userId);
      return;
    }
  }
});

// =====================
// Callback Handler
// =====================
bot.on("callback_query", async (q) => {
  if (!dbReady) {
    await safeAnswerCallback(q, "Bot initializing...");
    return;
  }

  const userId = q.from.id;
  const chatId = q.message.chat.id;
  const msgId = q.message.message_id;
  const admin = isAdmin(userId);

  const session = await getSession(userId);
  const nowMs = Date.now();
  if (nowMs - (session.lastCallbackAt || 0) < CONFIG.DUP_CLICK_MS) {
    await safeAnswerCallback(q, "Biroz kuting...");
    return;
  }
  await setSession(userId, { lastCallbackAt: nowMs });

  if (!admin && (await guardBan(q))) return;

  const data = q.data || "";
  const parts = data.split(":");

  // Admin actions
  if (admin && data.startsWith("a:")) {
    await safeAnswerCallback(q);

    if (
      (parts[1] === "tests" || parts[1] === "test") &&
      parts[2] === "q" &&
      parts[3] === "skip-image"
    ) {
      const data = session.data || {};
      await setSession(userId, { state: "ADMIN_ADD_TEST_A", data });
      const rows = [
        [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
        [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
        [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
      ];
      await sendOrEdit(chatId, msgId, "Qaysi javob to'g'ri? (A/B/C/D)", {
        parse_mode: "HTML",
        protect_content: true,
        ...kb(rows),
      });
      return;
    }

    if (
      parts[1] === "tests" &&
      parts[2] === "q" &&
      parts[3] === "add" &&
      parts[4] === "skip-image"
    ) {
      const data = session.data || {};
      if (!data.currentQuestion || !data.testId) {
        await sendOrEdit(
          chatId,
          msgId,
          "Savol topilmadi. Qaytadan boshlang.",
          kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
        );
        return;
      }
      await setSession(userId, { state: "ADMIN_ADD_EXISTING_Q_ANSWER", data });
      const rows = [
        [
          {
            text: "⬅️ Orqaga",
            callback_data: `a:tests:q:manage:${data.testId}:page:1`,
          },
        ],
        [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
      ];
      await sendOrEdit(chatId, msgId, "Qaysi javob to'g'ri? (A/B/C/D)", {
        parse_mode: "HTML",
        protect_content: true,
        ...kb(rows),
      });
      return;
    }

    switch (parts[1]) {
      case "menu": {
        await showAdminMenu(chatId, msgId, q.from.first_name || "Admin");
        return;
      }
      case "top": {
        const view = await renderAdminTop();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
      case "stats": {
        const view = await renderStats();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
      case "users": {
        if (parts[2] === "page") {
          const page = Number(parts[3]) || 1;
          const queryIdx = parts.indexOf("q");
          const query =
            queryIdx > -1
              ? decodeURIComponent(parts.slice(queryIdx + 1).join(":"))
              : "";
          const view = await renderUsersList(page, query);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "add") {
          if (!(await ensurePrivateChat(q, userId))) return;
          await setSession(userId, { state: "ADMIN_ADD_USER_NAME", data: {} });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Foydalanuvchining ism-familyasini kiriting:",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "search") {
          if (!(await ensurePrivateChat(q, userId))) return;
          await setSession(userId, { state: "ADMIN_USERS_SEARCH", data: {} });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Qidirish uchun ism yoki familiya kiriting:",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "view") {
          if (session.state === "ADMIN_MSG_PICK_USER") {
            const u = await usersCol.findOne({ _id: new ObjectId(parts[3]) });
            if (!u || !u.telegramId) {
              await sendOrEdit(
                chatId,
                msgId,
                "Foydalanuvchi hali ro'yxatdan o'tmagan.",
                kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg:one" }]]),
              );
              return;
            }
            await setSession(userId, {
              state: "ADMIN_MSG_SINGLE_TEXT",
              data: { targetUserId: u.telegramId },
            });
            await sendOrEdit(
              chatId,
              msgId,
              `Foydalanuvchiga yuboriladigan xabarni kiriting: (${esc(u.fullName)})`,
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg:one" }]]),
            );
            return;
          }
          const view = await renderUserDetails(parts[3]);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "ban") {
          const targetId = new ObjectId(parts[3]);
          const u = await usersCol.findOne({ _id: targetId });
          if (!u) {
            await sendOrEdit(
              chatId,
              msgId,
              "Foydalanuvchi topilmadi.",
              kb([[{ text: "Orqaga", callback_data: "a:users:page:1" }]]),
            );
            return;
          }
          await usersCol.updateOne(
            { _id: targetId },
            { $set: { isBanned: true } },
          );
          await wipeUserHistory(u);
          await logAdminAction(userId, "USER_BAN", { userId: parts[3] });
          const view = await renderUserDetails(parts[3]);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "unban") {
          const targetId = new ObjectId(parts[3]);
          const u = await usersCol.findOne({ _id: targetId });
          if (!u) {
            await sendOrEdit(
              chatId,
              msgId,
              "Foydalanuvchi topilmadi.",
              kb([[{ text: "Orqaga", callback_data: "a:users:page:1" }]]),
            );
            return;
          }
          await wipeUserHistory(u);
          const settings = await getSettings();
          const code = await uniqueCode(
            settings.codeLength || CONFIG.DEFAULT_CODE_LENGTH,
          );
          await usersCol.updateOne(
            { _id: targetId },
            {
              $set: {
                isBanned: false,
                oneTimeCode: code,
                codeUsed: false,
                telegramId: null,
                username: null,
                joinedAt: null,
                lastActiveAt: null,
                totalTestsWorked: 0,
                averageScore: 0,
                lastResult: null,
                results: [],
              },
            },
          );
          await logAdminAction(userId, "USER_UNBAN", { userId: parts[3] });
          const view = await renderUserDetails(parts[3]);
          await sendOrEdit(
            chatId,
            msgId,
            view.text + `\n\nYangi kod: <code>${code}</code>`,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "code") {
          const settings = await getSettings();
          const code = await uniqueCode(
            settings.codeLength || CONFIG.DEFAULT_CODE_LENGTH,
          );
          await usersCol.updateOne(
            { _id: new ObjectId(parts[3]) },
            {
              $set: {
                oneTimeCode: code,
                codeUsed: false,
                telegramId: null,
                username: null,
                joinedAt: null,
              },
            },
          );
          await logAdminAction(userId, "USER_RENEW_CODE", { userId: parts[3] });
          const view = await renderUserDetails(parts[3]);
          await sendOrEdit(
            chatId,
            msgId,
            view.text + `\n\nYangi kod: <code>${code}</code>`,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "del" && parts[3] === "yes") {
          const id = parts[4];
          await usersCol.deleteOne({ _id: new ObjectId(id) });
          await logAdminAction(userId, "USER_DELETE", { userId: id });
          const view = await renderUsersList(1);
          await sendOrEdit(
            chatId,
            msgId,
            "✅ Foydalanuvchi o'chirildi.\n\n" + view.text,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "del") {
          const rows = [
            [{ text: "✅ Ha", callback_data: `a:users:del:yes:${parts[3]}` }],
            [{ text: "❌ Yo'q", callback_data: `a:users:view:${parts[3]}` }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Rostdan ham foydalanuvchini o'chirmoqchimisiz?",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "results") {
          const u = await usersCol.findOne({ _id: new ObjectId(parts[3]) });
          if (!u) {
            await sendOrEdit(
              chatId,
              msgId,
              "Foydalanuvchi topilmadi.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:users:page:1" }]]),
            );
            return;
          }
          const res = await resultsCol
            .find({ userId: u._id })
            .sort({ createdAt: -1 })
            .limit(10)
            .toArray();
          let text = `📊 <b>${esc(u.fullName)}</b> natijalari\n\n`;
          if (!res.length) text += "Natija yo'q.";
          for (const r of res) {
            const t = await testsCol.findOne({ _id: r.testId });
            text += `• ${esc(t?.title || "Test")} — <b>${r.percentage.toFixed(1)}%</b> (${r.correctCount}/${r.totalCount}) | ${formatDate(r.createdAt)}\n`;
          }
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: `a:users:view:${parts[3]}` }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(chatId, msgId, text, kb(rows));
          return;
        }
        return;
      }

      case "tests": {
        if (parts[2] === "page") {
          const view = await renderTestsList(Number(parts[3]) || 1);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "q" && parts[3] === "manage") {
          const pageIndex = parts.indexOf("page");
          const page = pageIndex > -1 ? Number(parts[pageIndex + 1]) : 1;
          const view = await renderTestQuestionsManage(parts[4], page || 1);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "q" && parts[3] === "edit") {
          if (!(await ensurePrivateChat(q, userId))) return;
          const testId = parts[4];
          const qIndex = Number(parts[5]);
          await setSession(userId, {
            state: "ADMIN_EDIT_Q_BLOCK",
            data: { testId, qIndex },
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${testId}:page:${Math.floor(qIndex / 3) + 1}`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Yangi savolni kiriting (savol + A/B/C/D):",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "q" && parts[3] === "answer") {
          if (!(await ensurePrivateChat(q, userId))) return;
          const testId = parts[4];
          const qIndex = Number(parts[5]);
          await setSession(userId, {
            state: "ADMIN_EDIT_Q_ANSWER",
            data: { testId, qIndex },
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${testId}:page:${Math.floor(qIndex / 3) + 1}`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Yangi to'g'ri javob (A/B/C/D):",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "q" && parts[3] === "image" && parts[4] === "remove") {
          const testId = parts[5];
          const qIndex = Number(parts[6]);
          const t = await testsCol.findOne({ _id: new ObjectId(testId) });
          if (!t || !t.questions?.[qIndex]) {
            await sendOrEdit(
              chatId,
              msgId,
              "Savol topilmadi.",
              kb([
                [
                  {
                    text: "⬅️ Orqaga",
                    callback_data: `a:tests:q:manage:${testId}:page:1`,
                  },
                ],
              ]),
            );
            return;
          }
          t.questions[qIndex].imageFileId = null;
          await testsCol.updateOne(
            { _id: t._id },
            { $set: { questions: t.questions } },
          );
          await logAdminAction(userId, "TEST_Q_IMAGE_REMOVE", {
            testId,
            qIndex,
          });
          const view = await renderTestQuestionsManage(
            testId,
            Math.floor(qIndex / 3) + 1,
          );
          await sendOrEdit(
            chatId,
            msgId,
            "Rasm o'chirildi.\n\n" + view.text,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "q" && parts[3] === "image") {
          if (!(await ensurePrivateChat(q, userId))) return;
          const testId = parts[4];
          const qIndex = Number(parts[5]);
          await setSession(userId, {
            state: "ADMIN_EDIT_Q_IMAGE",
            data: { testId, qIndex },
          });
          const rows = [
            [
              {
                text: "Rasmni o'chirish",
                callback_data: `a:tests:q:image:remove:${testId}:${qIndex}`,
              },
            ],
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${testId}:page:${Math.floor(qIndex / 3) + 1}`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            'Yangi rasm yuboring yoki "REMOVE" yozing:',
            kb(rows),
          );
          return;
        }
        if (parts[2] === "q" && parts[3] === "del" && parts[4] === "yes") {
          const testId = parts[5];
          const qIndex = Number(parts[6]);
          const t = await testsCol.findOne({ _id: new ObjectId(testId) });
          if (!t || !t.questions?.[qIndex]) {
            await sendOrEdit(
              chatId,
              msgId,
              "Savol topilmadi.",
              kb([
                [
                  {
                    text: "⬅️ Orqaga",
                    callback_data: `a:tests:q:manage:${testId}:page:1`,
                  },
                ],
              ]),
            );
            return;
          }
          t.questions.splice(qIndex, 1);
          await testsCol.updateOne(
            { _id: t._id },
            {
              $set: {
                questions: t.questions,
                questionCount: t.questions.length,
              },
            },
          );
          await logAdminAction(userId, "TEST_Q_DELETE", { testId, qIndex });
          const view = await renderTestQuestionsManage(
            testId,
            Math.max(1, Math.floor(qIndex / 3) + 1),
          );
          await sendOrEdit(
            chatId,
            msgId,
            "Savol o'chirildi.\n\n" + view.text,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "q" && parts[3] === "del") {
          const testId = parts[4];
          const qIndex = Number(parts[5]);
          const rows = [
            [
              {
                text: "✅ Ha",
                callback_data: `a:tests:q:del:yes:${testId}:${qIndex}`,
              },
            ],
            [
              {
                text: "❌ Yo'q",
                callback_data: `a:tests:q:manage:${testId}:page:${Math.floor(qIndex / 3) + 1}`,
              },
            ],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Savolni o'chirmoqchimisiz?",
            kb(rows),
          );
          return;
        }
        if (
          parts[2] === "q" &&
          parts[3] === "add" &&
          parts[4] !== "skip-image"
        ) {
          if (!(await ensurePrivateChat(q, userId))) return;
          const testId = parts[4];
          await setSession(userId, {
            state: "ADMIN_ADD_EXISTING_Q_BLOCK",
            data: { testId },
          });
          const rows = [
            [
              {
                text: "⬅️ Orqaga",
                callback_data: `a:tests:q:manage:${testId}:page:1`,
              },
            ],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Yangi savolni kiriting (savol + A/B/C/D). Rasm bo'lsa, rasmni yuborib captionda yozing:",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "add") {
          if (!(await ensurePrivateChat(q, userId))) return;
          await setSession(userId, { state: "ADMIN_ADD_TEST_TITLE", data: {} });
          const rows = [
            [{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }],
            [{ text: "🏠 Bosh menyu", callback_data: "a:menu" }],
            [{ text: "❌ Bekor qilish", callback_data: "a:flow:cancel" }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Test uchun titul kiriting:",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "view") {
          const view = await renderTestDetails(parts[3]);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "questions") {
          const pageIndex = parts.indexOf("page");
          const page = pageIndex > -1 ? Number(parts[pageIndex + 1]) : 1;
          const view = await renderTestQuestions(parts[3], page);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "leader") {
          const view = await renderTestLeaderboard(parts[3]);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "edit" && parts[3] === "title") {
          if (!(await ensurePrivateChat(q, userId))) return;
          const id = parts[4];
          await setSession(userId, {
            state: "ADMIN_TEST_EDIT_TITLE",
            data: { testId: id },
          });
          await sendOrEdit(
            chatId,
            msgId,
            "Yangi test nomini kiriting:",
            kb([[{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${id}` }]]),
          );
          return;
        }
        if (parts[2] === "edit" && parts[3] === "desc") {
          if (!(await ensurePrivateChat(q, userId))) return;
          const id = parts[4];
          await setSession(userId, {
            state: "ADMIN_TEST_EDIT_DESC",
            data: { testId: id },
          });
          await sendOrEdit(
            chatId,
            msgId,
            "Yangi izoh kiriting:",
            kb([[{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${id}` }]]),
          );
          return;
        }
        if (parts[2] === "del" && parts[3] === "yes") {
          const id = parts[4];
          await testsCol.deleteOne({ _id: new ObjectId(id) });
          await logAdminAction(userId, "TEST_DELETE", { testId: id });
          const view = await renderTestsList(1);
          await sendOrEdit(
            chatId,
            msgId,
            "✅ Test o'chirildi.\n\n" + view.text,
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "del") {
          const rows = [
            [{ text: "✅ Ha", callback_data: `a:tests:del:yes:${parts[3]}` }],
            [{ text: "❌ Yo'q", callback_data: `a:tests:view:${parts[3]}` }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Rostdan ham testni o'chirmoqchimisiz?",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "edit") {
          const rows = [
            [
              {
                text: "✏️ Nomi",
                callback_data: `a:tests:edit:title:${parts[3]}`,
              },
            ],
            [
              {
                text: "📝 Izoh",
                callback_data: `a:tests:edit:desc:${parts[3]}`,
              },
            ],
            [{ text: "⬅️ Orqaga", callback_data: `a:tests:view:${parts[3]}` }],
          ];
          await sendOrEdit(
            chatId,
            msgId,
            "Nimani tahrirlashni tanlang:",
            kb(rows),
          );
          return;
        }
        if (parts[2] === "save") {
          const data = session.data || {};
          if (!data.title || !data.questions || !data.count) {
            await sendOrEdit(
              chatId,
              msgId,
              "Saqlash uchun maʼlumotlar yetarli emas.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:tests:page:1" }]]),
            );
            return;
          }
          const doc = {
            title: data.title,
            description: data.description || "",
            questionCount: data.count,
            questions: data.questions,
            createdAt: now(),
            totalAttempts: 0,
            averageScore: 0,
            lastUsedAt: null,
          };
          await testsCol.insertOne(doc);
          await logAdminAction(userId, "TEST_ADD", { title: data.title });
          await clearSession(userId);
          const view = await renderTestsList(1);
          await sendOrEdit(
            chatId,
            msgId,
            "✅ Test saqlandi.\n\n" + view.text,
            view.keyboard,
          );
          return;
        }
        return;
      }

      case "msg": {
        if (parts[2] === undefined) {
          const rows = [
            [{ text: "👤 Bitta foydalanuvchiga", callback_data: "a:msg:one" }],
            [{ text: "📢 Barchaga", callback_data: "a:msg:all" }],
            [{ text: "⬅️ Orqaga", callback_data: "a:menu" }],
          ];
          await sendOrEdit(chatId, msgId, "📨 <b>Xabar yuborish</b>", kb(rows));
          return;
        }
        if (parts[2] === "one") {
          if (!(await ensurePrivateChat(q, userId))) return;
          await setSession(userId, { state: "ADMIN_MSG_PICK_USER", data: {} });
          const view = await renderUsersList(1);
          await sendOrEdit(
            chatId,
            msgId,
            "Bitta foydalanuvchini tanlang:",
            view.keyboard,
          );
          return;
        }
        if (parts[2] === "all") {
          if (!(await ensurePrivateChat(q, userId))) return;
          await setSession(userId, { state: "ADMIN_MSG_ALL_TEXT", data: {} });
          await sendOrEdit(
            chatId,
            msgId,
            "Barchaga yuboriladigan xabarni kiriting:",
            kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
          );
          return;
        }
        if (parts[2] === "send" && parts[3] === "single") {
          const data = session.data || {};
          if (!data.targetUserId || !data.messageText) {
            await sendOrEdit(
              chatId,
              msgId,
              "Xabar maʼlumotlari topilmadi.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
            );
            return;
          }
          try {
            await bot.sendMessage(data.targetUserId, data.messageText, {
              protect_content: true,
            });
            await sendOrEdit(
              chatId,
              msgId,
              "✅ Xabar yuborildi.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
            );
          } catch (err) {
            await sendOrEdit(
              chatId,
              msgId,
              "❌ Xabar yuborishda xatolik.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
            );
          }
          await logAdminAction(userId, "MSG_SINGLE");
          await clearSession(userId);
          return;
        }
        if (parts[2] === "send" && parts[3] === "all") {
          const data = session.data || {};
          if (!data.messageText) {
            await sendOrEdit(
              chatId,
              msgId,
              "Xabar matni topilmadi.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
            );
            return;
          }
          const users = await usersCol
            .find({ telegramId: { $ne: null } })
            .toArray();
          let sent = 0;
          let failed = 0;
          for (const u of users) {
            try {
              await bot.sendMessage(u.telegramId, data.messageText, {
                protect_content: true,
              });
              sent++;
            } catch (err) {
              failed++;
            }
          }
          const text = `✅ Yuborish yakunlandi.\n\nYuborildi: <b>${sent}</b>\nXatolik: <b>${failed}</b>`;
          await sendOrEdit(
            chatId,
            msgId,
            text,
            kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
          );
          await logAdminAction(userId, "MSG_ALL", { sent, failed });
          await clearSession(userId);
          return;
        }
        if (parts[2] === "cancel") {
          await clearSession(userId);
          await sendOrEdit(
            chatId,
            msgId,
            "Bekor qilindi.",
            kb([[{ text: "⬅️ Orqaga", callback_data: "a:msg" }]]),
          );
          return;
        }
        return;
      }

      case "settings": {
        const view = await renderSettings();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }

      case "flow": {
        if (parts[2] === "cancel") {
          await clearSession(userId);
          await sendOrEdit(
            chatId,
            msgId,
            "Bekor qilindi.",
            kb([[{ text: "🏠 Bosh menyu", callback_data: "a:menu" }]]),
          );
          return;
        }
        return;
      }

      default:
        break;
    }

    if (data.startsWith("a:settings:")) {
      const key = parts[2];
      if (key === "botname") {
        if (!(await ensurePrivateChat(q, userId))) return;
        await setSession(userId, { state: "ADMIN_SETTINGS_BOTNAME", data: {} });
        await sendOrEdit(
          chatId,
          msgId,
          "Bot nomini kiriting:",
          kb([[{ text: "⬅️ Orqaga", callback_data: "a:settings" }]]),
        );
        return;
      }
      if (key === "welcome") {
        if (!(await ensurePrivateChat(q, userId))) return;
        await setSession(userId, { state: "ADMIN_SETTINGS_WELCOME", data: {} });
        await sendOrEdit(
          chatId,
          msgId,
          "Welcome textni kiriting ( {name} ishlatilishi mumkin):",
          kb([[{ text: "⬅️ Orqaga", callback_data: "a:settings" }]]),
        );
        return;
      }
      if (key === "codelen") {
        if (!(await ensurePrivateChat(q, userId))) return;
        await setSession(userId, { state: "ADMIN_SETTINGS_CODELEN", data: {} });
        await sendOrEdit(
          chatId,
          msgId,
          "Kod uzunligini kiriting (4-12):",
          kb([[{ text: "⬅️ Orqaga", callback_data: "a:settings" }]]),
        );
        return;
      }
      if (key === "regtoggle") {
        const settings = await getSettings();
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { registrationEnabled: !settings.registrationEnabled } },
        );
        await logAdminAction(userId, "SETTINGS_REG_TOGGLE", {
          enabled: !settings.registrationEnabled,
        });
        const view = await renderSettings();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
      if (key === "testtoggle") {
        const settings = await getSettings();
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { testEnabled: !settings.testEnabled } },
        );
        await logAdminAction(userId, "SETTINGS_TEST_TOGGLE", {
          enabled: !settings.testEnabled,
        });
        const view = await renderSettings();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
      if (key === "logchan") {
        if (!(await ensurePrivateChat(q, userId))) return;
        await setSession(userId, { state: "ADMIN_SETTINGS_LOGCHAN", data: {} });
        await sendOrEdit(
          chatId,
          msgId,
          "Admin log kanal ID kiriting (o'chirish uchun 0):",
          kb([[{ text: "⬅️ Orqaga", callback_data: "a:settings" }]]),
        );
        return;
      }
      if (key === "timelimit") {
        const settings = await getSettings();
        await settingsCol.updateOne(
          { _id: "main" },
          { $set: { testTimeLimitEnabled: !settings.testTimeLimitEnabled } },
        );
        await logAdminAction(userId, "SETTINGS_TIMELIMIT_TOGGLE", {
          enabled: !settings.testTimeLimitEnabled,
        });
        const view = await renderSettings();
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
    }
  }

  // User actions
  if (!admin && data.startsWith("u:")) {
    await safeAnswerCallback(q);

    switch (parts[1]) {
      case "menu": {
        await showUserMenu(chatId, msgId, userId);
        return;
      }
      case "test": {
        if (parts[2] === "finish" && parts[3] === "yes") {
          const testId = parts[4];
          const s = await getSession(userId);
          if (s.state !== "USER_TEST" || s.data?.testId !== testId) return;
          await finalizeTest(userId, testId, s.data, chatId, msgId);
          return;
        }
        if (parts[2] === "finish" && parts[3] === "no") {
          const testId = parts[4];
          const s = await getSession(userId);
          if (s.state !== "USER_TEST" || s.data?.testId !== testId) return;
          const test = await testsCol.findOne({ _id: new ObjectId(testId) });
          if (!test) return;
          const settings = await getSettings();
          await sendQuestionWithImage(
            chatId,
            msgId,
            test,
            s.data.index || 0,
            s.data,
            settings,
          );
          return;
        }
        if (parts[2] === "view") {
          const view = await renderUserTestInfo(parts[3]);
          await sendOrEdit(chatId, msgId, view.text, view.keyboard);
          return;
        }
        if (parts[2] === "start") {
          const settings = await getSettings();
          if (!settings.testEnabled) {
            await sendOrEdit(
              chatId,
              msgId,
              "Testlar vaqtincha o'chirilgan.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "u:menu" }]]),
            );
            return;
          }
          const test = await testsCol.findOne({ _id: new ObjectId(parts[3]) });
          if (!test) {
            await sendOrEdit(
              chatId,
              msgId,
              "Test topilmadi.",
              kb([[{ text: "⬅️ Orqaga", callback_data: "u:menu" }]]),
            );
            return;
          }
          const data = {
            testId: test._id.toString(),
            index: 0,
            answers: [],
            startedAt: now(),
          };
          await setSession(userId, { state: "USER_TEST", data });
          await updateLastActive(userId);
          await sendQuestionWithImage(chatId, null, test, 0, data, settings);
          return;
        }
        if (parts[2] === "ans") {
          const testId = parts[3];
          const index = Number(parts[4]);
          const option = parts[5];
          const s = await getSession(userId);
          if (s.state !== "USER_TEST" || s.data?.testId !== testId) return;

          const settings = await getSettings();
          if (
            settings.testTimeLimitEnabled &&
            elapsedSeconds(s.data) >= CONFIG.TEST_TIME_LIMIT_SEC
          ) {
            await finalizeTest(userId, testId, s.data, chatId, msgId, "time");
            return;
          }

          const test = await testsCol.findOne({ _id: new ObjectId(testId) });
          if (!test) return;

          if (!s.data.answers) s.data.answers = [];
          if (s.data.answers[index]) {
            await sendQuestionWithImage(
              chatId,
              msgId,
              test,
              index,
              s.data,
              settings,
            );
            return;
          }

          const correct = test.questions[index].correctAnswer === option;
          s.data.answers[index] = {
            selected: option,
            correct,
            correctAnswer: test.questions[index].correctAnswer,
          };
          await setSession(userId, { data: s.data });

          await sendQuestionWithImage(
            chatId,
            msgId,
            test,
            index,
            s.data,
            settings,
          );
          return;
        }
        if (parts[2] === "next") {
          const testId = parts[3];
          const index = Number(parts[4]);
          const s = await getSession(userId);
          if (s.state !== "USER_TEST" || s.data?.testId !== testId) return;

          const settings = await getSettings();
          if (
            settings.testTimeLimitEnabled &&
            elapsedSeconds(s.data) >= CONFIG.TEST_TIME_LIMIT_SEC
          ) {
            await finalizeTest(userId, testId, s.data, chatId, msgId, "time");
            return;
          }

          const test = await testsCol.findOne({ _id: new ObjectId(testId) });
          if (!test) return;

          const nextIndex = index + 1;
          if (nextIndex >= test.questionCount) {
            const rows = [
              [{ text: "✅ Ha", callback_data: `u:test:finish:yes:${testId}` }],
              [
                {
                  text: "❌ Yo'q",
                  callback_data: `u:test:finish:no:${testId}`,
                },
              ],
            ];
            await clearInlineKeyboard(chatId, msgId);
            await sendOrEdit(
              chatId,
              null,
              "Savollar tugadi. Testni yakunlamoqchimisiz?",
              kb(rows),
            );
            return;
          }

          s.data.index = nextIndex;
          await setSession(userId, { data: s.data });
          await sendQuestionWithImage(
            chatId,
            msgId,
            test,
            nextIndex,
            s.data,
            settings,
            true,
          );
          return;
        }
        if (parts[2] === "finish" && parts[3] !== "yes" && parts[3] !== "no") {
          const rows = [
            [{ text: "✅ Ha", callback_data: `u:test:finish:yes:${parts[3]}` }],
            [
              {
                text: "❌ Yo'q",
                callback_data: `u:test:finish:no:${parts[3]}`,
              },
            ],
          ];
          await clearInlineKeyboard(chatId, msgId);
          await sendOrEdit(
            chatId,
            null,
            "Testni yakunlamoqchimisiz?",
            kb(rows),
          );
          return;
        }
        return;
      }
      case "results": {
        const view = await renderUserResults(userId);
        await sendOrEdit(chatId, msgId, view.text, view.keyboard);
        return;
      }
      case "help": {
        const text =
          "ℹ️ <b>Yordam</b>\n\nTestni tanlang va boshlang. Har savolga bir marta javob beriladi. Orqaga qaytib o'zgartirib bo'lmaydi.";
        const rows = [
          [{ text: "⬅️ Orqaga", callback_data: "u:menu" }],
          [{ text: "🏠 Bosh menyu", callback_data: "u:menu" }],
        ];
        await sendOrEdit(chatId, msgId, text, kb(rows));
        return;
      }
      case "noop":
        return;
      default:
        break;
    }
  }
});

// =====================
// Settings view
// =====================
async function renderSettings() {
  const s = await getSettings();
  const text = [
    "⚙️ <b>Sozlamalar</b>",
    "",
    `• Bot nomi: <b>${esc(s.botName)}</b>`,
    `• Welcome text: ${esc(s.welcomeText)}`,
    `• Kod uzunligi: <b>${s.codeLength}</b>`,
    `• Ro'yxatdan o'tish: <b>${s.registrationEnabled ? "ON" : "OFF"}</b>`,
    `• Test ishlash: <b>${s.testEnabled ? "ON" : "OFF"}</b>`,
    `• Admin log kanal ID: <b>${s.adminLogChannelId ?? "-"}</b>`,
    `• Test time limit: <b>${s.testTimeLimitEnabled ? "ON" : "OFF"}</b> (${Math.floor(CONFIG.TEST_TIME_LIMIT_SEC / 60)} min)`,
  ].join("\n");

  const rows = [
    [{ text: "Bot nomi", callback_data: "a:settings:botname" }],
    [{ text: "Welcome text", callback_data: "a:settings:welcome" }],
    [{ text: "Kod uzunligi", callback_data: "a:settings:codelen" }],
    [
      {
        text: `Ro'yxatdan o'tish: ${s.registrationEnabled ? "ON" : "OFF"}`,
        callback_data: "a:settings:regtoggle",
      },
    ],
    [
      {
        text: `Test ishlash: ${s.testEnabled ? "ON" : "OFF"}`,
        callback_data: "a:settings:testtoggle",
      },
    ],
    [{ text: "Admin log kanal ID", callback_data: "a:settings:logchan" }],
    [
      {
        text: `Test time limit: ${s.testTimeLimitEnabled ? "ON" : "OFF"}`,
        callback_data: "a:settings:timelimit",
      },
    ],
    [{ text: "⬅️ Orqaga", callback_data: "a:menu" }],
  ];
  return { text, keyboard: kb(rows) };
}

// =====================
// Init
// =====================
async function init() {
  await mongo.connect();
  db = mongo.db();
  usersCol = db.collection("users");
  testsCol = db.collection("tests");
  resultsCol = db.collection("results");
  settingsCol = db.collection("settings");
  sessionsCol = db.collection("sessions");
  logsCol = db.collection("logs");

  await ensureSettings();

  await usersCol.createIndex(
    { oneTimeCode: 1 },
    { unique: true, sparse: true },
  );
  await usersCol.createIndex({ telegramId: 1 }, { unique: true, sparse: true });
  await resultsCol.createIndex({ userId: 1, createdAt: -1 });
  await testsCol.createIndex({ createdAt: -1 });
  await sessionsCol.createIndex({ userId: 1 }, { unique: true });

  dbReady = true;
  console.log("Bot started.");
}

async function start() {
  try {
    await init();
    await bot.startPolling();
  } catch (err) {
    console.error("Init error:", err);
    process.exit(1);
  }
}

start();
