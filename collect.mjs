#!/usr/bin/env node
/**
 * feeds-collector / collect.mjs
 * ---------------------------------------------------------------------------
 * 「每日信息源聚合」管线的采集端。零依赖、Node >= 20、纯 ESM，只用 node: 内置模块
 * （fetch 为全局）。中文精炼在 App 本机做，这里只输出原始语言的 title/snippet
 * + 确定性打分。
 *
 * 运行环境：GitHub Actions（数据仓库 williamgao1313-design/JT-EOS-feeds），
 * 产出 data/feeds/latest.json 提交回同一仓库。本机只从 raw.githubusercontent.com
 * 读这一个文件。
 *
 * 用法：
 *   node collect.mjs                       # 输出到 ./data/feeds/latest.json
 *   node collect.mjs --out <path>          # 覆盖输出路径
 *   node collect.mjs --only key1,key2      # 只抓指定源（调试）
 *
 * 抓取闸门（硬性）：
 *   - 全程串行，任意两次请求之间 >= 1000ms（以上一次请求**结束**为起点计时）
 *   - 同一 host 一旦收到 403/429，立即停止抓该 host，错误如实记录
 *   - 请求总数上限 15（含重试）；网络/解析失败绝不折叠成 "0 条"，必须留下 error
 *
 * 十一个源（key / 形态 / domain）：
 *   hackernews             HN Algolia search_by_date（48h 内 >50 分）  → 按内容判 ai/github/vibe_coding
 *   show_hn                HN Algolia show_hn（48h 内 >25 分）        → 强制 vibe_coding
 *   lobsters               lobste.rs/rss                              → vibe_coding
 *   github_trending        github.com/trending?since=daily（HTML）     → github
 *   simonwillison          simonwillison.net/atom/everything/（Atom）  → ai
 *   google_news_ai         Google News RSS "AI model release when:1d" → ai
 *   google_news_onhold     Google News RSS on hold/delisted when:30d  → publishing
 *   google_news_retraction Google News RSS 撤稿 期刊 when:30d          → publishing
 *   infodocket             infodocket.com/feed/（RSS）                 → publishing
 *   scholarly_kitchen      scholarlykitchen.sspnet.org/feed/（RSS）    → publishing
 *   retraction_watch       retractionwatch.com/feed/（RSS）            → publishing
 *
 * HN 的两个特殊处理（实测得出的，别删）：
 *   1) vibe_coding 落的 HN 条目必须过**技术主题闸门** HN_TECH_WORDS：HN 是通用热榜，
 *      48h/50 分以上的帖子里混着"最老的陆地动物"这类与前端后端无关的新闻（实测 48 条里 28 条该丢）。
 *   2) show_hn 用 forceDomain 整体归 vibe_coding，且不过闸门（"Show HN" 本身就是技术帖）。
 *   3) /search 端点按相关度排序、没有时间窗，叠加 30 天窗口会返回 200 但 0 条 ——
 *      必须用 /search_by_date + created_at_i。
 *
 * 打分公式（确定性，结果 clamp 到 0..10 后取整）：
 *   score = clamp(0, 10, round(
 *       baseline(source)                  // 源优先级基线：retraction_watch 4；
 *                                         //   scholarly_kitchen / infodocket /
 *                                         //   google_news_retraction / google_news_onhold /
 *                                         //   simonwillison / lobsters 3；其余 2
 *     + freshness(publishedAt)            // <=24h: +3 / <=72h: +2 / 其它（含 null）: +1
 *     + min(signalHits, 2)                // 每个命中的 domain 信号词 +1，最多 +2
 *                                         //   publishing: on hold/delisted/delist/Clarivate/
 *                                         //     Web of Science/Scopus/retraction/撤稿/收录
 *                                         //   ai: OpenAI/Anthropic/Gemini/Llama/DeepSeek/
 *                                         //     benchmark/release
 *                                         //   vibe_coding: vscode/copilot/cursor/typescript/
 *                                         //     react/rust/library/frontend/backend/api/cli/
 *                                         //     browser/performance/database/show hn
 *                                         //   github: trending（+ 下面的 star 数）
 *     + githubStars(starsToday)           // 仅 domain=github：>=1000: +2 / >=200: +1
 *   ))
 *
 * counts 定义：
 *   raw     = 所有源通过 30 天窗口后的条目总数（= 各源 count 之和，含重复）
 *   deduped = 经 URL 指纹 + title 指纹确定性去重后的条目数
 *   kept    = 最终写入 items 的数量（当前不做额外截断，故通常等于 deduped）
 *
 * 去重（确定性，不依赖随机/时间）：先按归一化 URL（小写 host+path，去协议/www/
 * 尾斜杠/查询串），再按 title 指纹（小写、去标点、压空白后取前 60 字符）。
 * 重复时保留分数更高的一条；同分保留 source 优先级更高、发布更晚的。
 *
 * 退出码：任意一个源成功 -> 0（部分成功也要提交）；全部失败 -> 1。
 * ---------------------------------------------------------------------------
 */

import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/* ============================== 常量 ============================== */

const SCHEMA_VERSION = 1;
const DEFAULT_OUT = 'data/feeds/latest.json';

const MIN_GAP_MS = 1000;          // 请求之间的最小间隔
const MAX_REQUESTS = 15;          // 总请求预算（含重试）
const REQUEST_TIMEOUT_MS = 20000;
const MAX_AGE_DAYS = 30;          // 只保留最近 30 天
const SNIPPET_MAX = 300;

const USER_AGENT =
  'Mozilla/5.0 (compatible; journal-tools-feeds-collector/1.0; +https://github.com/williamgao1313-design/JT-EOS-feeds)';

const ACCEPT_FEED =
  'application/atom+xml, application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, text/html;q=0.8, application/json;q=0.8, */*;q=0.5';

/** 源优先级基线（同时作为去重同分时的 source 优先级）。 */
const SOURCE_BASELINE = {
  retraction_watch: 4,
  scholarly_kitchen: 3,
  infodocket: 3,
  google_news_retraction: 3,
  google_news_onhold: 3,
  simonwillison: 3,
  lobsters: 3,
  google_news_ai: 2,
  hackernews: 2,
  show_hn: 2,
  github_trending: 2,
};

/** 各 domain 的关键词信号（tags 里放命中的这些词）。 */
const SIGNALS = {
  publishing: [
    'on hold', 'delisted', 'delist', 'clarivate', 'web of science',
    'scopus', 'retraction', '撤稿', '收录',
  ],
  ai: ['openai', 'anthropic', 'gemini', 'llama', 'deepseek', 'benchmark', 'release'],
  vibe_coding: ['vscode', 'copilot', 'cursor', 'typescript', 'react', 'rust', 'library',
    'frontend', 'backend', 'api', 'cli', 'browser', 'performance', 'database', 'show hn'],
  github: ['trending'],
};

/** 用于判定 hackernews 条目是否落到 ai domain 的词表（比 SIGNALS.ai 宽）。 */
const AI_WORDS = [
  'ai', 'llm', 'llms', 'gpt', 'gpt-4', 'gpt-5', 'openai', 'anthropic', 'claude',
  'gemini', 'llama', 'deepseek', 'mistral', 'qwen', 'neural', 'transformer',
  'machine learning', 'deep learning', 'agentic', 'inference', 'fine-tuning',
  'embedding', 'diffusion',
];

/** HN 热榜是**通用**热榜：实测 48h/50 分以上的帖子里混着"最老的陆地动物""Margaret Hamilton
 *  逝世"这类与前端后端无关的新闻。所以 vibe_coding 落的条目必须再过一道**技术主题闸门**：
 *  命中任一技术信号词（词边界匹配，rust 不会命中 trust）或链接指向 github.com 才留下，
 *  否则丢弃（丢弃条数打到 stdout，不改 JSON 契约）。ai / github 两类本来就自带主题。 */
const HN_TECH_WORDS = [
  'github', 'gitlab', 'git', 'commit', 'code', 'coding', 'codebase', 'programmer',
  'programming', 'developer', 'software', 'engineer', 'engineering', 'library',
  'library', 'framework', 'compiler', 'interpreter', 'runtime', 'syntax',
  'typescript', 'javascript', 'python', 'rust', 'golang', 'ruby', 'php', 'java',
  'kotlin', 'swift', 'scala', 'haskell', 'elixir', 'clojure', 'lua', 'zig',
  'react', 'vue', 'svelte', 'angular', 'node', 'deno', 'bun', 'npm', 'pnpm',
  'vite', 'webpack', 'rollup', 'esbuild', 'css', 'html', 'dom', 'api', 'sdk',
  'cli', 'terminal', 'shell', 'bash', 'zsh', 'fish', 'vim', 'neovim', 'emacs',
  'vscode', 'editor', 'ide', 'debugger', 'database', 'postgres', 'postgresql',
  'mysql', 'sqlite', 'redis', 'mongodb', 'sql', 'query', 'server', 'backend',
  'frontend', 'fullstack', 'browser', 'chrome', 'firefox', 'safari', 'webkit',
  'http', 'tcp', 'dns', 'latency', 'performance', 'benchmark', 'profil',
  'refactor', 'debugging', 'unit test', 'integration test', 'testing', 'docker',
  'kubernetes', 'k8s', 'container', 'aws', 'azure', 'cloudflare', 'cloud',
  'linux', 'kernel', 'unix', 'macos', 'windows', 'android', 'ios', 'mobile',
  'security', 'encryption', 'authentication', 'vulnerability', 'cve', 'regex',
  'algorithm', 'data structure', 'distributed', 'concurrency', 'async',
  'threading', 'memory', 'cache', 'garbage collection', 'typescript', 'monorepo',
  'release', 'changelog', 'migration', 'devops', 'observability', 'logging',
  'graphql', 'rest', 'grpc', 'websocket', 'oauth', 'json', 'yaml', 'markdown',
  'lint', 'formatter', 'prettier', 'eslint', 'bundler', 'transpiler', 'wasm',
  'webassembly', 'cargo', 'pip', 'poetry', 'homebrew', 'apt', 'nix', 'tmux',
  'ssh', 'curl', 'grep', 'awk', 'sed', 'make', 'cmake', 'bazel', 'gradle',
  'terraform', 'ansible', 'playwright', 'puppeteer', 'selenium', 'jest',
  'vitest', 'pytest', 'gpu', 'cuda', 'pytorch', 'tensorflow', 'jax', 'dataset',
  'tokenizer', 'huggingface', 'compiler', 'web server', 'open source',
  'opensource', 'startup', 'version', 'upgrade', 'patch', 'bug', 'feature flag',
];

/** 跟踪参数：整参数丢弃。 */
const TRACKING_PARAMS = new Set([
  'fbclid', 'gclid', 'dclid', 'msclkid', 'mc_cid', 'mc_eid', 'igshid', 'yclid',
  '_hsenc', '_hsmi', 'vero_id', 'wickedid', 'oly_anon_id', 'oly_enc_id',
  's_kwcid', 'spm', 'scid', 'ref', 'ref_src', 'ref_url', 'source', 'campaign',
]);

const NAMED_ENTITIES = {
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ensp: ' ', emsp: ' ',
  ndash: '\u2013', mdash: '\u2014', hellip: '\u2026', middot: '\u00b7', bull: '\u2022',
  lsquo: '\u2018', rsquo: '\u2019', ldquo: '\u201c', rdquo: '\u201d',
  copy: '\u00a9', reg: '\u00ae', trade: '\u2122', deg: '\u00b0', plusmn: '\u00b1',
  times: '\u00d7', laquo: '\u00ab', raquo: '\u00bb', eacute: '\u00e9',
  agrave: '\u00e0', uuml: '\u00fc', ouml: '\u00f6',
};

const CJK_RE = /[\u3400-\u9fff\uf900-\ufaff]/;

/* ============================== 源定义 ============================== */

function googleNewsUrl(query) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;
}

const SOURCES = [
  {
    key: 'hackernews',
    kind: 'hn',
    domain: 'vibe_coding',
    // ⚠ 不能用 /search：它按相关度排序、没有时间窗，叠加脚本的 30 天窗口后实测返回 200 但 0 条，
    // 结果是 vibe_coding 一整个领域空着。改用 /search_by_date + created_at_i，只取最近 48h 内
    // 50 分以上的 story（HN 现在几乎每天都有够分的技术帖，48h 兜住周末/跑批间隔）。
    url: () => {
      const since = Math.floor(Date.now() / 1000) - 48 * 3600;
      return `https://hn.algolia.com/api/v1/search_by_date?tags=story&numericFilters=points%3E50,created_at_i%3E${since}&hitsPerPage=50`;
    },
  },
  {
    key: 'show_hn',
    kind: 'hn',
    domain: 'vibe_coding',
    // Show HN = 独立开发者刚上线的工具/项目，正是"vibe coding"想看的东西（实测普通 HN 热榜
    // 48h 内够分的帖子里只有 3 条属于前端后端）。这类天然是技术帖，不必再过技术闸门。
    forceDomain: 'vibe_coding',
    url: () => {
      const since = Math.floor(Date.now() / 1000) - 48 * 3600;
      return `https://hn.algolia.com/api/v1/search_by_date?tags=show_hn&numericFilters=points%3E25,created_at_i%3E${since}&hitsPerPage=40`;
    },
  },
  {
    key: 'lobsters',
    kind: 'feed',
    domain: 'vibe_coding',
    // 纯编程社区，没有通用新闻噪音，正好补 vibe_coding 的日常量。
    url: 'https://lobste.rs/rss',
  },
  {
    key: 'github_trending',
    kind: 'github',
    domain: 'github',
    url: 'https://github.com/trending?since=daily',
  },
  {
    key: 'simonwillison',
    kind: 'feed',
    domain: 'ai',
    url: 'https://simonwillison.net/atom/everything/',
  },
  {
    key: 'google_news_ai',
    kind: 'feed',
    domain: 'ai',
    url: googleNewsUrl('AI model release when:1d'),
  },
  {
    key: 'google_news_onhold',
    kind: 'feed',
    domain: 'publishing',
    url: googleNewsUrl('"on hold" OR delisted journal when:30d'),
  },
  {
    key: 'google_news_retraction',
    kind: 'feed',
    domain: 'publishing',
    url: googleNewsUrl('撤稿 期刊 when:30d'),
  },
  {
    key: 'infodocket',
    kind: 'feed',
    domain: 'publishing',
    url: 'https://www.infodocket.com/feed/',
  },
  {
    key: 'scholarly_kitchen',
    kind: 'feed',
    domain: 'publishing',
    url: 'https://scholarlykitchen.sspnet.org/feed/',
  },
  {
    key: 'retraction_watch',
    kind: 'feed',
    domain: 'publishing',
    url: 'https://retractionwatch.com/feed/',
  },
];

/* ============================== 小工具 ============================== */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function decodeEntities(input) {
  return String(input).replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);/g, (whole, body) => {
    if (body[0] === '#') {
      const hex = body[1] === 'x' || body[1] === 'X';
      const code = Number.parseInt(body.slice(hex ? 2 : 1), hex ? 16 : 10);
      if (Number.isFinite(code) && code > 0 && code <= 0x10ffff) {
        try { return String.fromCodePoint(code); } catch { return whole; }
      }
      return whole;
    }
    const key = body.toLowerCase();
    return Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, key) ? NAMED_ENTITIES[key] : whole;
  });
}

/** HTML/XML -> 纯文本：剥 CDATA、剥脚本样式、剥标签、解实体、压空白。 */
function toPlainText(raw) {
  let s = String(raw == null ? '' : raw);
  s = s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
  s = s.replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ');
  s = s.replace(/<[^>]*>/g, ' ');
  s = decodeEntities(s);
  s = s.replace(/<[^>]*>/g, ' '); // 解码后可能又冒出标签（&lt;p&gt;），再剥一次
  s = decodeEntities(s);
  return s.replace(/\s+/g, ' ').trim();
}

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** 词命中：ASCII 词用词边界（避免 rust 命中 trust），CJK 词用子串。 */
function hasWord(haystackLower, word) {
  const w = String(word).toLowerCase();
  if (!w) return false;
  if (CJK_RE.test(w)) return haystackLower.includes(w);
  const re = new RegExp(`(^|[^a-z0-9])${escapeRe(w)}([^a-z0-9]|$)`);
  return re.test(haystackLower);
}

function isTrackingParam(name) {
  const n = String(name).toLowerCase();
  return n.startsWith('utm_') || TRACKING_PARAMS.has(n);
}

/** 输出用 URL：绝对地址 + 去掉跟踪参数（保留其它查询串）。 */
function stripTracking(raw) {
  try {
    const u = new URL(raw);
    const keep = [];
    for (const [k, v] of u.searchParams) if (!isTrackingParam(k)) keep.push([k, v]);
    u.search = '';
    for (const [k, v] of keep) u.searchParams.append(k, v);
    u.hash = '';
    return u.toString();
  } catch {
    return String(raw || '').trim();
  }
}

/** 去重 / id 用归一化 URL：小写 host + path，去协议、www、尾斜杠、查询串。 */
function normalizeUrlKey(raw) {
  try {
    const u = new URL(raw);
    let host = u.hostname.toLowerCase();
    if (host.startsWith('www.')) host = host.slice(4);
    let path = u.pathname.replace(/\/+$/, '');
    if (path === '') path = '/';
    return `${host}${path}`;
  } catch {
    return String(raw || '')
      .trim()
      .toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split(/[?#]/)[0]
      .replace(/\/+$/, '');
  }
}

function titleFingerprint(title) {
  return String(title || '')
    .toLowerCase()
    .replace(/[\p{P}\p{S}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 60);
}

function makeId(url) {
  return createHash('sha1').update(normalizeUrlKey(url)).digest('hex').slice(0, 16);
}

/** 宽松日期解析：解析不出来返回 null（条目仍保留）。 */
function parseDate(raw) {
  const s = toPlainText(raw);
  if (!s) return null;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function withinWindow(publishedAt) {
  if (publishedAt === null) return true; // 解析不出来 -> 保留
  const age = Date.now() - Date.parse(publishedAt);
  if (!Number.isFinite(age)) return true;
  return age <= MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
}

/* ============================== 闸门 ============================== */

class FetchGate {
  constructor(maxRequests, gapMs) {
    this.max = maxRequests;
    this.gapMs = gapMs;
    this.used = 0;
    this.lastFinishedAt = 0;
    this.blockedHosts = new Set();
  }

  /** 返回 { status, ok, text }；HTTP 错误 / 网络错误一律抛异常。 */
  async fetchText(url, attempt = 1) {
    const u = new URL(url);
    const host = u.hostname;

    if (this.blockedHosts.has(host)) {
      throw new Error(`host ${host} blocked (previous 403/429)`);
    }
    if (this.used >= this.max) {
      throw new Error(`request budget exhausted (max ${this.max})`);
    }

    const wait = this.gapMs - (Date.now() - this.lastFinishedAt);
    if (wait > 0) await sleep(wait);

    this.used += 1;
    let res;
    try {
      res = await fetch(url, {
        redirect: 'follow',
        headers: { 'user-agent': USER_AGENT, accept: ACCEPT_FEED, 'accept-language': 'en-US,en;q=0.9' },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      const status = res.status;
      if (status === 403 || status === 429) this.blockedHosts.add(host);
      const text = await res.text();
      if (status === 403 || status === 429) {
        throw new Error(`HTTP ${status} (rate limited / forbidden) - host ${host} now blocked`);
      }
      if (status >= 500 && attempt < 2) {
        return await this.fetchText(url, attempt + 1);
      }
      if (status < 200 || status >= 300) throw new Error(`HTTP ${status}`);
      return { status, ok: true, text };
    } catch (err) {
      const msg = String((err && err.message) || err);
      const retryable = !/HTTP 4\d\d/.test(msg) && attempt < 2 && this.used < this.max;
      if (retryable && !this.blockedHosts.has(host)) {
        return await this.fetchText(url, attempt + 1);
      }
      throw err;
    } finally {
      this.lastFinishedAt = Date.now();
    }
  }
}

/* ============================== RSS / Atom 解析 ============================== */

function firstTag(block, names) {
  for (const name of names) {
    const re = new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${name}>`, 'i');
    const m = block.match(re);
    if (m) return m[1];
  }
  return null;
}

/** Atom 的自闭合 <link href="…"/>；优先 rel="alternate"。 */
function atomLink(block) {
  let best = null;
  for (const m of block.matchAll(/<link\b([^>]*?)\/?>/gi)) {
    const attrs = m[1];
    const hrefMatch = attrs.match(/\bhref\s*=\s*"([^"]*)"/i) || attrs.match(/\bhref\s*=\s*'([^']*)'/i);
    if (!hrefMatch) continue;
    const href = decodeEntities(hrefMatch[1]).trim();
    if (!href) continue;
    const relMatch = attrs.match(/\brel\s*=\s*"([^"]*)"/i);
    const rel = relMatch ? relMatch[1].toLowerCase() : '';
    if (rel === 'alternate') return href;
    if (best === null) best = href;
  }
  return best;
}

function extractLink(block) {
  const textLink = firstTag(block, ['link']);
  if (textLink) {
    const v = toPlainText(textLink);
    if (/^https?:\/\//i.test(v)) return v;
  }
  const href = atomLink(block);
  if (href) return href;
  if (textLink) {
    const v = toPlainText(textLink);
    if (v) return v;
  }
  return null;
}

function absolutize(href, base) {
  if (!href) return null;
  const s = String(href).trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  if (/^(mailto|javascript|data):/i.test(s)) return null;
  try { return new URL(s, base).toString(); } catch { return null; }
}

export function parseFeedEntries(xml, source) {
  const itemBlocks = [...String(xml).matchAll(/<item\b[\s\S]*?<\/item>/gi)].map((m) => m[0]);
  const entryBlocks = [...String(xml).matchAll(/<entry\b[\s\S]*?<\/entry>/gi)].map((m) => m[0]);
  const blocks = itemBlocks.length > 0 ? itemBlocks : entryBlocks;

  const items = [];
  for (const block of blocks) {
    const title = toPlainText(firstTag(block, ['title']));
    if (!title) continue;

    const url = absolutize(extractLink(block), source.url);
    if (!url) continue;

    const snippetRaw =
      firstTag(block, ['description', 'summary', 'content:encoded', 'content']) || '';
    const snippet = toPlainText(snippetRaw).slice(0, SNIPPET_MAX);

    items.push({
      title,
      url: stripTracking(url),
      publishedAt: parseDate(firstTag(block, ['pubDate', 'published', 'updated', 'dc:date', 'date'])),
      snippet,
      source: source.key,
      domain: source.domain,
      meta: { starsToday: null },
    });
  }
  return items;
}

/* ============================== 各源解析 ============================== */

function detectHackerNewsDomain(text, url) {
  const t = String(text).toLowerCase();
  if (AI_WORDS.some((w) => hasWord(t, w))) return 'ai';
  if (url && /^https?:\/\/(www\.)?github\.com\//i.test(url)) return 'github';
  return 'vibe_coding';
}

export function collectHackerNews(text, opts = {}) {
  const skipTopicGate = opts.skipTopicGate === true;
  const sourceKey = opts.sourceKey || 'hackernews';
  const data = JSON.parse(text);
  if (!data || !Array.isArray(data.hits)) {
    throw new Error('unexpected HN payload: missing hits[]');
  }
  const items = [];
  let dropped = 0;
  for (const hit of data.hits) {
    const title = toPlainText(hit.title || hit.story_title);
    if (!title) continue;
    const external =
      typeof hit.url === 'string' && /^https?:\/\//i.test(hit.url) ? hit.url : null;
    const url = external || `https://news.ycombinator.com/item?id=${hit.objectID}`;
    const body = toPlainText(hit.story_text || hit.comment_text || '');
    const haystack = `${title} ${body}`;
    const t = haystack.toLowerCase();
    const domain = detectHackerNewsDomain(haystack, external);
    // 只有落到 vibe_coding 的才需要过技术闸门（ai / github 自带主题）。
    if (!skipTopicGate && domain === 'vibe_coding' && !HN_TECH_WORDS.some((w) => hasWord(t, w))) {
      dropped++;
      continue;
    }
    items.push({
      title,
      url: stripTracking(url),
      publishedAt: parseDate(hit.created_at),
      snippet: body.slice(0, SNIPPET_MAX),
      source: sourceKey,
      domain,
      meta: { starsToday: null },
    });
  }
  if (dropped) process.stdout.write(`[hn]   filtered ${dropped} off-topic stories\n`);
  return items;
}

export function collectGithubTrending(html) {
  const src = String(html);
  const blocks = [...src.matchAll(/<article\b[^>]*class="[^"]*Box-row[^"]*"[^>]*>([\s\S]*?)<\/article>/gi)]
    .map((m) => m[1]);
  const items = [];
  for (const block of blocks) {
    const h2 = block.match(/<h2[\s\S]*?<\/h2>/i);
    const scope = h2 ? h2[0] : block;
    const a = scope.match(/<a\b[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/i);
    if (!a) continue;
    const href = decodeEntities(a[1]).trim();
    const url = absolutize(href, 'https://github.com/');
    if (!url) continue;
    const title = toPlainText(a[2]).replace(/\s*\/\s*/g, '/').trim();
    if (!title) continue;

    const descMatch = block.match(/<p\b[^>]*class="[^"]*col-9[^"]*"[^>]*>([\s\S]*?)<\/p>/i);
    const description = descMatch ? toPlainText(descMatch[1]) : '';
    const langMatch = block.match(/itemprop="programmingLanguage"[^>]*>([\s\S]*?)<\/span>/i);
    const language = langMatch ? toPlainText(langMatch[1]) : '';
    const starsMatch = block.match(/([\d,]+)\s+stars?\s+today/i);
    const starsToday = starsMatch ? Number.parseInt(starsMatch[1].replace(/,/g, ''), 10) : null;

    const snippetParts = [];
    if (description) snippetParts.push(description);
    if (language) snippetParts.push(`[${language}]`);
    if (Number.isFinite(starsToday)) snippetParts.push(`${starsToday} stars today`);

    items.push({
      title,
      url: stripTracking(url),
      publishedAt: null,
      snippet: snippetParts.join(' ').slice(0, SNIPPET_MAX),
      source: 'github_trending',
      domain: 'github',
      meta: { starsToday: Number.isFinite(starsToday) ? starsToday : null },
    });
  }
  return items;
}

/* ============================== 打分 ============================== */

function scoreSignals(item) {
  const text = `${item.title} ${item.snippet}`.toLowerCase();
  const tags = [];
  for (const w of SIGNALS[item.domain] || []) {
    if (hasWord(text, w)) tags.push(w);
  }
  if (item.domain === 'github' && Number.isFinite(item.meta && item.meta.starsToday)) {
    tags.push('stars');
  }
  return tags;
}

function freshnessBonus(publishedAt) {
  if (!publishedAt) return 1;
  const age = Date.now() - Date.parse(publishedAt);
  if (!Number.isFinite(age)) return 1;
  if (age <= 24 * 60 * 60 * 1000) return 3;
  if (age <= 72 * 60 * 60 * 1000) return 2;
  return 1;
}

function scoreItem(item) {
  const baseline = SOURCE_BASELINE[item.source] != null ? SOURCE_BASELINE[item.source] : 2;
  const tags = scoreSignals(item);
  let score = baseline + freshnessBonus(item.publishedAt) + Math.min(tags.length, 2);
  if (item.domain === 'github' && Number.isFinite(item.meta && item.meta.starsToday)) {
    if (item.meta.starsToday >= 1000) score += 2;
    else if (item.meta.starsToday >= 200) score += 1;
  }
  const clamped = Math.max(0, Math.min(10, score));
  return { score: Math.round(clamped), tags };
}

/* ============================== 去重 ============================== */

export function dedupeItems(items) {
  const order = items.slice().sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const pa = SOURCE_BASELINE[a.source] || 0;
    const pb = SOURCE_BASELINE[b.source] || 0;
    if (pb !== pa) return pb - pa;
    return compareDateDesc(a.publishedAt, b.publishedAt);
  });

  const seenUrl = new Set();
  const seenTitle = new Set();
  const kept = [];
  for (const it of order) {
    const urlKey = normalizeUrlKey(it.url);
    const titleKey = titleFingerprint(it.title);
    if (urlKey && seenUrl.has(urlKey)) continue;
    if (titleKey && seenTitle.has(titleKey)) continue;
    if (urlKey) seenUrl.add(urlKey);
    if (titleKey) seenTitle.add(titleKey);
    kept.push(it);
  }
  return kept;
}

function compareDateDesc(a, b) {
  if (a === b) return 0;
  if (a === null || a === undefined) return 1; // null 最后
  if (b === null || b === undefined) return -1;
  return a < b ? 1 : -1; // ISO 字符串字典序 == 时间序
}

function sortFinal(a, b) {
  if (b.score !== a.score) return b.score - a.score;
  const d = compareDateDesc(a.publishedAt, b.publishedAt);
  if (d !== 0) return d;
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/* ============================== 组装（纯函数，无 IO） ============================== */

/** 打分 -> 去重 -> 排序 -> 投影成跨端契约 JSON。不联网、不写盘，便于离线复算。 */
export function buildDocument(collected, sourceReports) {
  const raw = collected.length;
  for (const it of collected) {
    const s = scoreItem(it);
    it.score = s.score;
    it.tags = s.tags;
    it.id = makeId(it.url); // sortFinal 的 id asc 兜底排序需要它先存在
  }
  const dedupedItems = dedupeItems(collected);
  dedupedItems.sort(sortFinal);

  const items = dedupedItems.map((it) => ({
    id: it.id,
    domain: it.domain,
    source: it.source,
    title: it.title,
    url: it.url,
    publishedAt: it.publishedAt,
    snippet: it.snippet,
    score: it.score,
    tags: it.tags,
  }));

  return {
    schemaVersion: SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    counts: { raw, deduped: dedupedItems.length, kept: items.length },
    sources: sourceReports,
    items,
  };
}

/* ============================== 主流程 ============================== */

function printHelp() {
  process.stdout.write(
    [
      'Usage: node collect.mjs [--out <path>] [--only <sourceKey,...>]',
      '',
      `Default --out: ${DEFAULT_OUT} (relative to cwd)`,
      `Sources: ${SOURCES.map((s) => s.key).join(', ')}`,
      '',
    ].join('\n'),
  );
}

function parseArgs(argv) {
  const opts = { out: DEFAULT_OUT, only: null, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') { opts.help = true; return opts; }
    if (a === '--out') { opts.out = argv[i + 1]; i += 1; continue; }
    if (a.startsWith('--out=')) { opts.out = a.slice('--out='.length); continue; }
    if (a === '--only') { opts.only = argv[i + 1]; i += 1; continue; }
    if (a.startsWith('--only=')) { opts.only = a.slice('--only='.length); continue; }
    throw new Error(`unknown argument: ${a}`);
  }
  if (opts.out == null || String(opts.out).trim() === '') {
    throw new Error('--out requires a non-empty path');
  }
  if (opts.only != null) {
    opts.only = String(opts.only).split(',').map((s) => s.trim()).filter(Boolean);
    const known = new Set(SOURCES.map((s) => s.key));
    const unknown = opts.only.filter((k) => !known.has(k));
    if (unknown.length > 0) throw new Error(`unknown source key(s): ${unknown.join(', ')}`);
  }
  return opts;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return 0; }

  const outPath = resolve(process.cwd(), opts.out);
  const selected = opts.only ? SOURCES.filter((s) => opts.only.includes(s.key)) : SOURCES;
  const gate = new FetchGate(MAX_REQUESTS, MIN_GAP_MS);

  const sourceReports = [];
  const collected = [];
  let anyOk = false;

  for (const source of selected) {
    const t0 = Date.now();
    const report = { key: source.key, ok: false, status: null, count: 0, error: null, ms: 0 };
    try {
      // url 允许是函数（hackernews 的 Algolia 查询要按"现在"算时间窗）。
      const targetUrl = typeof source.url === 'function' ? source.url() : source.url;
      const res = await gate.fetchText(targetUrl);
      report.status = res.status;

      let parsed;
      if (source.kind === 'hn') {
        parsed = collectHackerNews(res.text, {
          skipTopicGate: Boolean(source.forceDomain),
          sourceKey: source.key,
        });
      } else if (source.kind === 'github') {
        parsed = collectGithubTrending(res.text);
      } else {
        const looksLikeFeed = /<rss\b|<feed\b|<channel\b/i.test(res.text);
        if (!looksLikeFeed) throw new Error('response is not an RSS/Atom document');
        parsed = parseFeedEntries(res.text, source);
      }
      // forceDomain：Show HN 这类源整体归一个 domain，不让启发式分类改归属。
      if (source.forceDomain) parsed = parsed.map((it) => ({ ...it, domain: source.forceDomain }));

      const fresh = parsed.filter((it) => withinWindow(it.publishedAt));
      report.count = fresh.length;
      report.ok = true;
      anyOk = true;
      collected.push(...fresh);
      process.stdout.write(
        `[ok]   ${source.key.padEnd(22)} status=${report.status} items=${fresh.length}\n`,
      );
    } catch (err) {
      report.ok = false;
      report.count = 0;
      report.error = String((err && err.message) || err);
      process.stdout.write(
        `[fail] ${source.key.padEnd(22)} status=${report.status} error=${report.error}\n`,
      );
    }
    report.ms = Date.now() - t0;
    sourceReports.push(report);
  }

  const doc = buildDocument(collected, sourceReports);

  await mkdir(dirname(outPath), { recursive: true });
  await writeFile(outPath, `${JSON.stringify(doc, null, 2)}\n`, 'utf8');

  process.stdout.write(
    `\nwrote ${doc.items.length} items -> ${outPath}\n` +
    `counts raw=${doc.counts.raw} deduped=${doc.counts.deduped} kept=${doc.counts.kept}\n` +
    `requests used=${gate.used}/${MAX_REQUESTS} blockedHosts=[${[...gate.blockedHosts].join(', ')}]\n`,
  );

  return anyOk ? 0 : 1;
}

// 直接执行时才跑主流程；被 import 时（离线复算 buildDocument）不产生副作用。
const normPath = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
const invokedDirectly =
  Boolean(process.argv[1]) &&
  normPath(resolve(process.argv[1])) === normPath(fileURLToPath(import.meta.url));

if (invokedDirectly) {
  main()
    .then((code) => { process.exitCode = code; })
    .catch((err) => {
      process.stderr.write(`fatal: ${String((err && err.stack) || err)}\n`);
      process.exitCode = 1;
    });
}
