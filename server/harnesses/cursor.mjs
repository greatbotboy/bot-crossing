/**
 * Harness adapter: Cursor (IDE agent / plan composers).
 *
 * Cursor keeps thread metadata in SQLite (`composerHeaders` inside state.vscdb) and the
 * readable transcript as JSONL under `~/.cursor/projects/<slug>/agent-transcripts/`. There
 * is no documented deep link that focuses an existing composer, so open/new hand the work
 * to the `cursor` CLI via `{ spawn }` — see `server/api.mjs` `launchResult`.
 *
 * Everything that knows Cursor's on-disk layout lives in this one module.
 */
import fsp from 'node:fs/promises'
import path from 'node:path'
import os from 'node:os'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { exists, jsonLines, num, readHead, readTail } from '../lib/fsutil.mjs'

const execFileAsync = promisify(execFile)
const HOME = os.homedir()

const PROJECTS = path.join(HOME, '.cursor', 'projects')

/** Linux / portable config root; macOS keeps the same layout under Application Support. */
const STATE_CANDIDATES = [
  path.join(HOME, '.config', 'Cursor', 'User', 'globalStorage', 'state.vscdb'),
  path.join(HOME, 'Library', 'Application Support', 'Cursor', 'User', 'globalStorage', 'state.vscdb'),
]

const SEARCH_CANDIDATES = [
  path.join(HOME, '.config', 'Cursor', 'User', 'globalStorage', 'conversation-search.db'),
  path.join(
    HOME,
    'Library',
    'Application Support',
    'Cursor',
    'User',
    'globalStorage',
    'conversation-search.db'
  ),
]

const HEAD_BYTES = 192 * 1024
const TAIL_BYTES = 64 * 1024
const ACTIVE_WINDOW_MS = 30 * 60 * 1000
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const MODES = new Set(['agent', 'plan'])

let cachedStateDb = null
let cachedSearchDb = null

async function resolveStateDb() {
  if (cachedStateDb && (await exists(cachedStateDb))) return cachedStateDb
  for (const p of STATE_CANDIDATES) {
    if (await exists(p)) {
      cachedStateDb = p
      return p
    }
  }
  return null
}

async function resolveSearchDb() {
  if (cachedSearchDb && (await exists(cachedSearchDb))) return cachedSearchDb
  for (const p of SEARCH_CANDIDATES) {
    if (await exists(p)) {
      cachedSearchDb = p
      return p
    }
  }
  return null
}

/** Path → Cursor project slug: strip leading `/`, replace `/` and `_` with `-`. */
function projectSlug(projectPath) {
  if (!projectPath || typeof projectPath !== 'string') return ''
  return projectPath.replace(/^\/+/, '').replace(/[/_]/g, '-')
}

function firstText(content) {
  if (typeof content === 'string') return content
  if (Array.isArray(content)) {
    for (const part of content) {
      if (typeof part === 'string') return part
      if (part && part.type === 'text' && typeof part.text === 'string') return part.text
    }
  }
  return ''
}

/** Pull the human prompt out of Cursor's `<user_query>` / timestamp wrappers. */
function cleanPrompt(s) {
  const raw = String(s)
  const query = raw.match(/<user_query>\s*([\s\S]*?)\s*<\/user_query>/i)
  const text = (query ? query[1] : raw)
    .replace(/<([a-z][\w-]*)(?:\s[^>]*)?>[\s\S]*?<\/\1>/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim()
  return text
}

/**
 * SQLite without a new npm dep: prefer Node's built-in `node:sqlite`, fall back to python3
 * (stdlib sqlite3) when the built-in is missing — common on Node 20.
 */
let nodeSqlite = null
async function loadNodeSqlite() {
  if (nodeSqlite !== null) return nodeSqlite
  try {
    nodeSqlite = await import('node:sqlite')
  } catch {
    nodeSqlite = false
  }
  return nodeSqlite
}

function pyQuote(s) {
  return JSON.stringify(String(s))
}

async function sqliteAll(dbPath, sql, params = []) {
  const mod = await loadNodeSqlite()
  if (mod) {
    const db = new mod.DatabaseSync(dbPath, { readOnly: true })
    try {
      db.exec('PRAGMA busy_timeout = 3000')
      return db.prepare(sql).all(...params)
    } finally {
      db.close()
    }
  }

  const script = `
import json, sqlite3, sys
db = sqlite3.connect(f"file:{${pyQuote(dbPath)}}?mode=ro", uri=True)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout = 3000")
params = json.loads(${pyQuote(JSON.stringify(params))})
rows = db.execute(${pyQuote(sql)}, params).fetchall()
print(json.dumps([dict(r) for r in rows]))
`
  const { stdout } = await execFileAsync('python3', ['-c', script], { maxBuffer: 32 * 1024 * 1024 })
  return JSON.parse(stdout || '[]')
}

async function sqliteRun(dbPath, statements) {
  const mod = await loadNodeSqlite()
  if (mod) {
    const db = new mod.DatabaseSync(dbPath)
    try {
      db.exec('PRAGMA busy_timeout = 5000')
      db.exec('BEGIN')
      try {
        for (const { sql, params = [] } of statements) {
          db.prepare(sql).run(...params)
        }
        db.exec('COMMIT')
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    } finally {
      db.close()
    }
    return
  }

  const script = `
import json, sqlite3
db = sqlite3.connect(${pyQuote(dbPath)})
db.execute("PRAGMA busy_timeout = 5000")
stmts = json.loads(${pyQuote(JSON.stringify(statements))})
try:
    db.execute("BEGIN")
    for s in stmts:
        db.execute(s["sql"], s.get("params") or [])
    db.commit()
except Exception:
    db.rollback()
    raise
finally:
    db.close()
`
  await execFileAsync('python3', ['-c', script], { maxBuffer: 8 * 1024 * 1024 })
}

async function readComposerHeaders(dbPath) {
  return sqliteAll(
    dbPath,
    `SELECT composerId, workspaceId, createdAt, lastUpdatedAt, isArchived, isSubagent, value
     FROM composerHeaders`
  )
}

const metaCache = new Map()

async function transcriptMeta(file, mtime, size) {
  const cached = metaCache.get(file)
  if (cached && cached.mtime === mtime) return cached.meta

  const meta = { firstPrompt: '', hasError: false }
  try {
    const head = jsonLines(await readHead(file, HEAD_BYTES))
    for (const r of head) {
      if (meta.firstPrompt) break
      if (r.role === 'user' && r.message) {
        const text = cleanPrompt(firstText(r.message.content))
        if (text) meta.firstPrompt = text
      }
    }
  } catch {
    /* mid-write or missing — leave defaults */
  }

  try {
    const tail = jsonLines(await readTail(file, TAIL_BYTES))
    for (let i = tail.length - 1; i >= 0; i--) {
      const r = tail[i]
      if (r && r.type === 'turn_ended') {
        meta.hasError = r.status === 'error'
        break
      }
    }
  } catch {
    /* ignore */
  }

  metaCache.set(file, { mtime, meta })
  return meta
}

function workspacePathOf(value, workspaceId) {
  const uri = value?.workspaceIdentifier?.uri
  if (uri && typeof uri.fsPath === 'string' && uri.fsPath.startsWith('/')) return uri.fsPath
  const repos = value?.trackedGitRepos
  if (Array.isArray(repos)) {
    for (const r of repos) {
      if (r && typeof r.repoPath === 'string' && r.repoPath.startsWith('/')) return r.repoPath
    }
  }
  // Numeric / empty-window workspace ids are not real folders.
  if (workspaceId && workspaceId.includes('/') && workspaceId.startsWith('/')) return workspaceId
  return ''
}

async function resolveTranscript(composerId, projectPath) {
  if (!UUID.test(composerId)) return null
  const slug = projectSlug(projectPath)
  const candidates = []
  if (slug) {
    candidates.push(path.join(PROJECTS, slug, 'agent-transcripts', composerId, `${composerId}.jsonl`))
  }
  // Fallback: walk project dirs looking for this composer (slug edge cases).
  if (!(await exists(candidates[0] || ''))) {
    try {
      const projects = await fsp.readdir(PROJECTS, { withFileTypes: true })
      for (const ent of projects) {
        if (!ent.isDirectory() || ent.name.startsWith('.')) continue
        candidates.push(
          path.join(PROJECTS, ent.name, 'agent-transcripts', composerId, `${composerId}.jsonl`)
        )
      }
    } catch {
      /* no projects dir */
    }
  }

  for (const file of candidates) {
    try {
      const stat = await fsp.stat(file)
      if (stat.isFile()) return { file, size: stat.size, mtime: stat.mtimeMs }
    } catch {
      /* try next */
    }
  }
  return null
}

async function scanThreads() {
  const dbPath = await resolveStateDb()
  if (!dbPath) return []

  let rows
  try {
    rows = await readComposerHeaders(dbPath)
  } catch (err) {
    console.warn('bot-crossing: cursor harness failed to read composerHeaders —', err?.message || err)
    return []
  }

  const now = Date.now()
  const threads = []

  for (const row of rows) {
    const composerId = row.composerId || ''
    if (!UUID.test(composerId)) continue
    if (row.isSubagent) continue

    let value = {}
    try {
      value = typeof row.value === 'string' ? JSON.parse(row.value) : row.value || {}
    } catch {
      continue
    }

    if (value.isDraft || value.isEphemeral) continue
    if (!MODES.has(value.unifiedMode)) continue

    const projectPath = workspacePathOf(value, row.workspaceId)
    const project = projectPath ? path.basename(projectPath) : 'unknown'
    const transcript = await resolveTranscript(composerId, projectPath)
    const meta = transcript
      ? await transcriptMeta(transcript.file, transcript.mtime, transcript.size)
      : { firstPrompt: '', hasError: false }

    const createdAt = num(row.createdAt) || num(value.createdAt)
    const lastActivityAt =
      num(row.lastUpdatedAt) || num(value.lastUpdatedAt) || num(transcript?.mtime) || createdAt
    const unfinishedAt = num(value.unfinishedRunAt)
    const running =
      unfinishedAt > 0 && now - Math.max(lastActivityAt, unfinishedAt) < ACTIVE_WINDOW_MS

    const title =
      (value.name && String(value.name).trim()) ||
      (value.subtitle && String(value.subtitle).trim()) ||
      meta.firstPrompt ||
      'Untitled thread'

    const gitBranch =
      (Array.isArray(value.trackedGitRepos) &&
        value.trackedGitRepos.find((r) => r?.branch)?.branch) ||
      ''

    let projectPathExists = false
    if (projectPath) {
      try {
        projectPathExists = (await fsp.stat(projectPath)).isDirectory()
      } catch {
        projectPathExists = false
      }
    }

    threads.push({
      id: `cursor:${composerId}`,
      title,
      preview: meta.firstPrompt ? meta.firstPrompt.slice(0, 240) : '',
      project,
      projectPath,
      worktree: '',
      cwd: projectPath,
      gitBranch: gitBranch && gitBranch !== 'HEAD' ? gitBranch : '',
      model: '',
      effort: '',
      createdAt,
      lastActivityAt,
      lastFocusedAt: 0,
      running,
      unread: value.hasUnreadMessages === true,
      hasError: meta.hasError,
      starred: false,
      routine: '',
      prState: '',
      archived: Boolean(row.isArchived) || value.isArchived === true,
      sizeBytes: transcript?.size || 0,
      source: value.unifiedMode || 'agent',
      hasTranscript: Boolean(transcript),
      canOpen: projectPathExists,
      canArchive: true,
      ref: { composerId, projectPath },
    })
  }

  return threads
}

/** Best-effort: point Glass at this composer before focusing the workspace window. */
async function selectComposer(composerId) {
  const dbPath = await resolveStateDb()
  if (!dbPath || !UUID.test(composerId)) return
  try {
    await sqliteRun(dbPath, [
      {
        sql: `INSERT INTO ItemTable (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        params: ['cursor/glass.selectedAgent', composerId],
      },
    ])
  } catch {
    /* Cursor may hold the DB — open still focuses the folder */
  }
}

function openThread(ref) {
  const composerId = ref?.composerId || ''
  const projectPath = ref?.projectPath || ''
  if (!UUID.test(composerId)) {
    return { ok: false, error: 'No openable Cursor composer id on that thread' }
  }
  if (!projectPath || !projectPath.startsWith('/')) {
    return { ok: false, error: 'No workspace folder for that Cursor thread' }
  }
  // Fire-and-forget select; open does not wait on the write.
  selectComposer(composerId).catch(() => {})
  return { ok: true, spawn: { command: 'cursor', args: ['-r', projectPath] } }
}

function newSession(dir) {
  if (!dir || !dir.startsWith('/')) {
    return { ok: false, error: 'Need an absolute folder for a new Cursor session' }
  }
  return { ok: true, spawn: { command: 'cursor', args: ['-n', dir] } }
}

async function setArchived(ref, archived) {
  const composerId = ref?.composerId || ''
  if (!UUID.test(composerId)) {
    return { ok: false, error: 'No Cursor composer id on that thread' }
  }

  const dbPath = await resolveStateDb()
  if (!dbPath) return { ok: false, error: 'Cursor state database not found' }

  let rows
  try {
    rows = await sqliteAll(dbPath, 'SELECT composerId, value FROM composerHeaders WHERE composerId = ?', [
      composerId,
    ])
  } catch (err) {
    return { ok: false, error: String(err?.message || err) }
  }

  const row = rows[0]
  if (!row || row.composerId !== composerId) {
    return { ok: false, error: 'No Cursor session record for that thread' }
  }

  let value = {}
  try {
    value = typeof row.value === 'string' ? JSON.parse(row.value) : row.value || {}
  } catch {
    return { ok: false, error: 'Session record is unreadable' }
  }
  if (value.composerId && value.composerId !== composerId) {
    return { ok: false, error: 'Session record did not look like the expected session' }
  }

  value.isArchived = Boolean(archived)
  const flag = archived ? 1 : 0

  try {
    await sqliteRun(dbPath, [
      {
        sql: 'UPDATE composerHeaders SET isArchived = ?, value = ? WHERE composerId = ?',
        params: [flag, JSON.stringify(value), composerId],
      },
    ])
  } catch (err) {
    return { ok: false, error: String(err?.message || err) }
  }

  const searchDb = await resolveSearchDb()
  if (searchDb) {
    try {
      await sqliteRun(searchDb, [
        {
          sql: 'UPDATE conversations SET is_archived = ? WHERE id = ?',
          params: [flag, composerId],
        },
      ])
    } catch {
      /* search index is optional */
    }
  }

  metaCache.clear()
  return { ok: true, archived: Boolean(archived) }
}

/**
 * When the Cursor app last launched — used for archivePending reconciliation. Helper
 * processes carry a `--type=` flag; only the main process counts.
 */
let appStartCache = { at: 0, checkedAt: 0 }
async function appStartedAt() {
  const now = Date.now()
  if (now - appStartCache.checkedAt < 15000) return appStartCache.at

  let started = 0
  try {
    const { stdout } = await execFileAsync('ps', ['-axo', 'pid=,lstart=,command='], {
      maxBuffer: 8 * 1024 * 1024,
    })
    for (const line of stdout.split('\n')) {
      const m = line.match(/^\s*\d+\s+(\w{3} \w{3}\s+\d+ \d{2}:\d{2}:\d{2} \d{4})\s+(\S.*)$/)
      if (!m) continue
      const [, when, command] = m
      if (command.includes('--type=')) continue
      const isCursor =
        command.includes('/Cursor.app/Contents/MacOS/Cursor') ||
        /(^|[/\s])cursor(\s|$)/i.test(command) ||
        command.includes('/usr/share/cursor/') ||
        command.includes('/cursor/cursor')
      if (!isCursor) continue
      // Prefer the real binary over wrapper scripts when several match.
      if (command.includes('crashpad') || command.includes('chrome_crashpad')) continue
      const parsed = Date.parse(when)
      if (!Number.isNaN(parsed)) {
        started = parsed
        // Keep scanning for a more specific Cursor.app match on macOS.
        if (command.includes('/Cursor.app/Contents/MacOS/Cursor')) break
      }
    }
  } catch {
    /* ps unavailable */
  }
  appStartCache = { at: started, checkedAt: now }
  return started
}

export default {
  id: 'cursor',
  name: 'Cursor',
  detect: async () => Boolean((await resolveStateDb()) || (await exists(PROJECTS))),
  scanThreads,
  openThread,
  newSession,
  setArchived,
  appStartedAt,
  paths: { PROJECTS, STATE_CANDIDATES },
}
