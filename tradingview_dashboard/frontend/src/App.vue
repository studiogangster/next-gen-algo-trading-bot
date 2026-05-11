<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { createChart, createSeriesMarkers, CandlestickSeries, HistogramSeries, LineSeries } from 'lightweight-charts'

const instrumentOptions = [
  { label: 'NIFTY 50', value: 256265, symbol: 'NIFTY', exchange: 'NSE' },
  { label: 'BANKNIFTY', value: 260105, symbol: 'BANKNIFTY', exchange: 'NSE' },
  { label: 'RELIANCE', value: 2885, symbol: 'RELIANCE', exchange: 'NSE' },
  { label: 'NIFTYBEES', value: 2707457, symbol: 'NIFTYBEES', exchange: 'NSE' },
]

const timeframeOptions = [
  { label: '1m', value: '1m' },
  { label: '5m', value: '5m' },
  { label: '15m', value: '15m' },
  { label: '1h', value: '1h' },
]

const timeframeSeconds = {
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '1h': 3600,
}

function sortTimeframesByGranularity(frames) {
  return [...frames].sort((a, b) => (timeframeSeconds[a] || Number.MAX_SAFE_INTEGER) - (timeframeSeconds[b] || Number.MAX_SAFE_INTEGER))
}

function getPrimaryTimeframe(frames) {
  const sorted = sortTimeframesByGranularity(frames)
  return sorted[0] || '1m'
}

const apiBase = import.meta.env.VITE_API_BASE || ''
const REALTIME_POLL_MS = 2000
const INDICATOR_POLL_MS = 5000
const ORDER_BOOK_POLL_MS = 2000
const HISTORY_PAGE_LIMIT = 2000
const INDICATOR_STALE_REFRESH_MS = 60000

const instrumentToken = ref(256265)
const selectedTimeframes = ref(['1m'])
const useFullRange = ref(true)
const rangeFromInput = ref('')
const rangeToInput = ref('')
const chartStates = ref([])
const signalStrategies = ref([])
const activeSignalStrategy = ref('')
const indicatorVisibility = ref({})
const isQuickIndicatorMenuOpen = ref(false)
const signalOutcomeScope = ref('today')
const selectedSignalOutcomeDay = ref('')

const uiError = ref('')
const connectionState = ref('idle')
const lastSyncTs = ref(null)
const isBootstrapped = ref(false)
const isReloading = ref(false)
const isControlDrawerOpen = ref(false)
const isInsightDrawerOpen = ref(false)
const isInsightDrawerWide = ref(true)
const selectedUniverseInstrument = ref(null)

const universeMeta = ref(null)
const universeIndexes = ref([])
const filteredIndexes = computed(() => {
  const query = indexSearchQuery.value.trim().toUpperCase()
  if (!query) return universeIndexes.value
  return universeIndexes.value.filter((item) => item.index_name.toUpperCase().includes(query))
})
const universeInstrumentRows = ref([])
const universeInstrumentTotal = ref(0)
const universeInstrumentCount = ref(0)
const universeConstituents = ref([])

const universePanelTab = ref('instruments')
const selectedIndexName = ref('')
const indexSearchQuery = ref('')
const instrumentSearch = ref('')
const instrumentExchange = ref('NSE')
const instrumentType = ref('EQ')
const instrumentSegment = ref('')
const instrumentLimit = ref(25)
const instrumentOffset = ref(0)

const universeMetaLoading = ref(false)
const universeIndexesLoading = ref(false)
const universeInstrumentsLoading = ref(false)
const universeConstituentsLoading = ref(false)
const universeError = ref('')
const lastApiResponseAtMs = ref(null)
const lastApiLatencyMs = ref(null)
const orderBookMode = ref('users-with-order-books')
const orderBookLimit = ref(25)
const orderBookOffset = ref(0)
const orderBookRows = ref([])
const orderBookUsersTotal = ref(0)
const orderBookLoading = ref(false)
const orderBookError = ref('')
const lastOrderBookResponseAtMs = ref(null)
const localTradeEvents = ref([])
const selectedOrderBookUsers = ref([])
const orderBookUserSubTabs = ref({})
const showOrderBookStats = ref(false)
const showInsightSnapshot = ref(false)
const showInsightSignals = ref(false)
const activeTradeContract = ref(null)

let realtimeTimer = null
let indicatorTimer = null
let orderBookTimer = null
let orderBookPollInFlight = false
let syncingLogicalRange = false
let syncingTimeRange = false
let logicalSyncRaf = null
let timeSyncRaf = null
let pendingLogicalSync = null
let pendingTimeSync = null
let reloadQueued = false
let reloadDebounceTimer = null
let latestLoadRequestId = 0

const activeInstrument = computed(() => {
  const fromPreset = instrumentOptions.find((option) => option.value === instrumentToken.value)
  if (fromPreset) return fromPreset

  if (selectedUniverseInstrument.value) {
    const symbol = selectedUniverseInstrument.value.tradingsymbol || selectedUniverseInstrument.value.symbol || 'CUSTOM'
    return {
      label: selectedUniverseInstrument.value.name || symbol,
      value: Number(selectedUniverseInstrument.value.instrument_token),
      symbol,
      exchange: selectedUniverseInstrument.value.exchange || 'NSE',
    }
  }

  return instrumentOptions[0]
})

const selectedIndexDetails = computed(
  () => universeIndexes.value.find((item) => item.index_name === selectedIndexName.value) || null,
)

const instrumentPageFrom = computed(() => {
  if (universeInstrumentTotal.value === 0) return 0
  return instrumentOffset.value + 1
})

const instrumentPageTo = computed(() =>
  Math.min(instrumentOffset.value + instrumentLimit.value, universeInstrumentTotal.value),
)

const marketPhase = computed(() => {
  const now = new Date()
  const istNow = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }))
  const hours = istNow.getHours()
  const minutes = istNow.getMinutes()
  const total = hours * 60 + minutes

  if (total < 555 || total > 930) return { label: 'Closed', tone: 'muted' }
  if (total < 570) return { label: 'Pre-Open', tone: 'warn' }
  return { label: 'Live Market', tone: 'good' }
})

const layoutStyle = computed(() => {
  const count = chartStates.value.length || 1
  if (count === 1) return { gridTemplateColumns: '1fr', gridTemplateRows: '1fr' }
  if (count === 2) return { gridTemplateColumns: '1fr 1fr', gridTemplateRows: '1fr' }
  return { gridTemplateColumns: '1fr 1fr', gridTemplateRows: '1fr 1fr' }
})

const summaryCards = computed(() => {
  return chartStates.value.map((state) => {
    const latest = state.candles[state.candles.length - 1]
    const previous = state.candles[state.candles.length - 2]

    const close = latest?.close ?? null
    const prevClose = previous?.close ?? close
    const change = close !== null && prevClose !== null ? close - prevClose : 0
    const pct = close && prevClose ? (change / prevClose) * 100 : 0

    return {
      timeframe: state.timeframe,
      close,
      high: latest?.high ?? null,
      low: latest?.low ?? null,
      volume: latest?.volume ?? null,
      change,
      pct,
      tone: change >= 0 ? 'good' : 'bad',
    }
  })
})

const signalTimeline = computed(() => {
  const out = []
  for (const state of chartStates.value) {
    const signals = Array.isArray(state.signals) ? state.signals : []
    for (const signal of signals) {
      const ts = normalizeTimestamp(signal.timestamp ?? signal.epoch)
      if (!ts) continue
      const action = normalizeSignalAction(signal.action)
      out.push({
        ...signal,
        action,
        timeframe: signal.timeframe || state.timeframe,
        timestamp: ts,
      })
    }
  }
  out.sort((a, b) => b.timestamp - a.timestamp)
  return out.slice(0, 160)
})

const validSignalStrategies = computed(() => signalStrategies.value.filter((item) => item.valid))
const tradeContractLabel = computed(() => {
  const tc = activeTradeContract.value
  if (!tc || typeof tc !== 'object') return 'SPOT (default)'
  if (tc.mode === 'FUT' && tc.tradingsymbol) {
    const pref = tc.future_preference ? ` ${String(tc.future_preference).toUpperCase()}` : ''
    return `${tc.tradingsymbol}${pref}`
  }
  if (tc.mode === 'SPOT') {
    return tc.tradingsymbol ? `${tc.tradingsymbol} (SPOT)` : 'SPOT'
  }
  if (tc.mode === 'FUT' && !tc.tradingsymbol) return 'FUT unresolved'
  return 'SPOT (default)'
})
const tradeContractUnresolved = computed(() => {
  const tc = activeTradeContract.value
  return Boolean(tc && tc.mode === 'FUT' && !tc.tradingsymbol)
})
const indicatorCatalog = computed(() => {
  const byName = new Map()
  for (const state of chartStates.value) {
    const indicators = Array.isArray(state?.indicators) ? state.indicators : []
    for (const indicator of indicators) {
      const name = normalizeIndicatorName(indicator?.name)
      if (!name) continue
      if (!byName.has(name)) {
        byName.set(name, {
          name,
          timeframes: new Set(),
          columns: new Set(),
        })
      }
      const item = byName.get(name)
      item.timeframes.add(state.timeframe)
      for (const col of indicator?.columns || []) {
        item.columns.add(String(col))
      }
    }
  }

  return [...byName.values()]
    .map((item) => ({
      name: item.name,
      timeframes: [...item.timeframes].sort(),
      columns: [...item.columns].sort(),
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
})
const enabledIndicatorCount = computed(() =>
  indicatorCatalog.value.filter((item) => isIndicatorEnabled(item.name)).length,
)

const signalOutcomePairs = computed(() => {
  const asc = [...signalTimeline.value].sort((a, b) => a.timestamp - b.timestamp)
  const openByStream = new Map()
  const outcomes = []

  for (const signal of asc) {
    const action = normalizeSignalAction(signal.action)
    if (action !== 'BUY' && action !== 'SELL' && action !== 'EXIT') continue

    const key = `${signal.timeframe || ''}|${signal.strategy || 'yaml_rule'}`
    const open = openByStream.get(key)
    const openAction = normalizeSignalAction(open?.action)

    const closeOpenSignal = (entrySignal, exitSignal, exitAction) => {
      const entryAction = normalizeSignalAction(entrySignal?.action)
      const entryPrice = Number(entrySignal?.price)
      const exitPrice = Number(exitSignal?.price)
      const hasPrices = Number.isFinite(entryPrice) && Number.isFinite(exitPrice)
      let pnl = null

      if (hasPrices) {
        if (entryAction === 'BUY') pnl = exitPrice - entryPrice
        else if (entryAction === 'SELL') pnl = entryPrice - exitPrice
      }

      outcomes.push({
        stream: key,
        timeframe: exitSignal.timeframe || entrySignal.timeframe,
        strategy: exitSignal.strategy || entrySignal.strategy || 'yaml_rule',
        side: entryAction === 'BUY' ? 'LONG' : 'SHORT',
        entryAction,
        entryTime: entrySignal.timestamp,
        entryPrice: hasPrices ? entryPrice : null,
        exitAction,
        exitTime: exitSignal.timestamp,
        exitPrice: hasPrices ? exitPrice : null,
        pnl,
        result: pnl === null ? 'na' : pnl >= 0 ? 'win' : 'loss',
      })
    }

    if (action === 'EXIT') {
      if (open && (openAction === 'BUY' || openAction === 'SELL')) {
        closeOpenSignal(open, signal, 'EXIT')
      }
      openByStream.delete(key)
      continue
    }

    if (!open) {
      openByStream.set(key, signal)
      continue
    }

    if (openAction === action) {
      openByStream.set(key, signal)
      continue
    }

    closeOpenSignal(open, signal, action)
    openByStream.set(key, signal)
  }

  outcomes.sort((a, b) => b.exitTime - a.exitTime)
  return outcomes.slice(0, 80)
})

const availableSignalOutcomeDays = computed(() => {
  const unique = new Set()
  for (const item of signalOutcomePairs.value) {
    unique.add(toISTDateKey(item.exitTime))
  }
  return [...unique].filter(Boolean).sort((a, b) => (a < b ? 1 : -1))
})

const scopedSignalOutcomePairs = computed(() => {
  const items = signalOutcomePairs.value
  if (signalOutcomeScope.value === 'window') return items
  if (signalOutcomeScope.value === 'day' && selectedSignalOutcomeDay.value) {
    return items.filter((item) => toISTDateKey(item.exitTime) === selectedSignalOutcomeDay.value)
  }
  const todayKey = toISTDateKey(Math.floor(Date.now() / 1000))
  return items.filter((item) => toISTDateKey(item.exitTime) === todayKey)
})

const recentSignalOutcomes = computed(() => scopedSignalOutcomePairs.value.slice(0, 12))

const signalOutcomeSummary = computed(() => {
  const items = scopedSignalOutcomePairs.value
  if (items.length === 0) {
    return { total: 0, wins: 0, losses: 0, winRate: null, netPnl: null }
  }
  let wins = 0
  let losses = 0
  let netPnl = 0
  let pnlCount = 0
  for (const item of items) {
    if (item.result === 'win') wins += 1
    if (item.result === 'loss') losses += 1
    if (item.pnl !== null && item.pnl !== undefined) {
      const num = Number(item.pnl)
      if (Number.isFinite(num)) {
        netPnl += num
        pnlCount += 1
      }
    }
  }
  const winRate = items.length > 0 ? (wins / items.length) * 100 : null
  return {
    total: items.length,
    wins,
    losses,
    winRate,
    netPnl: pnlCount > 0 ? netPnl : null,
  }
})

const signalOutcomeDailySummary = computed(() => {
  const byDay = new Map()
  for (const item of signalOutcomePairs.value) {
    const dayKey = toISTDateKey(item.exitTime)
    if (!dayKey) continue
    if (!byDay.has(dayKey)) {
      byDay.set(dayKey, {
        dayKey,
        pairs: 0,
        wins: 0,
        losses: 0,
        winRate: null,
        netPnl: null,
      })
    }
    const row = byDay.get(dayKey)
    row.pairs += 1
    if (item.result === 'win') row.wins += 1
    if (item.result === 'loss') row.losses += 1
    const pnl = Number(item.pnl)
    if (Number.isFinite(pnl)) {
      row.netPnl = Number(row.netPnl || 0) + pnl
    }
  }

  const out = [...byDay.values()].map((row) => ({
    ...row,
    winRate: row.pairs > 0 ? (row.wins / row.pairs) * 100 : null,
  }))
  out.sort((a, b) => (a.dayKey < b.dayKey ? 1 : -1))
  return out
})

const latestHeartbeat = computed(() => {
  if (!lastSyncTs.value) return 'No sync yet'
  return formatRelativeTime(lastSyncTs.value)
})

const latestCandleEpoch = computed(() => {
  let latest = null
  for (const state of chartStates.value) {
    const lastCandle = state.candles[state.candles.length - 1]
    if (!lastCandle) continue
    const ts = normalizeTimestamp(lastCandle.timestamp)
    if (ts && (latest === null || ts > latest)) latest = ts
  }
  return latest
})

const latestCandleAgeLabel = computed(() => {
  if (!latestCandleEpoch.value) return '--'
  return formatAgePrecise(latestCandleEpoch.value * 1000)
})

const apiResponseAgeLabel = computed(() => {
  if (!lastApiResponseAtMs.value) return '--'
  return formatAgePrecise(lastApiResponseAtMs.value)
})

const apiLatencyLabel = computed(() => {
  if (lastApiLatencyMs.value === null || lastApiLatencyMs.value === undefined) return '--'
  return `${lastApiLatencyMs.value.toFixed(2)}ms`
})

const orderBookAgeLabel = computed(() => {
  if (!lastOrderBookResponseAtMs.value) return '--'
  return formatAgePrecise(lastOrderBookResponseAtMs.value)
})

const orderBookLastSyncEpochMs = computed(() => {
  if (!lastOrderBookResponseAtMs.value) return '--'
  return String(lastOrderBookResponseAtMs.value)
})

const orderBookSyncLedClass = computed(() => {
  if (orderBookLoading.value) return 'footer-led-warn'
  if (orderBookError.value) return 'footer-led-bad'
  if (!lastOrderBookResponseAtMs.value) return 'footer-led-bad'

  const ageSec = (Date.now() - lastOrderBookResponseAtMs.value) / 1000
  if (ageSec <= 3) return 'footer-led-good'
  if (ageSec <= 10) return 'footer-led-warn'
  return 'footer-led-bad'
})

const brokerOrdersCount = computed(() =>
  orderBookRows.value.reduce((sum, row) => sum + (Array.isArray(row.orders) ? row.orders.length : 0), 0),
)

const brokerUniquePositionsCount = computed(() =>
  orderBookRows.value.reduce((sum, row) => sum + getMergedPositions(row).length, 0),
)

const localPendingOrdersCount = computed(
  () => localTradeEvents.value.filter((event) => event.sync_status !== 'broker_synced').length,
)

const orderTerminalOrders = computed(() => {
  const out = []
  for (const row of orderBookRows.value) {
    for (const order of row.orders || []) out.push({ user_id: row.user_id, ...order })
  }
  return out
})

function normalizeOrderStatus(order) {
  return String(order?.status || '').trim().toUpperCase()
}

function isPendingOrderStatus(status) {
  return [
    'OPEN',
    'TRIGGER PENDING',
    'PUT ORDER REQ RECEIVED',
    'VALIDATION PENDING',
    'MODIFY VALIDATION PENDING',
    'MODIFY PENDING',
    'CANCEL PENDING',
    'AMO REQ RECEIVED',
    'AFTER MARKET ORDER REQ RECEIVED',
  ].includes(status)
}

function isCompletedOrderStatus(status) {
  return status === 'COMPLETE'
}

const orderTerminalPendingOrders = computed(() =>
  orderTerminalOrders.value.filter((order) => isPendingOrderStatus(normalizeOrderStatus(order))),
)

const orderTerminalCompletedOrders = computed(() =>
  orderTerminalOrders.value.filter((order) => isCompletedOrderStatus(normalizeOrderStatus(order))),
)

function getPositionQty(pos) {
  const value = Number(pos?.quantity ?? pos?.net_quantity ?? pos?.buy_quantity ?? 0)
  return Number.isFinite(value) ? value : 0
}

const orderTerminalActivePositions = computed(() => {
  const out = []
  for (const row of orderBookRows.value) {
    for (const pos of getMergedPositions(row)) {
      if (Math.abs(getPositionQty(pos)) <= 0) continue
      out.push({ user_id: row.user_id, ...pos })
    }
  }
  return out
})

const orderTerminalNetPositions = computed(() => {
  const out = []
  for (const row of orderBookRows.value) {
    const net = Array.isArray(row.positions?.net) ? row.positions.net : []
    for (const pos of net) out.push({ user_id: row.user_id, ...pos, __source: 'NET' })
  }
  return out
})

const orderTerminalDayPositions = computed(() => {
  const out = []
  for (const row of orderBookRows.value) {
    const day = Array.isArray(row.positions?.day) ? row.positions.day : []
    for (const pos of day) out.push({ user_id: row.user_id, ...pos, __source: 'DAY' })
  }
  return out
})

const activeOrderBookUserRows = computed(() => {
  if (!selectedOrderBookUsers.value.length) return []
  const byUserId = new Map(orderBookRows.value.map((row) => [String(row.user_id), row]))
  return selectedOrderBookUsers.value.map((userId) => byUserId.get(userId)).filter(Boolean)
})

function normalizeTimestamp(ts) {
  if (typeof ts === 'string') {
    const parsedDate = Date.parse(ts)
    if (!Number.isNaN(parsedDate)) return Math.floor(parsedDate / 1000)

    const parsedNumber = Number(ts)
    if (!Number.isNaN(parsedNumber)) return parsedNumber > 1e12 ? Math.floor(parsedNumber / 1000) : Math.floor(parsedNumber)
    return 0
  }

  if (typeof ts === 'number') return ts > 1e12 ? Math.floor(ts / 1000) : Math.floor(ts)
  return 0
}

function formatLocalDateTimeInput(epochSec) {
  const date = new Date(epochSec * 1000)
  const y = date.getFullYear()
  const m = String(date.getMonth() + 1).padStart(2, '0')
  const d = String(date.getDate()).padStart(2, '0')
  const hh = String(date.getHours()).padStart(2, '0')
  const mm = String(date.getMinutes()).padStart(2, '0')
  return `${y}-${m}-${d}T${hh}:${mm}`
}

function parseLocalDateTimeInput(value) {
  if (!value) return null
  const ms = new Date(value).getTime()
  if (Number.isNaN(ms)) return null
  return Math.floor(ms / 1000)
}

function formatLoadingRangeLabel(fromTs, toTs) {
  const from = new Date(fromTs * 1000)
  const to = new Date(toTs * 1000)
  const formatOptions = {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'Asia/Kolkata',
  }
  return `${from.toLocaleTimeString('en-IN', formatOptions)} - ${to.toLocaleTimeString('en-IN', formatOptions)}`
}

function formatIST(epochSec) {
  if (!epochSec) return '--'
  return new Date(epochSec * 1000).toLocaleString('en-IN', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

function toISTDateKey(epochSec) {
  if (!epochSec) return ''
  const parts = new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date(epochSec * 1000))
  const year = parts.find((p) => p.type === 'year')?.value
  const month = parts.find((p) => p.type === 'month')?.value
  const day = parts.find((p) => p.type === 'day')?.value
  if (!year || !month || !day) return ''
  return `${year}-${month}-${day}`
}

function formatISTDayLabel(epochSec) {
  if (!epochSec) return ''
  return new Date(epochSec * 1000).toLocaleDateString('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: '2-digit',
    month: 'short',
  })
}

function formatRelativeTime(epochSec) {
  const diff = Math.max(0, Math.floor(Date.now() / 1000) - epochSec)
  if (diff < 3) return 'Just now'
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

function formatAgePrecise(epochMs) {
  const diffMs = Math.max(0, Date.now() - epochMs)
  if (diffMs < 1000) return `${Math.floor(diffMs)}ms ago`

  const seconds = diffMs / 1000
  if (seconds < 60) return `${seconds.toFixed(3)}s ago`

  const minutes = Math.floor(seconds / 60)
  const secRemainder = seconds - minutes * 60
  return `${minutes}m ${secRemainder.toFixed(3)}s ago`
}

function normalizeIndicatorName(name) {
  const raw = String(name || '').trim()
  return raw.toLowerCase()
}

function isIndicatorEnabled(name) {
  const key = normalizeIndicatorName(name)
  if (!key) return true
  const value = indicatorVisibility.value[key]
  return value !== false
}

function ensureIndicatorVisibilityDefaults(indicators) {
  if (!Array.isArray(indicators)) return
  const next = { ...indicatorVisibility.value }
  let changed = false
  for (const indicator of indicators) {
    const key = normalizeIndicatorName(indicator?.name)
    if (!key) continue
    if (!(key in next)) {
      next[key] = true
      changed = true
    }
  }
  if (changed) indicatorVisibility.value = next
}

function setIndicatorEnabled(name, enabled) {
  const key = normalizeIndicatorName(name)
  if (!key) return
  indicatorVisibility.value = {
    ...indicatorVisibility.value,
    [key]: !!enabled,
  }
}

function setAllIndicatorsEnabled(enabled) {
  const next = { ...indicatorVisibility.value }
  for (const item of indicatorCatalog.value) {
    const key = normalizeIndicatorName(item.name)
    if (!key) continue
    next[key] = !!enabled
  }
  indicatorVisibility.value = next
}

async function fetchJsonWithMetrics(url, errorPrefix) {
  const start = performance.now()
  const response = await fetch(url)
  lastApiResponseAtMs.value = Date.now()
  lastApiLatencyMs.value = performance.now() - start
  if (!response.ok) throw new Error(`${errorPrefix} (${response.status})`)
  return response.json()
}

function normalizeOrderBookRows(payload) {
  let rows = []
  if (Array.isArray(payload?.users_with_order_books)) rows = payload.users_with_order_books
  else if (Array.isArray(payload?.order_books)) rows = payload.order_books
  else if (Array.isArray(payload?.items)) rows = payload.items
  else if (Array.isArray(payload?.users)) rows = payload.users
  else if (Array.isArray(payload?.data)) rows = payload.data
  else if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    const entries = Object.entries(payload).filter(([, value]) => value && typeof value === 'object')
    if (entries.length > 0) {
      rows = entries.map(([key, value]) => ({ user_id: key, ...value }))
    }
  }

  return rows.map((row, idx) => {
    if (typeof row === 'string' || typeof row === 'number') {
      return {
        user_id: String(row),
        orders: [],
        raw: { user_id: String(row) },
      }
    }

    const userId = String(row.user_id ?? row.user ?? row.account_id ?? row.client_id ?? `user-${idx + 1}`)
    const orders = Array.isArray(row.orders)
      ? row.orders
      : Array.isArray(row.order_book)
        ? row.order_book
        : Array.isArray(row.orderBook)
          ? row.orderBook
        : Array.isArray(row.items)
          ? row.items
          : []

    const sourcePositions = row.positions && typeof row.positions === 'object'
      ? row.positions
      : row.position && typeof row.position === 'object'
        ? row.position
        : null

    const netPositions = Array.isArray(sourcePositions?.net)
      ? sourcePositions.net
      : Array.isArray(sourcePositions?.overnight)
        ? sourcePositions.overnight
        : Array.isArray(row.net_positions)
          ? row.net_positions
          : Array.isArray(row.net)
            ? row.net
            : Array.isArray(sourcePositions)
              ? sourcePositions
              : []

    const dayPositions = Array.isArray(sourcePositions?.day)
      ? sourcePositions.day
      : Array.isArray(sourcePositions?.intraday)
        ? sourcePositions.intraday
        : Array.isArray(row.day_positions)
          ? row.day_positions
          : Array.isArray(row.day)
            ? row.day
            : []

    const positions = {
      net: netPositions,
      day: dayPositions,
    }

    return {
      user_id: userId,
      orders,
      positions,
      raw: row,
    }
  })
}

function getOrderSyncState(order) {
  if (order?.__sync_state) return order.__sync_state
  if (order?.sync_status) return order.sync_status
  return 'broker_synced'
}

function positionDisplayKey(pos) {
  return [
    String(pos?.instrument_token ?? ''),
    String(pos?.exchange ?? ''),
    String(pos?.product ?? ''),
    String(pos?.tradingsymbol ?? pos?.symbol ?? ''),
  ].join('|')
}

function getMergedPositions(row) {
  const merged = new Map()
  const net = Array.isArray(row?.positions?.net) ? row.positions.net : []
  const day = Array.isArray(row?.positions?.day) ? row.positions.day : []

  for (const pos of net) {
    const key = positionDisplayKey(pos)
    if (!merged.has(key)) {
      merged.set(key, { ...pos, __source: 'NET' })
    }
  }
  for (const pos of day) {
    const key = positionDisplayKey(pos)
    if (!merged.has(key)) {
      merged.set(key, { ...pos, __source: 'DAY' })
    } else {
      const prev = merged.get(key)
      prev.__source = prev.__source === 'NET' ? 'NET+DAY' : prev.__source
      merged.set(key, prev)
    }
  }

  return Array.from(merged.values())
}

function syncSelectedOrderBookUsers() {
  const availableUserIds = orderBookRows.value.map((row) => String(row.user_id))
  const availableSet = new Set(availableUserIds)
  const retained = selectedOrderBookUsers.value.filter((userId) => availableSet.has(userId))

  selectedOrderBookUsers.value = retained.length > 0 ? retained : availableUserIds

  const nextTabs = {}
  for (const userId of availableUserIds) {
    const prev = orderBookUserSubTabs.value[userId]
    nextTabs[userId] = prev === 'net' || prev === 'day' || prev === 'pending' || prev === 'executed' ? prev : 'net'
  }
  orderBookUserSubTabs.value = nextTabs
}

function toggleOrderBookUser(userId) {
  const id = String(userId)
  const set = new Set(selectedOrderBookUsers.value)
  if (set.has(id)) set.delete(id)
  else set.add(id)
  selectedOrderBookUsers.value = orderBookRows.value
    .map((row) => String(row.user_id))
    .filter((candidate) => set.has(candidate))
}

function selectAllOrderBookUsers() {
  selectedOrderBookUsers.value = orderBookRows.value.map((row) => String(row.user_id))
}

function clearOrderBookUsers() {
  selectedOrderBookUsers.value = []
}

function getOrderBookUserSubTab(userId) {
  return orderBookUserSubTabs.value[String(userId)] || 'net'
}

function setOrderBookUserSubTab(userId, tab) {
  const id = String(userId)
  orderBookUserSubTabs.value = {
    ...orderBookUserSubTabs.value,
    [id]: tab,
  }
}

function getUserNetPositions(row) {
  return Array.isArray(row?.positions?.net) ? row.positions.net : []
}

function getUserDayPositions(row) {
  return Array.isArray(row?.positions?.day) ? row.positions.day : []
}

function parseEpochMs(value) {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null
    return value > 1e12 ? Math.floor(value) : Math.floor(value * 1000)
  }
  const asNumber = Number(value)
  if (!Number.isNaN(asNumber)) return asNumber > 1e12 ? Math.floor(asNumber) : Math.floor(asNumber * 1000)
  const parsed = Date.parse(String(value))
  if (!Number.isNaN(parsed)) return parsed
  return null
}

function getOrderEpochMs(order) {
  return (
    parseEpochMs(order?.exchange_update_timestamp) ||
    parseEpochMs(order?.exchange_timestamp) ||
    parseEpochMs(order?.order_timestamp) ||
    parseEpochMs(order?.updated_at) ||
    parseEpochMs(order?.created_at) ||
    parseEpochMs(order?.order_datetime) ||
    0
  )
}

function formatOrderTime(order) {
  const epochMs = getOrderEpochMs(order)
  if (!epochMs) return '--'
  return new Date(epochMs).toLocaleString('en-IN', {
    timeZone: 'Asia/Kolkata',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

function getUserRecentExecutedOrders(row) {
  const orders = Array.isArray(row?.orders) ? row.orders : []
  return orders
    .filter((order) => isCompletedOrderStatus(normalizeOrderStatus(order)))
    .slice()
    .sort((a, b) => getOrderEpochMs(b) - getOrderEpochMs(a))
}

function getUserPendingOrders(row) {
  const orders = Array.isArray(row?.orders) ? row.orders : []
  return orders
    .filter((order) => isPendingOrderStatus(normalizeOrderStatus(order)))
    .slice()
    .sort((a, b) => getOrderEpochMs(b) - getOrderEpochMs(a))
}

function normalizeOrderType(order) {
  return String(order?.order_type || order?.ordertype || '').trim().toUpperCase()
}

function getOrderOriginLabel(order) {
  const explicit = String(order?.origin || order?.source || order?.entry_source || '').trim().toUpperCase()
  const tag = String(order?.tag || order?.tags || order?.strategy || '').trim().toUpperCase()
  const merged = `${explicit} ${tag}`.trim()

  if (/(ALGO|AUTO|BOT|SIGNAL|STRATEGY|SYSTEM)/.test(merged)) return 'ALGO'
  if (/(MANUAL|CUSTOM|USER)/.test(merged)) return 'CUSTOM'
  return '--'
}

function getOrderPendingPriceLabel(order) {
  const type = normalizeOrderType(order)
  const rawPrice = Number(order?.price)
  const rawTrigger = Number(order?.trigger_price)
  const price = Number.isFinite(rawPrice) ? rawPrice : null
  const trigger = Number.isFinite(rawTrigger) ? rawTrigger : null

  if (type === 'MARKET') return 'MKT'
  if (type === 'LIMIT') return price === null ? 'LMT' : `LMT ${formatNumber(price)}`
  if (type === 'SL') {
    if (price !== null && trigger !== null) return `SL ${formatNumber(price)} / T ${formatNumber(trigger)}`
    if (price !== null) return `SL ${formatNumber(price)}`
    if (trigger !== null) return `SL T ${formatNumber(trigger)}`
    return 'SL'
  }
  if (type === 'SL-M' || type === 'SLM') return trigger === null ? 'SL-M' : `SL-M T ${formatNumber(trigger)}`
  if (price !== null && trigger !== null) return `${formatNumber(price)} / T ${formatNumber(trigger)}`
  if (price !== null) return formatNumber(price)
  if (trigger !== null) return `T ${formatNumber(trigger)}`
  return '--'
}

function getPositionPnlValue(pos) {
  const raw =
    pos?.pnl ??
    pos?.m2m ??
    pos?.unrealised ??
    pos?.unrealized ??
    pos?.day_pnl ??
    pos?.mtm ??
    null
  if (raw === null || raw === undefined || raw === '') return null
  const numeric = Number(raw)
  return Number.isFinite(numeric) ? numeric : null
}

function getPositionPnlClass(pos) {
  const pnl = getPositionPnlValue(pos)
  if (pnl === null) return ''
  return pnl >= 0 ? 'good-text' : 'bad-text'
}

function getUserPanelTotals(row) {
  const netPositions = getUserNetPositions(row)
  let netQty = 0
  let totalPnl = 0
  let winners = 0
  let losers = 0

  for (const pos of netPositions) {
    netQty += getPositionQty(pos)
    const pnl = getPositionPnlValue(pos)
    if (pnl === null) continue
    totalPnl += pnl
    if (pnl >= 0) winners += 1
    else losers += 1
  }

  return {
    netQty,
    totalPnl,
    winners,
    losers,
  }
}

async function refreshOrderBooks() {
  if (orderBookPollInFlight) return
  orderBookPollInFlight = true
  orderBookLoading.value = true
  orderBookError.value = ''
  try {
    let endpoint = '/accounts/users-with-order-books'
    if (orderBookMode.value === 'users') endpoint = '/accounts/users'
    if (orderBookMode.value === 'order-books') endpoint = '/accounts/order-books'
    if (orderBookMode.value === 'bulk') endpoint = '/accounts/users-with-order-books/bulk'

    const params = new URLSearchParams()
    if (orderBookMode.value !== 'bulk') {
      params.set('limit', String(orderBookLimit.value))
      params.set('offset', String(orderBookOffset.value))
    }

    const url = `${apiBase}${endpoint}${params.size ? `?${params.toString()}` : ''}`
    const payload = await fetchJsonWithMetrics(url, 'order books fetch failed')
    orderBookRows.value = normalizeOrderBookRows(payload)
    orderBookUsersTotal.value = Number(
      payload?.total_users ?? payload?.total ?? payload?.count ?? payload?.users_count ?? orderBookRows.value.length,
    )
    syncSelectedOrderBookUsers()
    lastOrderBookResponseAtMs.value = Date.now()
  } catch (error) {
    orderBookError.value = error?.message || String(error)
  } finally {
    orderBookLoading.value = false
    orderBookPollInFlight = false
  }
}

function formatNumber(value) {
  if (value === null || value === undefined) return '--'
  return Number(value).toLocaleString('en-IN', {
    maximumFractionDigits: 2,
  })
}

function formatSignedNumber(value) {
  if (value === null || value === undefined) return '--'
  const num = Number(value)
  if (!Number.isFinite(num)) return '--'
  return `${num > 0 ? '+' : ''}${formatNumber(num)}`
}

function getSignalOutcomeClass(outcome) {
  if (!outcome || outcome.result === 'na') return ''
  return outcome.result === 'win' ? 'good-text' : 'bad-text'
}

function normalizeSignalAction(action) {
  return String(action || '').trim().toUpperCase()
}

function signalBadgeClass(action) {
  const normalized = normalizeSignalAction(action)
  if (normalized === 'BUY') return 'signal-buy'
  if (normalized === 'SELL') return 'signal-sell'
  if (normalized === 'EXIT') return 'signal-exit'
  return 'signal-neutral'
}

function getSignalMarkerVisual(action) {
  const normalized = normalizeSignalAction(action)
  if (normalized === 'BUY') {
    return { position: 'belowBar', color: '#14B8A6', shape: 'arrowUp', text: 'B' }
  }
  if (normalized === 'SELL') {
    return { position: 'aboveBar', color: '#FB7185', shape: 'arrowDown', text: 'S' }
  }
  if (normalized === 'EXIT') {
    return { position: 'inBar', color: '#94A3B8', shape: 'square', text: 'X' }
  }
  return null
}

function buildTradingDayBoundaryMarkers(candles) {
  if (!Array.isArray(candles) || candles.length === 0) return []

  const markers = []
  let prevDayKey = ''
  for (const candle of candles) {
    const ts = normalizeTimestamp(candle?.timestamp)
    if (!ts) continue
    const dayKey = toISTDateKey(ts)
    if (!dayKey) continue
    if (prevDayKey && dayKey !== prevDayKey) {
      markers.push({
        time: ts,
        position: 'inBar',
        color: 'rgba(148, 163, 184, 0.42)',
        shape: 'square',
        text: `| ${formatISTDayLabel(ts)}`,
      })
    }
    prevDayKey = dayKey
  }
  return markers
}

function toChartData(candles) {
  return candles
    .map((candle) => toChartPoint(candle))
    .filter(Boolean)
}

function toVolumeData(candles) {
  return candles
    .map((candle) => toVolumePoint(candle))
    .filter(Boolean)
}

function toChartPoint(candle) {
  const time = normalizeTimestamp(candle.timestamp)
  const open = Number(candle.open)
  const high = Number(candle.high)
  const low = Number(candle.low)
  const close = Number(candle.close)
  if (
    !Number.isFinite(time) ||
    time <= 0 ||
    !Number.isFinite(open) ||
    !Number.isFinite(high) ||
    !Number.isFinite(low) ||
    !Number.isFinite(close)
  ) {
    return null
  }
  return { time, open, high, low, close }
}

function toVolumePoint(candle) {
  const time = normalizeTimestamp(candle.timestamp)
  const open = Number(candle.open)
  const close = Number(candle.close)
  const volume = Number(candle.volume)
  if (!Number.isFinite(time) || time <= 0 || !Number.isFinite(open) || !Number.isFinite(close)) {
    return null
  }
  return {
    time,
    value: Number.isFinite(volume) ? volume : 0,
    color: close >= open ? 'rgba(45, 212, 191, 0.68)' : 'rgba(248, 113, 113, 0.68)',
  }
}

function mergeCandles(existing, incoming) {
  const byTimestamp = new Map(existing.map((candle) => [normalizeTimestamp(candle.timestamp), candle]))
  for (const candle of incoming) {
    byTimestamp.set(normalizeTimestamp(candle.timestamp), candle)
  }
  return [...byTimestamp.values()].sort((a, b) => normalizeTimestamp(a.timestamp) - normalizeTimestamp(b.timestamp))
}

function applyRealtimeCandleUpdates(state, incomingCandles) {
  if (!state?.mainSeries || !state?.volumeSeries) return false
  if (!Array.isArray(incomingCandles) || incomingCandles.length === 0) return true

  const ordered = [...incomingCandles].sort(
    (a, b) => normalizeTimestamp(a.timestamp) - normalizeTimestamp(b.timestamp),
  )
  let lastTs = state.candles.length
    ? normalizeTimestamp(state.candles[state.candles.length - 1].timestamp)
    : null

  for (const candle of ordered) {
    const ts = normalizeTimestamp(candle.timestamp)
    if (!ts) continue
    if (lastTs !== null && ts < lastTs) return false

    if (state.candles.length === 0 || lastTs === null || ts > lastTs) {
      state.candles.push(candle)
      const chartPoint = toChartPoint(candle)
      const volumePoint = toVolumePoint(candle)
      if (chartPoint) state.mainSeries.update(chartPoint)
      if (volumePoint) state.volumeSeries.update(volumePoint)
      lastTs = ts
      continue
    }

    state.candles[state.candles.length - 1] = candle
    const chartPoint = toChartPoint(candle)
    const volumePoint = toVolumePoint(candle)
    if (chartPoint) state.mainSeries.update(chartPoint)
    if (volumePoint) state.volumeSeries.update(volumePoint)
  }

  return true
}

async function fetchCandles({ timeframe, start, end, limit }) {
  const payload = await fetchJsonWithMetrics(
    `${apiBase}/candles?instrument_token=${instrumentToken.value}&timeframe=${timeframe}&start=${start}&end=${end}&limit=${limit}`,
    'candles fetch failed',
  )
  return payload.candles || []
}

async function fetchChartBundle({ timeframe, start, end, limit }) {
  const params = new URLSearchParams({
    instrument_token: String(instrumentToken.value),
    timeframe,
    start: String(start),
    end: String(end),
    limit: String(limit),
  })
  if (activeSignalStrategy.value) {
    params.set('strategy_name', activeSignalStrategy.value)
  }

  try {
    const payload = await fetchJsonWithMetrics(
      `${apiBase}/chart-data?${params.toString()}`,
      'chart data fetch failed',
    )
    return {
      candles: payload.candles || [],
      indicators: payload.indicators || [],
      signals: payload.signals || [],
      trade_contract: payload.trade_contract || null,
    }
  } catch (error) {
    // Compatibility fallback for older backend versions.
    const candles = await fetchCandles({ timeframe, start, end, limit })
    const indicators = await fetchIndicatorsForTimeframe({ timeframe, start, end, limit })
    return { candles, indicators, signals: [], trade_contract: null }
  }
}

async function fetchSignalCatalog() {
  try {
    const payload = await fetchJsonWithMetrics(`${apiBase}/signals/catalog`, 'signals catalog fetch failed')
    signalStrategies.value = Array.isArray(payload?.items) ? payload.items : []
    const valid = signalStrategies.value.filter((item) => item.valid)
    if (!activeSignalStrategy.value && valid.length > 0) {
      activeSignalStrategy.value = valid[0].name
    } else if (activeSignalStrategy.value && !valid.some((item) => item.name === activeSignalStrategy.value)) {
      activeSignalStrategy.value = valid.length > 0 ? valid[0].name : ''
    }
  } catch {
    signalStrategies.value = []
    activeSignalStrategy.value = ''
  }
}

async function fetchIndicatorsForTimeframe({ timeframe, start, end, limit }) {
  const params = new URLSearchParams({
    instrument_token: String(instrumentToken.value),
    timeframe,
    start: String(start),
    end: String(end),
    limit: String(limit),
  })

  const payload = await fetchJsonWithMetrics(
    `${apiBase}/indicators?${params.toString()}`,
    'indicators fetch failed',
  )
  return payload.indicators || []
}

function getStateRange(state) {
  if (!state?.candles?.length) return null
  const start = normalizeTimestamp(state.candles[0].timestamp)
  const end = normalizeTimestamp(state.candles[state.candles.length - 1].timestamp)
  if (!start || !end) return null
  return { start, end }
}

async function fetchLatestTimestamp(timeframe) {
  const payload = await fetchJsonWithMetrics(
    `${apiBase}/candles/latest?instrument_token=${instrumentToken.value}&timeframe=${timeframe}`,
    'latest timestamp fetch failed',
  )
  if (payload?.timestamp === null || payload?.timestamp === undefined) return null
  return normalizeTimestamp(payload.timestamp)
}

function setUniverseError(error, prefix = 'Universe') {
  const msg = error?.message || String(error)
  universeError.value = `${prefix}: ${msg}`
}

async function fetchUniverseMeta() {
  universeMetaLoading.value = true
  universeError.value = ''
  try {
    const payload = await fetchJsonWithMetrics(`${apiBase}/universe/meta`, 'meta fetch failed')
    universeMeta.value = payload.meta || null
  } catch (error) {
    setUniverseError(error, 'Universe meta')
  } finally {
    universeMetaLoading.value = false
  }
}

async function fetchUniverseIndexes() {
  universeIndexesLoading.value = true
  universeError.value = ''
  try {
    const payload = await fetchJsonWithMetrics(`${apiBase}/universe/indexes`, 'indexes fetch failed')
    universeIndexes.value = payload.indexes || []
    if (!selectedIndexName.value && universeIndexes.value.length > 0) {
      selectedIndexName.value = universeIndexes.value[0].index_name
    }
  } catch (error) {
    setUniverseError(error, 'Universe indexes')
  } finally {
    universeIndexesLoading.value = false
  }
}

async function fetchUniverseInstruments({ resetOffset = false } = {}) {
  universeInstrumentsLoading.value = true
  universeError.value = ''
  try {
    if (resetOffset) instrumentOffset.value = 0
    const params = new URLSearchParams()
    params.set('limit', String(instrumentLimit.value))
    params.set('offset', String(instrumentOffset.value))
    if (instrumentExchange.value.trim()) params.set('exchange', instrumentExchange.value.trim())
    if (instrumentType.value.trim()) params.set('instrument_type', instrumentType.value.trim())
    if (instrumentSegment.value.trim()) params.set('segment', instrumentSegment.value.trim())
    if (instrumentSearch.value.trim()) params.set('search', instrumentSearch.value.trim())

    const payload = await fetchJsonWithMetrics(
      `${apiBase}/universe/instruments?${params.toString()}`,
      'instruments fetch failed',
    )
    universeInstrumentRows.value = payload.instruments || []
    universeInstrumentTotal.value = Number(payload.total || 0)
    universeInstrumentCount.value = Number(payload.count || universeInstrumentRows.value.length)
  } catch (error) {
    setUniverseError(error, 'Universe instruments')
  } finally {
    universeInstrumentsLoading.value = false
  }
}

async function fetchIndexConstituents(indexName) {
  if (!indexName) return
  universeConstituentsLoading.value = true
  universeError.value = ''
  try {
    const payload = await fetchJsonWithMetrics(
      `${apiBase}/universe/index/${encodeURIComponent(indexName)}/constituents`,
      'constituents fetch failed',
    )
    universeConstituents.value = payload.constituents || []
    selectedIndexName.value = indexName
  } catch (error) {
    setUniverseError(error, 'Index constituents')
  } finally {
    universeConstituentsLoading.value = false
  }
}

async function initializeUniversePanel() {
  await Promise.all([fetchUniverseMeta(), fetchUniverseIndexes()])
  await fetchUniverseInstruments({ resetOffset: true })
  if (selectedIndexName.value) {
    await fetchIndexConstituents(selectedIndexName.value)
  }
}

function clearIndicatorSeries(state) {
  for (const series of state.indicatorSeries) {
    state.chart.removeSeries(series)
  }
  state.indicatorSeries = []
}

function indicatorPaneIndex(indicator) {
  const name = normalizeIndicatorName(indicator?.name)
  const cols = Array.isArray(indicator?.columns) ? indicator.columns.map((c) => String(c).toLowerCase()) : []
  const isRsi = name.includes('rsi') || cols.some((c) => c.includes('rsi'))
  return isRsi ? 2 : 0
}

function renderIndicatorsForState(state) {
  if (!state?.chart) return
  clearIndicatorSeries(state)
  let hasLowerPaneIndicator = false
  for (const indicator of state.indicators || []) {
    if (!isIndicatorEnabled(indicator?.name)) continue
    const paneIndex = indicatorPaneIndex(indicator)
    if (paneIndex > 0) hasLowerPaneIndicator = true
    if (indicator?.values && indicator.values.length > 0) {
      state.indicatorSeries.push(...addIndicatorToChart(state, indicator, paneIndex))
    }
  }
  if (hasLowerPaneIndicator) ensureIndicatorPane(state)
}

function hexToRgba(hex, alpha) {
  const raw = String(hex || '').replace('#', '')
  if (raw.length !== 6) return hex
  const r = parseInt(raw.slice(0, 2), 16)
  const g = parseInt(raw.slice(2, 4), 16)
  const b = parseInt(raw.slice(4, 6), 16)
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function addIndicatorToChart(state, indicator, paneIndex = 2) {
  const created = []
  const indicatorName = normalizeIndicatorName(indicator?.name)
  let firstRsiRenderableSeries = null
  let firstIndicatorTime = null
  let lastIndicatorTime = null

  indicator.columns.forEach((column, idx) => {
    const isRenderable =
      column === 'value' ||
      column.startsWith('kc_') ||
      column.endsWith('ema') ||
      column.endsWith('rsi') ||
      column.endsWith('sma')

    if (!isRenderable) return

    const palette = ['#4F46E5', '#06B6D4', '#10B981', '#F59E0B', '#F43F5E']
    const scaleId = `ind-${indicator.name}-${column}`
    const targetPriceScaleId = paneIndex === 0 ? 'right' : scaleId

    const baseColor = palette[idx % palette.length]
    const lineData = indicator.values
      .filter((value) => value[column] !== null && value[column] !== undefined)
      .map((value) => ({
        time: normalizeTimestamp(value.timestamp),
        value: Number(value[column]),
        source: value._source || indicator.source || 'computed',
      }))
      .filter((point) => Number.isFinite(point.time) && point.time > 0 && Number.isFinite(point.value))

    if (lineData.length > 0) {
      const firstTime = lineData[0].time
      const lastTime = lineData[lineData.length - 1].time
      if (firstIndicatorTime === null || firstTime < firstIndicatorTime) firstIndicatorTime = firstTime
      if (lastIndicatorTime === null || lastTime > lastIndicatorTime) lastIndicatorTime = lastTime
    }

    const precomputedData = lineData.filter((p) => p.source === 'precomputed').map(({ time, value }) => ({ time, value }))
    const computedData = lineData.filter((p) => p.source !== 'precomputed').map(({ time, value }) => ({ time, value }))

    const makeLine = (opts, data) => {
      const line = state.chart.addSeries(
        LineSeries,
        {
          ...opts,
          priceScaleId: targetPriceScaleId,
        },
        paneIndex,
      )
      line.setData(data)
      created.push(line)
      if (!firstRsiRenderableSeries && indicatorName.includes('rsi')) {
        firstRsiRenderableSeries = line
      }
    }

    if (precomputedData.length > 0) {
      makeLine(
        {
          color: hexToRgba(baseColor, 0.65),
          lineWidth: 2,
          title: `${indicator.name.toUpperCase()} ${column} [pre]`,
        },
        precomputedData,
      )
    }

    if (computedData.length > 0) {
      makeLine(
        {
          color: baseColor,
          lineWidth: 2,
          lineStyle: 2,
          title: `${indicator.name.toUpperCase()} ${column} [live]`,
        },
        computedData,
      )
    }

    if (paneIndex > 0) {
      state.chart.priceScale(scaleId, paneIndex).applyOptions({
        scaleMargins: { top: 0.15, bottom: 0.15 },
        borderVisible: false,
      })
    }
  })

  const isRsi = indicatorName.includes('rsi')
  if (isRsi && paneIndex > 0 && firstRsiRenderableSeries) {
    firstRsiRenderableSeries.createPriceLine({
      price: 70,
      color: 'rgba(248, 113, 113, 0.46)',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: false,
      title: '',
    })
    firstRsiRenderableSeries.createPriceLine({
      price: 30,
      color: 'rgba(34, 197, 94, 0.46)',
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: false,
      title: '',
    })
  }

  return created
}

function computeIndicatorsSignature(indicators) {
  const indicatorParts = Array.isArray(indicators)
    ? indicators.map((indicator) => {
        const name = normalizeIndicatorName(indicator?.name)
        const columns = Array.isArray(indicator?.columns) ? indicator.columns.join(',') : ''
        const values = Array.isArray(indicator?.values) ? indicator.values : []
        const lastTs = values.length
          ? normalizeTimestamp(values[values.length - 1]?.timestamp ?? values[values.length - 1]?.time)
          : 0
        return `${name}|${columns}|${values.length}|${lastTs}`
      })
    : []

  return indicatorParts.join('||')
}

function computeSignalsSignature(signals) {
  const signalParts = Array.isArray(signals)
    ? signals.map((signal) => {
        const ts = normalizeTimestamp(signal?.timestamp ?? signal?.epoch)
        return `${normalizeSignalAction(signal?.action)}|${ts}|${signal?.strategy || ''}`
      })
    : []

  return signalParts.join('||')
}

function getLastCandleTimestamp(state) {
  if (!state?.candles?.length) return 0
  return normalizeTimestamp(state.candles[state.candles.length - 1].timestamp) || 0
}

function buildIndicatorInputKey(state, range) {
  const lastCandleTs = getLastCandleTimestamp(state)
  return `${range.start}:${range.end}:${lastCandleTs}:${activeSignalStrategy.value || ''}`
}

function shouldPollIndicatorsForState(state) {
  if (!state) return false
  if (activeSignalStrategy.value) return true
  const indicators = Array.isArray(state.indicators) ? state.indicators : []
  return indicators.some((indicator) => isIndicatorEnabled(indicator?.name))
}

function applyIndicatorBundleToState(state, indicators, signals) {
  const nextIndicators = Array.isArray(indicators) ? indicators : []
  const nextSignals = Array.isArray(signals) ? signals : []

  const nextIndicatorSig = computeIndicatorsSignature(nextIndicators)
  const nextSignalSig = computeSignalsSignature(nextSignals)

  const indicatorsChanged = nextIndicatorSig !== state.indicatorSignature
  const signalsChanged = nextSignalSig !== state.signalSignature

  if (indicatorsChanged) {
    state.indicators = nextIndicators
    ensureIndicatorVisibilityDefaults(state.indicators)
    renderIndicatorsForState(state)
    state.indicatorSignature = nextIndicatorSig
  }

  if (signalsChanged) {
    state.signals = nextSignals
    applySignalMarkers(state)
    state.signalSignature = nextSignalSig
  }

  return indicatorsChanged || signalsChanged
}

function ensureIndicatorPane(state) {
  if (!state.chart || state.indicatorSeries.length > 0 || state.candles.length === 0) return

  const first = normalizeTimestamp(state.candles[0].timestamp)
  const last = normalizeTimestamp(state.candles[state.candles.length - 1].timestamp)
  if (!first || !last) return

  const placeholder = state.chart.addSeries(
    LineSeries,
    {
      color: 'rgba(0,0,0,0)',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    },
    2,
  )

  placeholder.setData([
    { time: first, value: 0 },
    { time: last, value: 0 },
  ])

  state.indicatorSeries.push(placeholder)
}

function applySignalMarkers(state) {
  if (!state?.mainSeries) return
  const signals = Array.isArray(state.signals) ? state.signals : []
  const signalMarkers = signals
    .map((signal) => {
      const action = normalizeSignalAction(signal.action)
      const visual = getSignalMarkerVisual(action)
      if (!visual) return null
      const ts = normalizeTimestamp(signal.timestamp ?? signal.epoch)
      if (!ts) return null
      return {
        time: ts,
        position: visual.position,
        color: visual.color,
        shape: visual.shape,
        text: visual.text,
      }
    })
    .filter(Boolean)
  const dayBoundaryMarkers = buildTradingDayBoundaryMarkers(state.candles)
  const markers = [...dayBoundaryMarkers, ...signalMarkers].sort((a, b) => a.time - b.time)

  if (state.signalMarkers && typeof state.signalMarkers.setMarkers === 'function') {
    state.signalMarkers.setMarkers(markers)
    return
  }

  if (typeof createSeriesMarkers === 'function') {
    state.signalMarkers = createSeriesMarkers(state.mainSeries, markers)
    return
  }

  if (typeof state.mainSeries.setMarkers === 'function') {
    state.mainSeries.setMarkers(markers)
  }
}

async function refreshIndicatorsForState(state) {
  if (!state || state.indicatorRefreshing) return
  if (!shouldPollIndicatorsForState(state)) return

  const range = getStateRange(state)
  if (!range) return

  const indicatorInputKey = buildIndicatorInputKey(state, range)
  const nowMs = Date.now()
  const recentlyPolled = state.lastIndicatorInputKey === indicatorInputKey
  const isStale = nowMs - (state.lastIndicatorRefreshAtMs || 0) >= INDICATOR_STALE_REFRESH_MS
  if (recentlyPolled && !isStale) return

  state.indicatorRefreshing = true
  try {
    const desired = Math.max(state.candles.length + 200, 500)
    const indicatorLimit = Math.min(desired, 20000)
    const bundle = await fetchChartBundle({
      timeframe: state.timeframe,
      start: range.start,
      end: range.end,
      limit: indicatorLimit,
    })
    if (bundle.trade_contract) activeTradeContract.value = bundle.trade_contract
    applyIndicatorBundleToState(state, bundle.indicators, bundle.signals ?? state.signals ?? [])
    state.lastIndicatorInputKey = indicatorInputKey
    state.lastIndicatorRefreshAtMs = Date.now()
    state.error = ''
  } catch (error) {
    state.error = error.message
  } finally {
    state.indicatorRefreshing = false
  }
}

function removeAllCharts() {
  chartStates.value.forEach((state) => state.chart?.remove())
}

function getLoadingRangeStyle(state) {
  if (!state.isLoadingRange || state.loadingRangeFrom === null || state.loadingRangeTo === null) return null
  if (state.visibleFrom === null || state.visibleTo === null || state.visibleTo <= state.visibleFrom) {
    return { left: '0%', width: '100%' }
  }

  const leftPct = ((state.loadingRangeFrom - state.visibleFrom) / (state.visibleTo - state.visibleFrom)) * 100
  const rightPct = ((state.loadingRangeTo - state.visibleFrom) / (state.visibleTo - state.visibleFrom)) * 100
  const clampedLeft = Math.max(0, Math.min(100, leftPct))
  const clampedRight = Math.max(0, Math.min(100, rightPct))
  const width = Math.max(2, clampedRight - clampedLeft)

  if (clampedRight <= 0 || clampedLeft >= 100) return null
  return { left: `${clampedLeft}%`, width: `${width}%` }
}

function resizeCharts() {
  for (const state of chartStates.value) {
    const container = document.getElementById(`chart-container-${state.timeframe}`)
    if (container && state.chart) {
      state.chart.applyOptions({
        width: container.clientWidth,
        height: container.clientHeight,
      })
    }
  }
}

function stopPolling() {
  if (realtimeTimer) clearInterval(realtimeTimer)
  if (indicatorTimer) clearInterval(indicatorTimer)
  if (orderBookTimer) clearInterval(orderBookTimer)
  realtimeTimer = null
  indicatorTimer = null
  orderBookTimer = null
}

function syncLogicalRangeAcrossCharts(sourceIndex, logicalRange) {
  if (!logicalRange) return
  pendingLogicalSync = { sourceIndex, logicalRange }
  if (logicalSyncRaf) return
  logicalSyncRaf = requestAnimationFrame(() => {
    logicalSyncRaf = null
    if (!pendingLogicalSync || syncingLogicalRange) return
    syncingLogicalRange = true
    try {
      const { sourceIndex: source, logicalRange: range } = pendingLogicalSync
      chartStates.value.forEach((otherState, otherIndex) => {
        if (otherIndex === source || !otherState.chart) return
        otherState.chart.timeScale().setVisibleLogicalRange(range)
      })
    } finally {
      syncingLogicalRange = false
      pendingLogicalSync = null
    }
  })
}

function areRangesEquivalent(a, b) {
  if (!a || !b) return false
  return Math.abs(Number(a.from) - Number(b.from)) < 1 && Math.abs(Number(a.to) - Number(b.to)) < 1
}

function applyTimeRangeSync(sourceIndex, timeRange) {
  syncingTimeRange = true
  try {
    chartStates.value.forEach((otherState, otherIndex) => {
      if (otherIndex === sourceIndex || !otherState.chart) return
      const currentRange = otherState.chart.timeScale().getVisibleRange()
      if (areRangesEquivalent(currentRange, timeRange)) return
      otherState.chart.timeScale().setVisibleRange(timeRange)
      otherState.visibleFrom = timeRange.from
      otherState.visibleTo = timeRange.to
    })
  } finally {
    syncingTimeRange = false
  }
}

function syncTimeRangeAcrossCharts(sourceIndex, timeRange) {
  if (!timeRange || syncingTimeRange) return
  pendingTimeSync = { sourceIndex, timeRange }
  if (timeSyncRaf) return
  timeSyncRaf = requestAnimationFrame(() => {
    timeSyncRaf = null
    if (!pendingTimeSync) return
    const { sourceIndex: source, timeRange: range } = pendingTimeSync
    pendingTimeSync = null
    applyTimeRangeSync(source, range)
  })
}

function startPolling() {
  stopPolling()
  realtimeTimer = setInterval(refreshRealtimeCandles, REALTIME_POLL_MS)
  indicatorTimer = setInterval(refreshIndicators, INDICATOR_POLL_MS)
  orderBookTimer = setInterval(refreshOrderBooks, ORDER_BOOK_POLL_MS)
}

async function fetchOlderCandles(state) {
  if (state.loadingOlder || state.noMoreHistory || state.candles.length === 0) return

  state.loadingOlder = true

  try {
    const oldestTs = normalizeTimestamp(state.candles[0].timestamp)
    if (state.rangeFromEpoch !== null && oldestTs <= state.rangeFromEpoch) {
      state.noMoreHistory = true
      return
    }

    const fetchStart = state.rangeFromEpoch !== null ? state.rangeFromEpoch : -1
    const fetchEnd = oldestTs - 1
    state.isLoadingRange = true
    state.loadingReason = 'history'
    state.loadingRangeFrom =
      fetchStart > 0 ? fetchStart : fetchEnd - (timeframeSeconds[state.timeframe] || 60) * HISTORY_PAGE_LIMIT
    state.loadingRangeTo = fetchEnd
    state.loadingRangeLabel = formatLoadingRangeLabel(state.loadingRangeFrom, state.loadingRangeTo)

    const older = await fetchCandles({
      timeframe: state.timeframe,
      start: fetchStart,
      end: fetchEnd,
      limit: HISTORY_PAGE_LIMIT,
    })

    if (older.length > 0) {
      state.candles = mergeCandles(state.candles, older)
      state.mainSeries.setData(toChartData(state.candles))
      state.volumeSeries.setData(toVolumeData(state.candles))

      const range = getStateRange(state)
      if (range) {
        const bundle = await fetchChartBundle({
          timeframe: state.timeframe,
          start: range.start,
          end: range.end,
          limit: Math.min(Math.max(state.candles.length + 200, 500), 20000),
        })
        if (bundle.trade_contract) activeTradeContract.value = bundle.trade_contract
        applyIndicatorBundleToState(state, bundle.indicators, bundle.signals ?? state.signals ?? [])
        state.lastIndicatorInputKey = buildIndicatorInputKey(state, range)
        state.lastIndicatorRefreshAtMs = Date.now()
      }
    } else {
      state.noMoreHistory = true
    }
  } catch (error) {
    state.error = error.message
    uiError.value = `Backfill failed for ${state.timeframe}: ${error.message}`
  } finally {
    state.loadingOlder = false
    state.isLoadingRange = false
    state.loadingReason = null
    state.loadingRangeFrom = null
    state.loadingRangeTo = null
    state.loadingRangeLabel = ''
  }
}

async function refreshRealtimeCandles() {
  if (isReloading.value) return
  if (!useFullRange.value) {
    const toEpoch = parseLocalDateTimeInput(rangeToInput.value)
    if (toEpoch !== null && toEpoch < Math.floor(Date.now() / 1000) - 120) return
  }

  let hadSuccess = false
  const frames = chartStates.value.map((state) => state.timeframe)
  const primaryTf = getPrimaryTimeframe(frames)
  const anchorLatest = await fetchLatestTimestamp(primaryTf)
  if (!anchorLatest) return

  const results = await Promise.allSettled(
    chartStates.value.map(async (state) => {
      try {
        const tfSec = timeframeSeconds[state.timeframe] || 60
        const latestTs = anchorLatest

        const lastTs = state.candles.length
          ? normalizeTimestamp(state.candles[state.candles.length - 1].timestamp)
          : latestTs - tfSec * HISTORY_PAGE_LIMIT

        if (latestTs <= lastTs) {
          return true
        }

        const start = Math.max(lastTs - tfSec, latestTs - tfSec * 1000)

        state.isLoadingRange = true
        state.loadingReason = 'realtime'
        state.loadingRangeFrom = start
        state.loadingRangeTo = latestTs
        state.loadingRangeLabel = formatLoadingRangeLabel(start, latestTs)

        const latest = await fetchCandles({
          timeframe: state.timeframe,
          start,
          end: latestTs,
          limit: 1000,
        })

        if (latest.length > 0) {
          const appliedIncrementally = applyRealtimeCandleUpdates(state, latest)
          if (!appliedIncrementally) {
            state.candles = mergeCandles(state.candles, latest)
            state.mainSeries.setData(toChartData(state.candles))
            state.volumeSeries.setData(toVolumeData(state.candles))
          }
        }

        state.error = ''
        lastSyncTs.value = Math.floor(Date.now() / 1000)
        return true
      } catch (error) {
        state.error = error?.message || String(error)
        return false
      } finally {
        state.isLoadingRange = false
        state.loadingReason = null
        state.loadingRangeFrom = null
        state.loadingRangeTo = null
        state.loadingRangeLabel = ''
      }
    }),
  )

  hadSuccess = results.some((result) => result.status === 'fulfilled' && result.value === true)

  if (hadSuccess) {
    connectionState.value = 'online'
  } else if (chartStates.value.length > 0) {
    connectionState.value = 'degraded'
  }
}

async function refreshIndicators() {
  if (isReloading.value) return
  const statesNeedingPoll = chartStates.value.filter((state) => shouldPollIndicatorsForState(state))
  if (statesNeedingPoll.length === 0) return
  await Promise.allSettled(statesNeedingPoll.map((state) => refreshIndicatorsForState(state)))
}

async function loadDataAndRenderMulti() {
  if (isReloading.value) {
    reloadQueued = true
    return
  }
  const requestId = ++latestLoadRequestId

  isReloading.value = true
  uiError.value = ''
  stopPolling()
  removeAllCharts()
  chartStates.value = []

  try {
    const targetFrames = [...new Set(selectedTimeframes.value)].slice(0, 4)
    if (targetFrames.length !== selectedTimeframes.value.length) {
      selectedTimeframes.value = targetFrames
    }

    if (targetFrames.length === 0) {
      selectedTimeframes.value = ['1m']
      return
    }

    connectionState.value = 'syncing'

    const sortedTargetFrames = sortTimeframesByGranularity(targetFrames)
    const primaryTf = getPrimaryTimeframe(sortedTargetFrames)
    const primaryLatestTs = await fetchLatestTimestamp(primaryTf)

    const nextStates = []
    for (const timeframe of sortedTargetFrames) {
      if (requestId !== latestLoadRequestId) return
      const now = Math.floor(Date.now() / 1000)
      const tfSec = timeframeSeconds[timeframe] || 60

      const fromEpoch = useFullRange.value ? null : parseLocalDateTimeInput(rangeFromInput.value)
      const toEpoch = useFullRange.value ? null : parseLocalDateTimeInput(rangeToInput.value)

      const fallbackEnd = primaryLatestTs || now
      const initialEnd = toEpoch !== null ? Math.min(toEpoch, fallbackEnd) : fallbackEnd

      const initialStart = fromEpoch !== null ? fromEpoch : Math.max(0, initialEnd - tfSec * HISTORY_PAGE_LIMIT)
      const normalizedStart = Math.min(initialStart, initialEnd)
      const normalizedEnd = Math.max(initialStart, initialEnd)

      const bundle = await fetchChartBundle({
        timeframe,
        start: normalizedStart,
        end: normalizedEnd,
        limit: HISTORY_PAGE_LIMIT,
      })
      if (bundle.trade_contract) activeTradeContract.value = bundle.trade_contract
      const candles = bundle.candles || []
      const indicators = bundle.indicators || []
      const signals = bundle.signals || []
      const indicatorSignature = computeIndicatorsSignature(indicators)
      const signalSignature = computeSignalsSignature(signals)
      const initialLastCandleTs = candles.length ? normalizeTimestamp(candles[candles.length - 1].timestamp) || 0 : 0
      const initialIndicatorInputKey = `${normalizedStart}:${normalizedEnd}:${initialLastCandleTs}:${activeSignalStrategy.value || ''}`

      nextStates.push({
        timeframe,
        candles,
        indicators,
        signals,
        indicatorSignature,
        signalSignature,
        lastIndicatorInputKey: initialIndicatorInputKey,
        lastIndicatorRefreshAtMs: Date.now(),
        chart: null,
        mainSeries: null,
        signalMarkers: null,
        volumeSeries: null,
        indicatorSeries: [],
        loadingOlder: false,
        noMoreHistory: false,
        rangeFromEpoch: fromEpoch,
        visibleFrom: null,
        visibleTo: null,
        isLoadingRange: false,
        loadingReason: null,
        loadingRangeFrom: null,
        loadingRangeTo: null,
        loadingRangeLabel: '',
        paneCount: 0,
        error: '',
        indicatorRefreshing: false,
      })
    }

    if (requestId !== latestLoadRequestId) return
    chartStates.value = nextStates

    await nextTick()
    if (requestId !== latestLoadRequestId) return

    chartStates.value.forEach((state, index) => {
      const container = document.getElementById(`chart-container-${state.timeframe}`)
      if (!container) return

      const chart = createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
          background: { color: '#050A14' },
          textColor: '#D9E3F0',
          panes: {
            enableResize: true,
            separatorColor: '#223045',
            separatorHoverColor: '#2F425E',
          },
        },
        grid: {
          vertLines: { color: '#1A2434' },
          horzLines: { color: '#1A2434' },
        },
        localization: {
          timeFormatter: (timestamp) =>
            new Date(timestamp * 1000).toLocaleString('en-IN', {
              timeZone: 'Asia/Kolkata',
              year: '2-digit',
              month: 'short',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false,
            }),
        },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          borderColor: '#223045',
          tickMarkFormatter: (timestamp) => {
            const date = new Date(timestamp * 1000)
            return date.toLocaleString('en-IN', {
              timeZone: 'Asia/Kolkata',
              day: '2-digit',
              month: 'short',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false,
            })
          },
        },
        rightPriceScale: {
          borderColor: '#223045',
          scaleMargins: { top: 0.1, bottom: 0.1 },
        },
        crosshair: {
          vertLine: { color: '#345079', width: 1, style: 3 },
          horzLine: { color: '#345079', width: 1, style: 3 },
        },
      })

      state.chart = chart

      state.mainSeries = chart.addSeries(
        CandlestickSeries,
        {
          upColor: '#22C55E',
          downColor: '#EF4444',
          borderVisible: false,
          wickUpColor: '#22C55E',
          wickDownColor: '#EF4444',
        },
        0,
      )
      state.mainSeries.setData(toChartData(state.candles))
      applySignalMarkers(state)

      state.volumeSeries = chart.addSeries(
        HistogramSeries,
        {
          priceFormat: { type: 'volume' },
          priceScaleId: 'right',
          lastValueVisible: false,
          priceLineVisible: false,
        },
        1,
      )
      state.volumeSeries.setData(toVolumeData(state.candles))

      ensureIndicatorVisibilityDefaults(state.indicators)
      renderIndicatorsForState(state)

      const panes = chart.panes()
      state.paneCount = panes.length
      if (panes.length > 0) panes[0].setHeight(Math.round(container.clientHeight * 0.63))
      if (panes.length > 1) panes[1].setHeight(Math.round(container.clientHeight * 0.17))
      if (panes.length > 2) panes[2].setHeight(Math.round(container.clientHeight * 0.2))

      const initialVisible = chart.timeScale().getVisibleRange()
      if (initialVisible) {
        state.visibleFrom = initialVisible.from
        state.visibleTo = initialVisible.to
      }

      chart.timeScale().subscribeVisibleLogicalRangeChange((logicalRange) => {
        if (!logicalRange) return
        syncLogicalRangeAcrossCharts(index, logicalRange)
      })

      chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
        if (!range) return

        state.visibleFrom = range.from
        state.visibleTo = range.to

        syncTimeRangeAcrossCharts(index, range)

        const oldest = state.candles.length ? normalizeTimestamp(state.candles[0].timestamp) : null
        if (oldest !== null && range.from <= oldest + (timeframeSeconds[state.timeframe] || 60) * 20) {
          fetchOlderCandles(state)
        }
      })
    })

    const primary = chartStates.value.find((state) => state.chart)
    if (primary?.chart) {
      const primaryLogicalRange = primary.chart.timeScale().getVisibleLogicalRange()
      if (primaryLogicalRange) {
        syncLogicalRangeAcrossCharts(-1, primaryLogicalRange)
      }

      const primaryTimeRange = primary.chart.timeScale().getVisibleRange()
      if (primaryTimeRange) {
        chartStates.value.forEach((state) => {
          state.visibleFrom = primaryTimeRange.from
          state.visibleTo = primaryTimeRange.to
        })
        syncTimeRangeAcrossCharts(-1, primaryTimeRange)
      }
    }

    if (requestId === latestLoadRequestId) {
      connectionState.value = 'online'
      lastSyncTs.value = Math.floor(Date.now() / 1000)
      startPolling()
    }
  } catch (error) {
    connectionState.value = 'degraded'
    uiError.value = error.message || 'Failed to load dashboard data.'
  } finally {
    if (requestId === latestLoadRequestId) {
      isReloading.value = false
    }
    if (reloadQueued) {
      reloadQueued = false
      // Run one more pass with the latest UI state after rapid toggles.
      loadDataAndRenderMulti()
    }
  }
}

function scheduleReload(delayMs = 120) {
  if (reloadDebounceTimer) clearTimeout(reloadDebounceTimer)
  reloadDebounceTimer = setTimeout(() => {
    reloadDebounceTimer = null
    loadDataAndRenderMulti()
  }, delayMs)
}

async function applyTimeRange() {
  const from = parseLocalDateTimeInput(rangeFromInput.value)
  const to = parseLocalDateTimeInput(rangeToInput.value)

  if (!useFullRange.value && from !== null && to !== null && from > to) {
    uiError.value = 'Invalid range: "From" must be earlier than "To".'
    return
  }

  await loadDataAndRenderMulti()
}

function toggleTimeframe(value) {
  const current = new Set(selectedTimeframes.value)

  if (current.has(value)) {
    if (current.size === 1) return
    current.delete(value)
  } else if (current.size < 4) {
    current.add(value)
  }

  selectedTimeframes.value = timeframeOptions
    .map((item) => item.value)
    .filter((item) => current.has(item))
}

function selectInstrument(value) {
  selectedUniverseInstrument.value = null
  activeTradeContract.value = null
  instrumentToken.value = value
}

function manualRefresh() {
  if (reloadDebounceTimer) {
    clearTimeout(reloadDebounceTimer)
    reloadDebounceTimer = null
  }
  loadDataAndRenderMulti()
}

function toggleQuickIndicatorMenu() {
  isQuickIndicatorMenuOpen.value = !isQuickIndicatorMenuOpen.value
}

async function quickRefreshOps() {
  if (reloadDebounceTimer) {
    clearTimeout(reloadDebounceTimer)
    reloadDebounceTimer = null
  }
  await Promise.all([loadDataAndRenderMulti(), refreshOrderBooks()])
}

function toggleControlDrawer() {
  isControlDrawerOpen.value = !isControlDrawerOpen.value
}

function toggleInsightDrawer() {
  isInsightDrawerOpen.value = !isInsightDrawerOpen.value
}

function toggleInsightWidth() {
  isInsightDrawerWide.value = !isInsightDrawerWide.value
}

function closeDrawers() {
  isControlDrawerOpen.value = false
  isInsightDrawerOpen.value = false
  isQuickIndicatorMenuOpen.value = false
}

function useUniverseInstrument(instrument) {
  const token = Number(instrument.instrument_token)
  if (!Number.isFinite(token)) return
  selectedUniverseInstrument.value = instrument
  instrumentToken.value = token
}

async function runInstrumentSearch() {
  instrumentOffset.value = 0
  await fetchUniverseInstruments({ resetOffset: true })
}

async function nextInstrumentPage() {
  if (instrumentOffset.value + instrumentLimit.value >= universeInstrumentTotal.value) return
  instrumentOffset.value += instrumentLimit.value
  await fetchUniverseInstruments()
}

async function prevInstrumentPage() {
  if (instrumentOffset.value === 0) return
  instrumentOffset.value = Math.max(0, instrumentOffset.value - instrumentLimit.value)
  await fetchUniverseInstruments()
}

async function openIndexDetails(indexName) {
  await fetchIndexConstituents(indexName)
}

async function changeOrderBookMode(mode) {
  orderBookMode.value = mode
  orderBookOffset.value = 0
  await refreshOrderBooks()
}

async function nextOrderBookPage() {
  if (orderBookMode.value === 'bulk') return
  if (orderBookOffset.value + orderBookLimit.value >= orderBookUsersTotal.value) return
  orderBookOffset.value += orderBookLimit.value
  await refreshOrderBooks()
}

async function prevOrderBookPage() {
  if (orderBookMode.value === 'bulk') return
  if (orderBookOffset.value === 0) return
  orderBookOffset.value = Math.max(0, orderBookOffset.value - orderBookLimit.value)
  await refreshOrderBooks()
}

function addLocalTradeEvent(event) {
  localTradeEvents.value.push({
    user_id: String(event.user_id || 'unknown'),
    order_id: event.order_id || `local-${Date.now()}`,
    tradingsymbol: event.tradingsymbol || event.symbol || 'UNKNOWN',
    quantity: event.quantity ?? 0,
    side: event.side || 'NA',
    status: event.status || 'PENDING',
    sync_status: event.sync_status || 'pending_local',
    created_at_ms: event.created_at_ms || Date.now(),
  })
}

onMounted(async () => {
  if (!rangeToInput.value) {
    const now = Math.floor(Date.now() / 1000)
    rangeToInput.value = formatLocalDateTimeInput(now)
  }
  if (!rangeFromInput.value) {
    const now = Math.floor(Date.now() / 1000)
    rangeFromInput.value = formatLocalDateTimeInput(now - 60 * 60 * 24)
  }

  await Promise.all([fetchSignalCatalog(), initializeUniversePanel(), refreshOrderBooks()])
  await loadDataAndRenderMulti()
  window.addEventListener('resize', resizeCharts)
  isBootstrapped.value = true
})

onBeforeUnmount(() => {
  stopPolling()
  if (reloadDebounceTimer) clearTimeout(reloadDebounceTimer)
  reloadDebounceTimer = null
  if (logicalSyncRaf) cancelAnimationFrame(logicalSyncRaf)
  if (timeSyncRaf) cancelAnimationFrame(timeSyncRaf)
  logicalSyncRaf = null
  timeSyncRaf = null
  pendingLogicalSync = null
  pendingTimeSync = null
  window.removeEventListener('resize', resizeCharts)
  removeAllCharts()
})

watch([instrumentToken, selectedTimeframes, useFullRange], async () => {
  if (!isBootstrapped.value) return
  scheduleReload(120)
})

watch(activeSignalStrategy, async () => {
  if (!isBootstrapped.value) return
  scheduleReload(120)
})

watch(
  indicatorVisibility,
  () => {
    for (const state of chartStates.value) {
      renderIndicatorsForState(state)
    }
  },
  { deep: true },
)

watch(availableSignalOutcomeDays, (days) => {
  if (!days || days.length === 0) {
    selectedSignalOutcomeDay.value = ''
    return
  }
  if (!selectedSignalOutcomeDay.value || !days.includes(selectedSignalOutcomeDay.value)) {
    selectedSignalOutcomeDay.value = days[0]
  }
})
</script>

<template>
  <div class="terminal-root">
    <section class="engine-shell">
      <button class="edge-toggle edge-toggle-left" :class="{ active: isControlDrawerOpen }" @click="toggleControlDrawer">
        {{ isControlDrawerOpen ? 'Hide Controls' : 'Show Controls' }}
      </button>
      <button class="edge-toggle edge-toggle-right" :class="{ active: isInsightDrawerOpen }" @click="toggleInsightDrawer">
        {{ isInsightDrawerOpen ? 'Hide Insights' : 'Show Insights' }}
      </button>

      <div class="stage-header">
        <div class="header-left">
          <h2>{{ activeInstrument.symbol }} Engine View</h2>
          <p>{{ activeInstrument.label }} · {{ selectedTimeframes.join(' / ') }} · IST</p>
          <p class="trade-contract-line" :class="{ bad: tradeContractUnresolved }">
            Will Trade: {{ tradeContractLabel }}
            <span v-if="tradeContractUnresolved"> · FUT mapping unresolved, fallback to spot</span>
          </p>
        </div>
        <div class="header-right">
          <button class="sync-chip" @click="manualRefresh">{{ isReloading ? 'Syncing...' : 'Sync Now' }}</button>
          <span class="micro-chip">Frames: {{ selectedTimeframes.length }}</span>
          <span class="micro-chip">Realtime: {{ Math.round(REALTIME_POLL_MS / 1000) }}s</span>
          <span class="micro-chip">Indicators: {{ Math.round(INDICATOR_POLL_MS / 1000) }}s</span>
        </div>
      </div>

      <div class="quick-ops-bar">
        <div class="quick-ops-group">
          <span class="quick-ops-label">Symbol</span>
          <select class="quick-ops-select" :value="instrumentToken" @change="selectInstrument(Number($event.target.value))">
            <option v-for="instrument in instrumentOptions" :key="`quick-instrument-${instrument.value}`" :value="instrument.value">
              {{ instrument.symbol }}
            </option>
          </select>
        </div>

        <div class="quick-ops-group">
          <span class="quick-ops-label">Timeframe</span>
          <div class="quick-ops-pills">
            <button
              v-for="tf in timeframeOptions"
              :key="`quick-tf-${tf.value}`"
              class="quick-ops-pill"
              :class="{ active: selectedTimeframes.includes(tf.value) }"
              :disabled="!selectedTimeframes.includes(tf.value) && selectedTimeframes.length >= 4"
              @click="toggleTimeframe(tf.value)"
            >
              {{ tf.label }}
            </button>
          </div>
        </div>

        <div class="quick-ops-group quick-ops-indicators">
          <button class="quick-ops-btn" @click="toggleQuickIndicatorMenu">
            Indicators {{ enabledIndicatorCount }}/{{ indicatorCatalog.length }}
          </button>
          <div v-if="isQuickIndicatorMenuOpen" class="quick-indicator-menu">
            <div class="quick-indicator-menu-head">
              <strong>Indicator Toggles</strong>
              <div class="pager-actions">
                <button class="ghost-btn tiny" @click="setAllIndicatorsEnabled(true)">All</button>
                <button class="ghost-btn tiny" @click="setAllIndicatorsEnabled(false)">None</button>
              </div>
            </div>
            <div class="quick-indicator-list">
              <label class="quick-indicator-row" v-for="item in indicatorCatalog" :key="`quick-ind-${item.name}`">
                <input
                  type="checkbox"
                  :checked="isIndicatorEnabled(item.name)"
                  @change="setIndicatorEnabled(item.name, $event.target.checked)"
                />
                <span>{{ item.name.toUpperCase() }}</span>
              </label>
            </div>
          </div>
        </div>

        <div class="quick-ops-group">
          <span class="quick-ops-label">Signal</span>
          <select class="quick-ops-select" v-model="activeSignalStrategy">
            <option value="">All YAML Signals</option>
            <option v-for="item in validSignalStrategies" :key="`quick-strategy-${item.name}`" :value="item.name">
              {{ item.name }}
            </option>
          </select>
        </div>

        <div class="quick-ops-group">
          <span class="quick-ops-label">P&amp;L Scope</span>
          <select class="quick-ops-select" v-model="signalOutcomeScope">
            <option value="today">Today</option>
            <option value="day">Selected Day</option>
            <option value="window">Window</option>
          </select>
          <select v-if="signalOutcomeScope === 'day'" class="quick-ops-select" v-model="selectedSignalOutcomeDay">
            <option v-for="day in availableSignalOutcomeDays" :key="`quick-day-${day}`" :value="day">{{ day }}</option>
          </select>
        </div>

        <div class="quick-ops-group quick-ops-actions">
          <button class="quick-ops-btn" @click="quickRefreshOps">Refresh All</button>
          <button class="quick-ops-btn" @click="refreshOrderBooks">Order Book</button>
          <button class="quick-ops-btn" @click="toggleControlDrawer">Controls</button>
          <button class="quick-ops-btn" @click="toggleInsightDrawer">Insights</button>
        </div>
      </div>

      <section class="charts-zone">
        <div class="chart-grid" :style="layoutStyle">
          <article v-for="state in chartStates" :key="state.timeframe" class="chart-card">
            <header class="chart-card-head">
              <div>
                <h3>{{ state.timeframe }} Structure</h3>
                <p>Last candle: {{ formatIST(normalizeTimestamp(state.candles[state.candles.length - 1]?.timestamp)) }}</p>
              </div>
              <div class="chart-meta">
                <span class="mini-pill">{{ state.paneCount }} panes</span>
                <span v-if="state.isLoadingRange" class="mini-pill active">
                  {{ state.loadingReason === 'history' ? 'Backfill' : 'Realtime' }}
                </span>
                <span v-if="state.error" class="mini-pill bad">Issue</span>
              </div>
            </header>

            <div class="chart-host" :id="`chart-container-${state.timeframe}`">
              <div
                v-if="state.isLoadingRange && getLoadingRangeStyle(state)"
                class="range-loading-overlay"
                :style="getLoadingRangeStyle(state)"
              ></div>
            </div>

            <footer class="chart-card-foot">
              <span>Candles: {{ state.candles.length }}</span>
              <span v-if="state.isLoadingRange">Window: {{ state.loadingRangeLabel }}</span>
              <span v-else-if="state.error" class="bad-text">{{ state.error }}</span>
              <span v-else class="good-text">Stable feed</span>
            </footer>
          </article>
        </div>
      </section>
    </section>

    <footer class="terminal-footer">
      <span class="footer-led" :class="`footer-led-${marketPhase.tone}`" :title="`Market: ${marketPhase.label}`"></span>
      <span class="footer-led" :class="`footer-led-${connectionState}`" :title="`Connection: ${connectionState}`"></span>
      <span class="footer-item mono" :title="'Latest candle epoch (seconds)'">cndl@{{ latestCandleEpoch ?? '--' }}</span>
      <span class="footer-item mono" :title="'Age of latest candle in local cache'">cndl_age {{ latestCandleAgeLabel }}</span>
      <span class="footer-item mono" :title="'Time since last backend API response'">api_age {{ apiResponseAgeLabel }}</span>
      <span class="footer-item mono" :title="'Latest API round-trip time'">api_rtt {{ apiLatencyLabel }}</span>
      <span class="footer-item mono">sym {{ activeInstrument.symbol }}</span>
    </footer>

    <div v-if="uiError" class="error-banner">
      {{ uiError }}
    </div>

    <div v-if="isControlDrawerOpen || isInsightDrawerOpen" class="drawer-backdrop" @click="closeDrawers"></div>

    <aside class="drawer drawer-left" :class="{ open: isControlDrawerOpen }">
      <div class="drawer-head">
        <h2>Control Panel</h2>
        <button class="close-btn" @click="isControlDrawerOpen = false">Close</button>
      </div>

      <div class="drawer-section insight-section insight-section-snapshot">
        <label class="field-label">Instrument</label>
        <div class="watchlist-pills">
          <button
            v-for="instrument in instrumentOptions"
            :key="instrument.value"
            class="watch-pill"
            :class="{ active: instrument.value === instrumentToken }"
            @click="selectInstrument(instrument.value)"
          >
            <span>{{ instrument.symbol }}</span>
            <small>{{ instrument.exchange }}</small>
          </button>
        </div>
      </div>

      <div class="drawer-section insight-section insight-section-signal">
        <label class="field-label">Timeframes (max 4)</label>
        <div class="timeframe-pills">
          <button
            v-for="tf in timeframeOptions"
            :key="tf.value"
            class="tf-pill"
            :class="{ active: selectedTimeframes.includes(tf.value) }"
            :disabled="!selectedTimeframes.includes(tf.value) && selectedTimeframes.length >= 4"
            @click="toggleTimeframe(tf.value)"
          >
            {{ tf.label }}
          </button>
        </div>
      </div>

      <div class="drawer-section">
        <label class="field-label">Data Range</label>
        <div class="range-controls">
          <label class="toggle-line">
            <input v-model="useFullRange" type="checkbox" />
            Full history
          </label>
          <input v-model="rangeFromInput" type="datetime-local" :disabled="useFullRange" />
          <input v-model="rangeToInput" type="datetime-local" :disabled="useFullRange" />
          <button class="primary-btn" @click="applyTimeRange">Apply Range</button>
        </div>
      </div>

      <div class="drawer-section">
        <div class="rail-card indicator-menu-card">
          <h2>Indicators</h2>
          <div class="table-actions">
            <span class="cell-sub">enabled {{ enabledIndicatorCount }}/{{ indicatorCatalog.length }}</span>
            <div class="pager-actions">
              <button class="ghost-btn tiny" @click="setAllIndicatorsEnabled(true)" :disabled="indicatorCatalog.length === 0">
                All
              </button>
              <button class="ghost-btn tiny" @click="setAllIndicatorsEnabled(false)" :disabled="indicatorCatalog.length === 0">
                None
              </button>
            </div>
          </div>
          <div v-if="indicatorCatalog.length === 0" class="empty-state">No indicators loaded yet.</div>
          <div v-else class="indicator-menu-list">
            <label class="indicator-toggle-row" v-for="item in indicatorCatalog" :key="`indicator-toggle-${item.name}`">
              <input
                type="checkbox"
                :checked="isIndicatorEnabled(item.name)"
                @change="setIndicatorEnabled(item.name, $event.target.checked)"
              />
              <span class="indicator-toggle-name">{{ item.name.toUpperCase() }}</span>
              <span class="cell-sub">{{ item.timeframes.join(', ') }}</span>
            </label>
          </div>
        </div>
      </div>

      <div class="drawer-section">
        <div class="rail-card universe-card">
          <h2>Universe Explorer</h2>
          <p v-if="universeError" class="universe-error">{{ universeError }}</p>

          <div class="meta-grid">
            <div>
              <span>Instruments</span>
              <strong>{{ universeMeta?.instruments_count || '--' }}</strong>
            </div>
            <div>
              <span>Indexes</span>
              <strong>{{ universeMeta?.indexes_count || '--' }}</strong>
            </div>
            <div>
              <span>Mapped</span>
              <strong>{{ universeMeta?.index_mapped_count || '--' }}</strong>
            </div>
            <div>
              <span>Version</span>
              <strong>{{ universeMeta?.version || '--' }}</strong>
            </div>
          </div>
          <p class="meta-caption">
            Ingested: {{ universeMeta?.ingested_at || (universeMetaLoading ? 'Loading...' : '--') }}
          </p>

          <div class="explorer-tabs">
            <button class="tab-btn" :class="{ active: universePanelTab === 'instruments' }" @click="universePanelTab = 'instruments'">
              Instruments
            </button>
            <button class="tab-btn" :class="{ active: universePanelTab === 'indexes' }" @click="universePanelTab = 'indexes'">
              Indexes
            </button>
          </div>

          <div v-if="universePanelTab === 'instruments'" class="panel-stack">
            <div class="input-grid input-grid-2">
              <input v-model="instrumentExchange" class="drawer-input" placeholder="Exchange (NSE)" />
              <input v-model="instrumentType" class="drawer-input" placeholder="Type (EQ)" />
            </div>
            <div class="input-grid input-grid-1">
              <input v-model="instrumentSegment" class="drawer-input" placeholder="Segment (optional)" />
              <input v-model="instrumentSearch" class="drawer-input" placeholder="Search symbol / name" />
            </div>
            <div class="table-actions">
              <button class="ghost-btn small" @click="runInstrumentSearch">
                {{ universeInstrumentsLoading ? 'Searching...' : 'Search' }}
              </button>
            </div>

            <div class="table-wrap">
              <table class="mini-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Token</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="instrument in universeInstrumentRows" :key="instrument.instrument_token">
                    <td>
                      <div class="cell-title">{{ instrument.tradingsymbol }}</div>
                      <div class="cell-sub">{{ instrument.name }}</div>
                    </td>
                    <td>{{ instrument.instrument_token }}</td>
                    <td>
                      <button class="ghost-btn tiny" @click="useUniverseInstrument(instrument)">Use</button>
                    </td>
                  </tr>
                  <tr v-if="!universeInstrumentsLoading && universeInstrumentRows.length === 0">
                    <td colspan="3" class="empty-row">No instruments found.</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div class="pager-line">
              <span>{{ instrumentPageFrom }}-{{ instrumentPageTo }} / {{ universeInstrumentTotal }}</span>
              <div class="pager-actions">
                <button class="ghost-btn tiny" :disabled="instrumentOffset === 0" @click="prevInstrumentPage">Prev</button>
                <button
                  class="ghost-btn tiny"
                  :disabled="instrumentOffset + instrumentLimit >= universeInstrumentTotal"
                  @click="nextInstrumentPage"
                >
                  Next
                </button>
              </div>
            </div>
          </div>

          <div v-else class="panel-stack">
            <div class="table-actions">
              <input v-model="indexSearchQuery" class="drawer-input" placeholder="Search index (NIFTY...)" />
              <button class="ghost-btn small" @click="fetchUniverseIndexes">
                {{ universeIndexesLoading ? 'Loading...' : 'Refresh' }}
              </button>
            </div>

            <div class="index-list">
              <button
                v-for="indexItem in filteredIndexes"
                :key="indexItem.index_name"
                class="index-item"
                :class="{ active: selectedIndexName === indexItem.index_name }"
                @click="openIndexDetails(indexItem.index_name)"
              >
                <div>
                  <strong>{{ indexItem.index_name }}</strong>
                  <p>
                    {{ indexItem.mapped_count }}/{{ indexItem.symbols_count }} mapped · {{ indexItem.weight_mode }}
                  </p>
                </div>
              </button>
              <p v-if="!universeIndexesLoading && filteredIndexes.length === 0" class="empty-row">No indexes matched.</p>
            </div>

            <div v-if="selectedIndexDetails" class="index-meta">
              <p>Selected: <strong>{{ selectedIndexDetails.index_name }}</strong></p>
              <p>Weights as of: {{ selectedIndexDetails.weight_asof || '--' }}</p>
              <p>Weighted symbols: {{ selectedIndexDetails.weighted_symbols_count || '--' }}</p>
            </div>

            <div class="table-wrap">
              <table class="mini-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Token</th>
                    <th>W%</th>
                    <th>Px</th>
                    <th>Δ%</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in universeConstituents" :key="`${selectedIndexName}-${item.symbol}`">
                    <td>{{ item.symbol }}</td>
                    <td>{{ item.instrument_token }}</td>
                    <td>{{ formatNumber(item.weight_pct) }}</td>
                    <td>{{ formatNumber(item.last_price) }}</td>
                    <td :class="Number(item.pchange) >= 0 ? 'good-text' : 'bad-text'">{{ formatNumber(item.pchange) }}</td>
                  </tr>
                  <tr v-if="!universeConstituentsLoading && universeConstituents.length === 0">
                    <td colspan="5" class="empty-row">No constituents loaded.</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      <div class="drawer-section">
        <div class="rail-card">
          <h2>Market Pulse</h2>
          <p class="rail-instrument">{{ activeInstrument.symbol }}</p>
          <p class="rail-subtitle">{{ activeInstrument.label }}</p>
          <p class="rail-caption">Primary feed: {{ activeInstrument.exchange }} cash market</p>
          <div class="rail-grid">
            <div>
              <span>Frames</span>
              <strong>{{ selectedTimeframes.length }}</strong>
            </div>
            <div>
              <span>Poll</span>
              <strong>{{ Math.round(REALTIME_POLL_MS / 1000) }}s</strong>
            </div>
            <div>
              <span>Indicators</span>
              <strong>{{ Math.round(INDICATOR_POLL_MS / 1000) }}s</strong>
            </div>
            <div>
              <span>Timezone</span>
              <strong>IST</strong>
            </div>
          </div>
        </div>
      </div>

      <div class="drawer-section">
        <div class="rail-card">
          <h2>Desk Notes</h2>
          <ul class="note-list">
            <li>Scroll left on charts to request historical backfill.</li>
            <li>Range limits affect initial load and realtime continuation.</li>
            <li>Indicators are refreshed independently every 5 seconds.</li>
          </ul>
        </div>
      </div>
    </aside>

    <aside class="drawer drawer-right" :class="{ open: isInsightDrawerOpen, wide: isInsightDrawerWide }">
      <div class="drawer-head">
        <h2>Insights</h2>
        <div class="inline-actions">
          <button class="ghost-btn tiny" @click="toggleInsightWidth">{{ isInsightDrawerWide ? 'Compact' : 'Expand' }}</button>
          <button class="close-btn" @click="isInsightDrawerOpen = false">Close</button>
        </div>
      </div>

      <div class="drawer-section insight-section insight-section-snapshot">
        <div class="rail-card">
          <div class="card-head-inline">
            <h2>Per-Frame Snapshot</h2>
            <button class="ghost-btn tiny" @click="showInsightSnapshot = !showInsightSnapshot">
              {{ showInsightSnapshot ? 'Hide' : 'Show' }}
            </button>
          </div>
          <div v-if="!showInsightSnapshot" class="empty-state">Collapsed in trade ops mode.</div>
          <div v-else-if="summaryCards.length === 0" class="empty-state">No chart summary available.</div>
          <div v-else class="summary-list">
            <article v-for="card in summaryCards" :key="card.timeframe" class="summary-item">
              <div class="summary-tape">
                <strong class="summary-timeframe">{{ card.timeframe }}</strong>
                <span :class="card.tone === 'good' ? 'good-text' : 'bad-text'">
                  {{ card.change >= 0 ? '+' : '' }}{{ formatNumber(card.change) }}
                  ({{ card.pct >= 0 ? '+' : '' }}{{ formatNumber(card.pct) }}%)
                </span>
                <span class="summary-metric">C <strong>{{ formatNumber(card.close) }}</strong></span>
                <span class="summary-metric">H/L <strong>{{ formatNumber(card.high) }} / {{ formatNumber(card.low) }}</strong></span>
                <span class="summary-metric">V <strong>{{ formatNumber(card.volume) }}</strong></span>
              </div>
            </article>
          </div>
        </div>
      </div>

      <div class="drawer-section insight-section insight-section-signal">
        <div class="rail-card">
          <div class="card-head-inline">
            <h2>Signal Timeline</h2>
            <button class="ghost-btn tiny" @click="showInsightSignals = !showInsightSignals">
              {{ showInsightSignals ? 'Hide' : 'Show' }}
            </button>
          </div>
          <div v-if="!showInsightSignals" class="empty-state">Collapsed in trade ops mode.</div>
          <template v-else>
          <div class="signal-toolbar">
            <label class="cell-sub">Active</label>
            <select class="drawer-input signal-select" v-model="activeSignalStrategy">
              <option value="">All YAML Signals</option>
              <option v-for="item in validSignalStrategies" :key="`sig-strategy-${item.name}`" :value="item.name">
                {{ item.name }}
              </option>
            </select>
          </div>
          <div v-if="signalTimeline.length === 0" class="empty-state">No YAML-rule signals in current chart window.</div>
          <div v-else class="signal-timeline">
            <article class="signal-item" v-for="(signal, idx) in signalTimeline" :key="`sig-${idx}`">
              <span class="signal-time">{{ formatIST(signal.timestamp) }}</span>
              <span class="signal-badge" :class="signalBadgeClass(signal.action)">{{ normalizeSignalAction(signal.action) }}</span>
              <span class="signal-meta">{{ signal.timeframe }}</span>
              <span class="signal-meta">{{ signal.strategy || 'yaml_rule' }}</span>
              <span class="signal-meta">px {{ formatNumber(signal.price) }}</span>
              <span v-if="normalizeSignalAction(signal.action) === 'EXIT'" class="signal-meta">session reset</span>
            </article>
          </div>

          <div class="signal-trade-panel">
            <p class="signal-trade-title">Signal Outcome Pairs (BUY/SELL With EXIT Support)</p>
            <div class="signal-toolbar signal-scope-toolbar">
              <label class="cell-sub">Scope</label>
              <select class="drawer-input signal-select" v-model="signalOutcomeScope">
                <option value="today">Today (IST)</option>
                <option value="day">Selected Day</option>
                <option value="window">Current Window</option>
              </select>
              <select
                v-if="signalOutcomeScope === 'day'"
                class="drawer-input signal-select"
                v-model="selectedSignalOutcomeDay"
              >
                <option v-for="day in availableSignalOutcomeDays" :key="`signal-day-${day}`" :value="day">
                  {{ day }}
                </option>
              </select>
            </div>

            <div class="signal-outcome-summary" v-if="signalOutcomeSummary.total > 0">
              <span class="signal-meta">pairs {{ signalOutcomeSummary.total }}</span>
              <span class="signal-meta">win {{ signalOutcomeSummary.wins }}</span>
              <span class="signal-meta">loss {{ signalOutcomeSummary.losses }}</span>
              <span class="signal-meta">wr {{ formatNumber(signalOutcomeSummary.winRate) }}%</span>
              <span class="signal-meta" :class="(signalOutcomeSummary.netPnl ?? 0) >= 0 ? 'good-text' : 'bad-text'">
                net {{ formatSignedNumber(signalOutcomeSummary.netPnl) }}
              </span>
            </div>
            <div v-if="recentSignalOutcomes.length === 0" class="empty-state">Need at least one BUY->SELL or SELL->BUY signal pair.</div>
            <div v-else class="signal-trade-list">
              <article class="signal-trade-item" v-for="(outcome, idx) in recentSignalOutcomes" :key="`signal-outcome-${idx}`">
                <span class="signal-meta">{{ outcome.strategy }}</span>
                <span class="signal-meta">{{ outcome.timeframe }}</span>
                <span class="signal-meta">{{ outcome.side }}</span>
                <span class="signal-meta">in {{ formatNumber(outcome.entryPrice) }}</span>
                <span class="signal-meta">out {{ formatNumber(outcome.exitPrice) }}</span>
                <span class="signal-meta" :class="getSignalOutcomeClass(outcome)">pnl {{ formatSignedNumber(outcome.pnl) }}</span>
              </article>
            </div>

            <p class="signal-trade-title">Day-wise P&amp;L (Window)</p>
            <div v-if="signalOutcomeDailySummary.length === 0" class="empty-state">No paired outcomes yet.</div>
            <div v-else class="signal-trade-list">
              <article class="signal-trade-item" v-for="day in signalOutcomeDailySummary" :key="`signal-day-pnl-${day.dayKey}`">
                <span class="signal-meta">{{ day.dayKey }}</span>
                <span class="signal-meta">pairs {{ day.pairs }}</span>
                <span class="signal-meta">win {{ day.wins }}</span>
                <span class="signal-meta">loss {{ day.losses }}</span>
                <span class="signal-meta">wr {{ formatNumber(day.winRate) }}%</span>
                <span class="signal-meta" :class="(day.netPnl ?? 0) >= 0 ? 'good-text' : 'bad-text'">
                  net {{ formatSignedNumber(day.netPnl) }}
                </span>
              </article>
            </div>
          </div>
          </template>
        </div>
      </div>

      <div class="drawer-section insight-section insight-section-orderbook">
        <div class="rail-card">
          <h2>Order Book</h2>
          <p v-if="orderBookError" class="universe-error">{{ orderBookError }}</p>

          <div class="orderbook-topbar">
            <label class="cell-sub">Mode</label>
            <select class="drawer-input orderbook-mode-select" :value="orderBookMode" @change="changeOrderBookMode($event.target.value)">
              <option value="users-with-order-books">Users + Books</option>
              <option value="order-books">Books</option>
              <option value="users">Users</option>
              <option value="bulk">Bulk</option>
            </select>
            <button class="ghost-btn tiny" @click="showOrderBookStats = !showOrderBookStats">
              {{ showOrderBookStats ? 'Hide Stats' : 'More Stats' }}
            </button>
          </div>

          <div class="table-actions">
            <span class="footer-led" :class="orderBookSyncLedClass" />
            <span class="footer-item mono">users {{ orderBookUsersTotal }}</span>
            <span class="footer-item mono">selected {{ selectedOrderBookUsers.length }}</span>
            <span class="footer-item mono" :title="'Time since last order book sync response'">sync_age {{ orderBookAgeLabel }}</span>
          </div>

          <div v-if="showOrderBookStats" class="order-metrics">
            <span class="footer-item mono">users {{ orderBookUsersTotal }}</span>
            <span class="footer-item mono">active_pos {{ orderTerminalActivePositions.length }}</span>
            <span class="footer-item mono">pending {{ orderTerminalPendingOrders.length }}</span>
            <span class="footer-item mono">completed {{ orderTerminalCompletedOrders.length }}</span>
            <span class="footer-item mono">net {{ orderTerminalNetPositions.length }}</span>
            <span class="footer-item mono">day {{ orderTerminalDayPositions.length }}</span>
            <span class="footer-item mono">local_pending {{ localPendingOrdersCount }}</span>
            <span class="footer-item mono">broker_orders {{ brokerOrdersCount }}</span>
            <span class="footer-item mono">uniq_pos {{ brokerUniquePositionsCount }}</span>
          </div>

          <div v-if="showOrderBookStats" class="table-actions">
            <span class="footer-item mono" :title="'Last order book API response epoch (ms)'">sync@{{ orderBookLastSyncEpochMs }}</span>
          </div>

          <div class="pager-line" v-if="orderBookMode !== 'bulk'">
            <span>{{ orderBookOffset + 1 }}-{{ Math.min(orderBookOffset + orderBookLimit, orderBookUsersTotal) }} / {{ orderBookUsersTotal }}</span>
            <div class="pager-actions">
              <button class="ghost-btn tiny" :disabled="orderBookOffset === 0" @click="prevOrderBookPage">Prev</button>
              <button
                class="ghost-btn tiny"
                :disabled="orderBookOffset + orderBookLimit >= orderBookUsersTotal"
                @click="nextOrderBookPage"
              >
                Next
              </button>
            </div>
          </div>

          <div class="user-selection-bar">
            <div class="user-tab-actions">
              <button class="ghost-btn tiny" @click="selectAllOrderBookUsers" :disabled="orderBookRows.length === 0">Select All</button>
              <button class="ghost-btn tiny" @click="clearOrderBookUsers" :disabled="selectedOrderBookUsers.length === 0">Clear</button>
            </div>

            <div class="user-tab-row">
              <button
                v-for="row in orderBookRows"
                :key="`usr-${row.user_id}`"
                class="user-tab-btn"
                :class="{ active: selectedOrderBookUsers.includes(String(row.user_id)) }"
                @click="toggleOrderBookUser(String(row.user_id))"
              >
                {{ row.user_id }}
              </button>
            </div>
          </div>

          <div v-if="!orderBookLoading && orderBookRows.length === 0" class="empty-state">No users in current response.</div>
          <div v-else-if="!orderBookLoading && selectedOrderBookUsers.length === 0" class="empty-state">
            Select one or more user tabs to view positions and executed orders.
          </div>

          <div v-else class="user-panels">
            <article class="user-panel" v-for="row in activeOrderBookUserRows" :key="`panel-${row.user_id}`">
              <div class="user-panel-head">
                <strong>{{ row.user_id }}</strong>
                <div class="order-metrics">
                  <span class="footer-item mono">net {{ getUserNetPositions(row).length }}</span>
                  <span class="footer-item mono">day {{ getUserDayPositions(row).length }}</span>
                  <span class="footer-item mono">pending {{ getUserPendingOrders(row).length }}</span>
                  <span class="footer-item mono">executed {{ getUserRecentExecutedOrders(row).length }}</span>
                  <span class="footer-item mono">net_qty {{ formatNumber(getUserPanelTotals(row).netQty) }}</span>
                  <span class="footer-item mono">w {{ getUserPanelTotals(row).winners }}</span>
                  <span class="footer-item mono">l {{ getUserPanelTotals(row).losers }}</span>
                  <span
                    class="footer-item mono"
                    :class="getUserPanelTotals(row).totalPnl >= 0 ? 'good-text' : 'bad-text'"
                  >
                    total_pnl {{ formatNumber(getUserPanelTotals(row).totalPnl) }}
                  </span>
                </div>
              </div>

              <div class="explorer-tabs terminal-tabs user-subtabs">
                <button
                  class="tab-btn"
                  :class="{ active: getOrderBookUserSubTab(row.user_id) === 'net' }"
                  @click="setOrderBookUserSubTab(row.user_id, 'net')"
                >
                  Current Net + P&L
                </button>
                <button
                  class="tab-btn"
                  :class="{ active: getOrderBookUserSubTab(row.user_id) === 'day' }"
                  @click="setOrderBookUserSubTab(row.user_id, 'day')"
                >
                  Day Positions
                </button>
                <button
                  class="tab-btn"
                  :class="{ active: getOrderBookUserSubTab(row.user_id) === 'pending' }"
                  @click="setOrderBookUserSubTab(row.user_id, 'pending')"
                >
                  Pending / Live
                </button>
                <button
                  class="tab-btn"
                  :class="{ active: getOrderBookUserSubTab(row.user_id) === 'executed' }"
                  @click="setOrderBookUserSubTab(row.user_id, 'executed')"
                >
                  Recent Executed
                </button>
              </div>

              <div class="table-wrap">
                <table class="mini-table" v-if="getOrderBookUserSubTab(row.user_id) === 'net'">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Qty</th>
                      <th>Avg</th>
                      <th>M2M</th>
                      <th>P&L</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(pos, idx) in getUserNetPositions(row).slice(0, 200)" :key="`u-net-${row.user_id}-${idx}`">
                      <td class="order-symbol">{{ pos.tradingsymbol || pos.symbol || '--' }}</td>
                      <td>{{ pos.quantity ?? pos.net_quantity ?? pos.buy_quantity ?? '--' }}</td>
                      <td>{{ formatNumber(pos.average_price ?? pos.avg_price ?? pos.buy_price) }}</td>
                      <td>{{ formatNumber(pos.m2m ?? pos.mtm ?? pos.unrealised ?? pos.unrealized) }}</td>
                      <td :class="getPositionPnlClass(pos)">{{ formatNumber(getPositionPnlValue(pos)) }}</td>
                    </tr>
                    <tr v-if="!orderBookLoading && getUserNetPositions(row).length === 0">
                      <td colspan="5" class="empty-row">No net positions.</td>
                    </tr>
                  </tbody>
                </table>

                <table class="mini-table" v-else-if="getOrderBookUserSubTab(row.user_id) === 'day'">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Qty</th>
                      <th>Avg</th>
                      <th>M2M</th>
                      <th>P&L</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(pos, idx) in getUserDayPositions(row).slice(0, 200)" :key="`u-day-${row.user_id}-${idx}`">
                      <td class="order-symbol">{{ pos.tradingsymbol || pos.symbol || '--' }}</td>
                      <td>{{ pos.quantity ?? pos.net_quantity ?? pos.buy_quantity ?? '--' }}</td>
                      <td>{{ formatNumber(pos.average_price ?? pos.avg_price ?? pos.buy_price) }}</td>
                      <td>{{ formatNumber(pos.m2m ?? pos.mtm ?? pos.unrealised ?? pos.unrealized) }}</td>
                      <td :class="getPositionPnlClass(pos)">{{ formatNumber(getPositionPnlValue(pos)) }}</td>
                    </tr>
                    <tr v-if="!orderBookLoading && getUserDayPositions(row).length === 0">
                      <td colspan="5" class="empty-row">No day positions.</td>
                    </tr>
                  </tbody>
                </table>

                <table class="mini-table" v-else-if="getOrderBookUserSubTab(row.user_id) === 'pending'">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Order</th>
                      <th>Symbol</th>
                      <th>Side</th>
                      <th>Type</th>
                      <th>Origin</th>
                      <th>Price</th>
                      <th>Qty</th>
                      <th>Filled</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr
                      v-for="(order, idx) in getUserPendingOrders(row).slice(0, 100)"
                      :key="`u-pending-${row.user_id}-${idx}`"
                    >
                      <td>{{ formatOrderTime(order) }}</td>
                      <td class="mono">{{ order.order_id || order.id || '--' }}</td>
                      <td class="order-symbol">{{ order.tradingsymbol || order.symbol || '--' }}</td>
                      <td>{{ order.transaction_type || order.side || '--' }}</td>
                      <td>{{ normalizeOrderType(order) || '--' }}</td>
                      <td>{{ getOrderOriginLabel(order) }}</td>
                      <td>{{ getOrderPendingPriceLabel(order) }}</td>
                      <td>{{ order.quantity ?? order.qty ?? '--' }}</td>
                      <td>{{ order.filled_quantity ?? order.filled_qty ?? 0 }}</td>
                      <td>{{ order.status || '--' }}</td>
                    </tr>
                    <tr v-if="!orderBookLoading && getUserPendingOrders(row).length === 0">
                      <td colspan="10" class="empty-row">No pending/live orders.</td>
                    </tr>
                  </tbody>
                </table>

                <table class="mini-table" v-else>
                  <thead>
                    <tr>
                      <th>Order</th>
                      <th>Symbol</th>
                      <th>Side</th>
                      <th>Qty</th>
                      <th>Status</th>
                      <th>Time (IST)</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr
                      v-for="(order, idx) in getUserRecentExecutedOrders(row).slice(0, 100)"
                      :key="`u-exe-${row.user_id}-${idx}`"
                    >
                      <td class="order-id">{{ order.order_id || order.exchange_order_id || '--' }}</td>
                      <td>{{ order.tradingsymbol || order.symbol || '--' }}</td>
                      <td>{{ order.transaction_type || order.side || '--' }}</td>
                      <td>{{ order.filled_quantity ?? order.quantity ?? '--' }}</td>
                      <td>{{ order.status || '--' }}</td>
                      <td>{{ formatOrderTime(order) }}</td>
                    </tr>
                    <tr v-if="!orderBookLoading && getUserRecentExecutedOrders(row).length === 0">
                      <td colspan="6" class="empty-row">No executed orders.</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </article>
          </div>
        </div>
      </div>

    </aside>
  </div>
</template>

<style scoped>
.terminal-root {
  height: 100vh;
  min-height: 100vh;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  color: #dce8f7;
  font-size: 13px;
  background:
    radial-gradient(circle at 10% 5%, rgba(34, 197, 94, 0.18), transparent 36%),
    radial-gradient(circle at 92% 0%, rgba(59, 130, 246, 0.18), transparent 40%),
    linear-gradient(145deg, #030711 0%, #071223 40%, #090f1d 100%);
  font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
}

.engine-shell {
  position: relative;
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border-radius: 10px;
  border: 1px solid #1a2a45;
  background: linear-gradient(180deg, rgba(7, 12, 22, 0.96), rgba(7, 12, 22, 0.9));
  overflow: hidden;
}

.edge-toggle {
  position: absolute;
  top: 10px;
  z-index: 22;
  border: 1px solid #2d4265;
  background: rgba(8, 16, 30, 0.94);
  color: #b8cbe3;
  font-size: 0.58rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 7px 8px;
  cursor: pointer;
  writing-mode: vertical-rl;
  transform: rotate(180deg);
  border-radius: 8px;
  transition: all 0.18s ease;
}

.edge-toggle-left {
  left: 6px;
}

.edge-toggle-right {
  right: 6px;
}

.edge-toggle.active {
  border-color: rgba(52, 211, 153, 0.7);
  color: #bbf7d0;
  background: rgba(7, 31, 24, 0.88);
}

.stage-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 9px 10px 7px 10px;
  border-bottom: 1px solid #17263f;
  margin-top: 0;
}

.header-left h2 {
  margin: 0;
  font-size: 0.82rem;
  letter-spacing: 0.03em;
}

.header-left p {
  margin: 4px 0 0;
  font-size: 0.66rem;
  color: #90a7c8;
}

.trade-contract-line {
  color: #9ddfc5;
}

.trade-contract-line.bad {
  color: #fecaca;
}

.header-right {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.sync-chip {
  border-radius: 999px;
  border: 1px solid rgba(52, 211, 153, 0.6);
  background: rgba(7, 31, 24, 0.84);
  color: #bbf7d0;
  padding: 3px 8px;
  font-size: 0.58rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  cursor: pointer;
}

.micro-chip {
  border-radius: 999px;
  border: 1px solid #2a3a58;
  padding: 3px 7px;
  font-size: 0.58rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #9db5d4;
}

.quick-ops-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 7px 10px;
  border-bottom: 1px solid #17263f;
  background: rgba(7, 15, 28, 0.65);
  position: relative;
  z-index: 18;
}

.quick-ops-group {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.quick-ops-label {
  font-size: 0.6rem;
  color: #8ea5c4;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.quick-ops-select {
  border: 1px solid #2a3b58;
  background: rgba(16, 26, 43, 0.9);
  color: #dce8f7;
  border-radius: 8px;
  padding: 4px 8px;
  font-size: 0.68rem;
}

.quick-ops-pills {
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.quick-ops-pill,
.quick-ops-btn {
  border: 1px solid #2a3b58;
  background: rgba(16, 26, 43, 0.88);
  color: #dce8f7;
  border-radius: 8px;
  padding: 4px 8px;
  font-size: 0.66rem;
  cursor: pointer;
}

.quick-ops-pill.active {
  border-color: rgba(52, 211, 153, 0.7);
  color: #bbf7d0;
  background: rgba(7, 31, 24, 0.88);
}

.quick-ops-actions {
  margin-left: auto;
  gap: 6px;
}

.quick-ops-indicators {
  position: relative;
}

.quick-indicator-menu {
  position: absolute;
  top: calc(100% + 6px);
  left: 0;
  min-width: 220px;
  max-width: 280px;
  border: 1px solid #2a3b58;
  border-radius: 10px;
  background: rgba(8, 15, 28, 0.98);
  box-shadow: 0 12px 24px rgba(0, 0, 0, 0.42);
  padding: 8px;
  z-index: 30;
}

.quick-indicator-menu-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 7px;
}

.quick-indicator-menu-head strong {
  font-size: 0.68rem;
  color: #b7cce7;
}

.quick-indicator-list {
  display: flex;
  flex-direction: column;
  gap: 5px;
  max-height: 210px;
  overflow: auto;
}

.quick-indicator-row {
  display: flex;
  align-items: center;
  gap: 7px;
  border: 1px solid #22304a;
  border-radius: 7px;
  background: rgba(19, 30, 48, 0.8);
  padding: 4px 6px;
  font-size: 0.66rem;
  color: #dbeafe;
}

.field-label {
  display: block;
  margin-bottom: 8px;
  color: #95aac7;
  font-size: 0.74rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.watchlist-pills,
.timeframe-pills {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.watch-pill,
.tf-pill {
  border: 1px solid #2a3b58;
  background: rgba(16, 26, 43, 0.86);
  color: #dce8f7;
  border-radius: 10px;
  padding: 8px 10px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.watch-pill {
  display: inline-flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  min-width: 96px;
}

.watch-pill small {
  font-size: 0.64rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #8ea4c4;
}

.tf-pill {
  min-width: 48px;
  font-weight: 600;
}

.watch-pill.active,
.tf-pill.active {
  border-color: rgba(52, 211, 153, 0.65);
  background: linear-gradient(180deg, rgba(16, 185, 129, 0.22), rgba(16, 185, 129, 0.12));
  color: #bbf7d0;
}

.tf-pill:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.range-controls {
  display: grid;
  grid-template-columns: 1fr;
  gap: 8px;
  align-items: center;
}

.toggle-line {
  font-size: 0.86rem;
  color: #dce8f7;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

input[type='datetime-local'] {
  border-radius: 10px;
  border: 1px solid #31415f;
  background: #0e1a2e;
  color: #dce8f7;
  padding: 8px 10px;
}

button {
  font-family: inherit;
}

.primary-btn {
  border-radius: 10px;
  padding: 8px 12px;
  font-weight: 600;
  cursor: pointer;
  border: 1px solid transparent;
  background: linear-gradient(180deg, #22c55e, #16a34a);
  color: #052e16;
}

.ghost-btn {
  border-radius: 10px;
  padding: 7px 11px;
  font-weight: 600;
  cursor: pointer;
  border: 1px solid #2e4466;
  background: rgba(15, 24, 38, 0.9);
  color: #dce8f7;
}

.ghost-btn.small {
  font-size: 0.74rem;
  padding: 6px 10px;
}

.ghost-btn.tiny {
  font-size: 0.7rem;
  padding: 4px 8px;
  border-radius: 7px;
}

.ghost-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.error-banner {
  border: 1px solid rgba(248, 113, 113, 0.55);
  color: #fecaca;
  background: rgba(127, 29, 29, 0.35);
  border-radius: 12px;
  padding: 8px 12px;
  font-size: 0.9rem;
}

.terminal-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  min-height: 22px;
  padding: 3px 8px;
  border: 1px solid #1d2d47;
  border-radius: 8px;
  background: rgba(7, 13, 24, 0.85);
}

.footer-item {
  font-size: 0.6rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #9db5d4;
}

.footer-item.mono {
  font-family: 'IBM Plex Mono', 'SFMono-Regular', Menlo, monospace;
}

.footer-led {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  display: inline-block;
  border: 1px solid #2a3a58;
  background: #5b6d86;
}

.footer-led-good {
  background: #22c55e;
  box-shadow: 0 0 8px rgba(34, 197, 94, 0.55);
}

.footer-led-warn {
  background: #f59e0b;
  box-shadow: 0 0 8px rgba(245, 158, 11, 0.55);
}

.footer-led-bad {
  background: #f43f5e;
  box-shadow: 0 0 8px rgba(244, 63, 94, 0.55);
}

.footer-led-muted,
.footer-led-idle {
  background: #64748b;
}

.footer-led-online {
  background: #22c55e;
  box-shadow: 0 0 8px rgba(34, 197, 94, 0.55);
}

.footer-led-syncing {
  background: #f59e0b;
  box-shadow: 0 0 8px rgba(245, 158, 11, 0.55);
}

.footer-led-degraded {
  background: #f43f5e;
  box-shadow: 0 0 8px rgba(244, 63, 94, 0.55);
}

.charts-zone {
  flex: 1;
  min-width: 0;
  min-height: 0;
  padding: 6px;
}

.chart-grid {
  height: 100%;
  display: grid;
  gap: 6px;
}

.chart-card {
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border-radius: 9px;
  border: 1px solid #1a2b44;
  background: linear-gradient(180deg, rgba(6, 11, 20, 0.98), rgba(6, 11, 20, 0.92));
  overflow: hidden;
}

.chart-card-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  padding: 5px 7px;
  border-bottom: 1px solid #192843;
}

.chart-card-head h3 {
  font-size: 0.8rem;
  margin: 0;
}

.chart-card-head p {
  font-size: 0.62rem;
  color: #88a2c4;
}

.chart-meta {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px;
}

.mini-pill {
  border-radius: 999px;
  border: 1px solid #2a3a58;
  padding: 2px 6px;
  font-size: 0.58rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #9db5d4;
}

.mini-pill.active {
  border-color: rgba(56, 189, 248, 0.5);
  color: #7dd3fc;
}

.mini-pill.bad {
  border-color: rgba(248, 113, 113, 0.5);
  color: #fecaca;
}

.chart-host {
  flex: 1;
  min-height: 170px;
  position: relative;
}

.range-loading-overlay {
  position: absolute;
  top: 0;
  bottom: 0;
  background: rgba(56, 189, 248, 0.16);
  border-left: 1px solid rgba(125, 211, 252, 0.45);
  border-right: 1px solid rgba(125, 211, 252, 0.45);
  pointer-events: none;
  z-index: 4;
}

.chart-card-foot {
  padding: 4px 7px;
  border-top: 1px solid #192843;
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 0.62rem;
  color: #8ba6c7;
}

.rail-card {
  border-radius: 14px;
  border: 1px solid #1a2a45;
  background: linear-gradient(180deg, rgba(8, 15, 28, 0.95), rgba(8, 14, 24, 0.84));
  padding: 12px;
}

.rail-card h2 {
  font-size: 0.9rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #96aecd;
  margin-bottom: 10px;
}

.rail-instrument {
  font-size: 1.35rem;
  font-weight: 700;
  margin-top: 2px;
}

.rail-subtitle {
  color: #93accd;
  font-size: 0.9rem;
}

.rail-caption {
  color: #7f93b1;
  font-size: 0.78rem;
  margin-top: 8px;
}

.rail-grid {
  margin-top: 12px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.rail-grid div {
  background: rgba(19, 30, 48, 0.8);
  border: 1px solid #22304a;
  border-radius: 10px;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.rail-grid span {
  color: #87a0c0;
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.note-list {
  margin: 0;
  padding-left: 16px;
  color: #a9bdd8;
  font-size: 0.84rem;
  line-height: 1.5;
}

.summary-list {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.summary-item {
  border: 1px solid #22304a;
  border-radius: 8px;
  background: rgba(19, 30, 48, 0.8);
  padding: 6px 7px;
}

.summary-tape {
  display: flex;
  align-items: center;
  gap: 7px;
  min-width: 0;
  font-size: 0.72rem;
  white-space: nowrap;
  overflow-x: auto;
  scrollbar-width: thin;
}

.summary-timeframe {
  border: 1px solid #355076;
  border-radius: 6px;
  padding: 1px 5px;
  font-size: 0.67rem;
  color: #d7e7fb;
  letter-spacing: 0.03em;
  flex: 0 0 auto;
}

.summary-metric {
  color: #9db5d4;
  flex: 0 0 auto;
}

.summary-metric strong {
  color: #e2edfc;
  font-weight: 600;
}

.signal-timeline {
  max-height: 190px;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.signal-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}

.indicator-menu-card {
  max-height: 270px;
  overflow: hidden;
}

.indicator-menu-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 185px;
  overflow: auto;
  padding-right: 2px;
}

.indicator-toggle-row {
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid #22304a;
  border-radius: 7px;
  background: rgba(19, 30, 48, 0.8);
  padding: 5px 7px;
  font-size: 0.72rem;
}

.indicator-toggle-name {
  color: #dbeafe;
  font-weight: 600;
  letter-spacing: 0.03em;
  min-width: 84px;
}

.signal-scope-toolbar {
  flex-wrap: wrap;
}

.signal-select {
  padding: 5px 8px;
  font-size: 0.72rem;
}

.signal-item {
  border: 1px solid #22304a;
  border-radius: 8px;
  background: rgba(19, 30, 48, 0.8);
  padding: 5px 7px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.7rem;
  white-space: nowrap;
}

.signal-time {
  color: #9db5d4;
  min-width: 132px;
}

.signal-badge {
  border-radius: 999px;
  border: 1px solid #2a3a58;
  padding: 1px 7px;
  font-weight: 700;
}

.signal-buy {
  border-color: rgba(52, 211, 153, 0.7);
  color: #6ee7b7;
}

.signal-sell {
  border-color: rgba(248, 113, 113, 0.7);
  color: #fca5a5;
}

.signal-exit {
  border-color: rgba(148, 163, 184, 0.7);
  color: #cbd5e1;
}

.signal-neutral {
  border-color: rgba(148, 163, 184, 0.5);
  color: #94a3b8;
}

.signal-meta {
  color: #c6d8ef;
}

.signal-trade-panel {
  margin-top: 7px;
  border-top: 1px solid #22304a;
  padding-top: 7px;
}

.signal-trade-title {
  margin: 0 0 6px;
  color: #9db5d4;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.signal-outcome-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
  margin-bottom: 6px;
}

.signal-trade-list {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.signal-trade-item {
  border: 1px solid #22304a;
  border-radius: 8px;
  background: rgba(16, 26, 43, 0.82);
  padding: 5px 7px;
  display: flex;
  align-items: center;
  gap: 7px;
  font-size: 0.69rem;
  white-space: nowrap;
  overflow-x: auto;
}

.health-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.health-list li {
  border: 1px solid #22304a;
  border-radius: 10px;
  background: rgba(19, 30, 48, 0.8);
  padding: 8px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  font-size: 0.82rem;
}

.empty-state {
  color: #91aacb;
  font-size: 0.84rem;
}

.drawer-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(2, 6, 14, 0.6);
  z-index: 30;
}

.drawer {
  position: fixed;
  top: 0;
  bottom: 0;
  width: min(330px, 92vw);
  z-index: 40;
  background: linear-gradient(180deg, rgba(6, 12, 22, 0.98), rgba(8, 15, 28, 0.96));
  border: 1px solid #1a2a45;
  box-shadow: 0 22px 40px rgba(0, 0, 0, 0.5);
  padding: 10px 9px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  overflow-y: auto;
  transition: transform 0.25s ease;
}

.drawer-left {
  left: 0;
  transform: translateX(-102%);
}

.drawer-right {
  right: 0;
  transform: translateX(102%);
}

.drawer-right.wide {
  width: min(620px, 96vw);
}

.drawer.open {
  transform: translateX(0);
}

.drawer-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.drawer-head h2 {
  margin: 0;
  font-size: 0.82rem;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: #9ab2d2;
}

.close-btn {
  border-radius: 8px;
  border: 1px solid #2e4466;
  color: #dce8f7;
  background: rgba(15, 24, 38, 0.9);
  padding: 4px 8px;
  cursor: pointer;
  font-size: 0.66rem;
}

.drawer-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.card-head-inline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.insight-section-orderbook {
  order: 1;
}

.insight-section-signal {
  order: 2;
}

.insight-section-snapshot {
  order: 3;
}

.universe-card {
  gap: 10px;
}

.universe-error {
  margin: 0;
  color: #fecaca;
  font-size: 0.75rem;
}

.meta-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}

.meta-grid div {
  border: 1px solid #22304a;
  border-radius: 8px;
  padding: 6px;
  background: rgba(19, 30, 48, 0.8);
}

.meta-grid span {
  display: block;
  color: #87a0c0;
  font-size: 0.68rem;
  text-transform: uppercase;
}

.meta-grid strong {
  font-size: 0.88rem;
}

.meta-caption {
  margin: 0;
  color: #8ea5c4;
  font-size: 0.72rem;
}

.explorer-tabs {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}

.tab-btn {
  border-radius: 8px;
  border: 1px solid #2e4466;
  background: rgba(15, 24, 38, 0.9);
  color: #dce8f7;
  font-size: 0.74rem;
  font-weight: 600;
  padding: 7px 8px;
  cursor: pointer;
}

.tab-btn.active {
  border-color: rgba(52, 211, 153, 0.65);
  color: #bbf7d0;
}

.panel-stack {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.input-grid {
  display: grid;
  gap: 8px;
}

.input-grid-2 {
  grid-template-columns: 1fr 1fr;
}

.input-grid-1 {
  grid-template-columns: 1fr;
}

.drawer-input {
  border-radius: 8px;
  border: 1px solid #31415f;
  background: #0e1a2e;
  color: #dce8f7;
  padding: 7px 9px;
}

.table-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.inline-actions {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.terminal-tabs {
  margin-top: 6px;
}

.table-wrap {
  max-height: 260px;
  overflow: auto;
  border: 1px solid #22304a;
  border-radius: 10px;
}

.mini-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.72rem;
}

.mini-table th,
.mini-table td {
  border-bottom: 1px solid #22304a;
  padding: 6px 7px;
  text-align: left;
  vertical-align: top;
}

.mini-table th {
  position: sticky;
  top: 0;
  background: #111d31;
  color: #9db5d4;
  z-index: 1;
}

.cell-title {
  font-weight: 700;
}

.cell-sub {
  color: #87a0c0;
  font-size: 0.66rem;
}

.empty-row {
  color: #8ea5c4;
  text-align: center;
  padding: 8px;
}

.pager-line {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  font-size: 0.72rem;
  color: #8ea5c4;
}

.pager-actions {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.index-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 170px;
  overflow: auto;
}

.index-item {
  border: 1px solid #2a3b58;
  background: rgba(16, 26, 43, 0.86);
  color: #dce8f7;
  border-radius: 8px;
  padding: 7px 8px;
  text-align: left;
  cursor: pointer;
}

.index-item strong {
  font-size: 0.76rem;
}

.index-item p {
  margin: 2px 0 0;
  color: #8ea5c4;
  font-size: 0.67rem;
}

.index-item.active {
  border-color: rgba(52, 211, 153, 0.65);
  color: #bbf7d0;
}

.index-meta {
  border: 1px solid #22304a;
  border-radius: 8px;
  padding: 7px 8px;
  background: rgba(19, 30, 48, 0.8);
  font-size: 0.7rem;
  color: #9db5d4;
}

.index-meta p {
  margin: 0 0 4px;
}

.index-meta p:last-child {
  margin-bottom: 0;
}

.order-metrics {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.orderbook-topbar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.orderbook-mode-select {
  max-width: 170px;
  padding: 5px 8px;
  font-size: 0.72rem;
}

.user-selection-bar {
  position: sticky;
  top: 0;
  z-index: 2;
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 6px 0;
  background: linear-gradient(180deg, rgba(8, 15, 28, 0.98), rgba(8, 15, 28, 0.9));
}

.user-tab-actions {
  display: flex;
  align-items: center;
  gap: 6px;
}

.user-tab-row {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.user-tab-btn {
  border-radius: 7px;
  border: 1px solid #2e4466;
  background: rgba(15, 24, 38, 0.9);
  color: #dce8f7;
  font-size: 0.71rem;
  font-weight: 600;
  padding: 4px 8px;
  cursor: pointer;
}

.user-tab-btn.active {
  border-color: rgba(52, 211, 153, 0.65);
  color: #bbf7d0;
  background: rgba(14, 45, 34, 0.55);
}

.user-panels {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.user-panel {
  border: 1px solid #22304a;
  border-radius: 10px;
  background: rgba(16, 26, 43, 0.72);
  padding: 7px;
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.user-panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
}

.user-subtabs {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.order-sample-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.order-detail-list,
.position-detail-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-bottom: 6px;
}

.order-detail-row,
.position-detail-row {
  display: grid;
  grid-template-columns: 1.6fr 1.1fr 0.9fr 0.9fr 1.1fr 1.1fr;
  gap: 6px;
  align-items: center;
  font-size: 0.66rem;
  color: #b8cbe3;
  border: 1px solid #22304a;
  border-radius: 6px;
  padding: 3px 5px;
  background: rgba(16, 26, 43, 0.72);
}

.position-detail-row {
  grid-template-columns: 1.8fr 0.9fr 1fr 1fr 1fr 0.9fr;
}

.order-id {
  font-family: 'IBM Plex Mono', 'SFMono-Regular', Menlo, monospace;
  color: #8ea5c4;
}

.order-symbol {
  font-weight: 600;
  color: #dce8f7;
}

.order-chip {
  border: 1px solid #2a3b58;
  border-radius: 6px;
  padding: 2px 5px;
  font-size: 0.62rem;
  color: #b8cbe3;
  background: rgba(16, 26, 43, 0.86);
}

.order-chip-broker_synced {
  border-color: rgba(52, 211, 153, 0.5);
  color: #bbf7d0;
}

.order-chip-pending_local {
  border-color: rgba(245, 158, 11, 0.5);
  color: #fde68a;
}

.order-chip-unsynced_local {
  border-color: rgba(245, 158, 11, 0.5);
  color: #fde68a;
}

.order-chip-conflict {
  border-color: rgba(244, 63, 94, 0.55);
  color: #fecaca;
}

.good-text {
  color: #6ee7b7;
}

.bad-text {
  color: #fca5a5;
}

@media (max-width: 1320px) {
  .stage-header {
    padding-left: 36px;
    padding-right: 36px;
  }

  .edge-toggle {
    top: 8px;
  }
}

@media (max-width: 1080px) {
  .terminal-root {
    padding: 6px;
  }

  .stage-header {
    flex-direction: column;
    align-items: flex-start;
  }

  .header-right {
    justify-content: flex-start;
  }

  .quick-ops-actions {
    margin-left: 0;
  }

  .edge-toggle {
    top: 8px;
    font-size: 0.54rem;
    padding: 6px 6px;
  }
}

@media (max-width: 760px) {
  .edge-toggle {
    top: 8px;
    font-size: 0.5rem;
    padding: 5px 5px;
  }

  .chart-host {
    min-height: 220px;
  }

  .chart-grid {
    grid-template-columns: 1fr !important;
    grid-template-rows: auto !important;
  }
}
</style>
