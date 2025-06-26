<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import TradingViewChart from './components/TradingViewChart.vue'

// Example chart configs (can be made dynamic)
const availableSymbols = ['NIFTY', 'BANKNIFTY', 'TCS']
const availableTimeframes = ['1m', '5m', '30m', '15m', '1h', '1d']

const charts = ref([
  { id: 1, symbol: 'NIFTY', timeframe: '1m', data: [] },
  // { id: 2, symbol: 'BANKNIFTY', timeframe: '5m', data: [] }
])

let nextId = 3

// Polling interval in milliseconds (default 1 second)
const pollInterval = ref(1000)

// Store polling interval IDs for cleanup
const pollingIntervals = ref({})

function addChart() {
  charts.value.push({
    id: nextId++,
    symbol: availableSymbols[0],
    timeframe: availableTimeframes[0],
    data: []
  })
}

function removeChart(id) {
  charts.value = charts.value.filter(c => c.id !== id)
}

const symbolToToken = {
  NIFTY: 256265,
  BANKNIFTY: 260105,
  TCS: 2953217
}

async function fetchChartData(symbol, timeframe, start = null, end = null, limit = 100) {
  const instrument_token = symbolToToken[symbol]
  if (!instrument_token) return []

  const tfSeconds = {
    '1m': 60,
    '5m': 300,
    '15m': 900,
    '1h': 3600,
    '1d': 86400
  }[timeframe] || 60

  let _end = end
  let _start = start

  if (!_end) {
    // Fetch latest epoch from backend
    try {
      const latestResp = await fetch(`/candles/latest?instrument_token=${instrument_token}&timeframe=${timeframe}`)
      const latestJson = await latestResp.json()
      if (latestJson && latestJson.epoch) {
        _end = Number(latestJson.epoch)
      } else {
        // No data available
        return []
      }
    } catch (e) {
      console.error('Failed to fetch latest candle', e)
      return []
    }
    _end = -1
  }
  if (!_start) {
    // _start = _end - tfSeconds * limit;
    _start = -1;
  }

  const url = `/candles?instrument_token=${instrument_token}&timeframe=${timeframe}&start=${_start}&end=${_end}&limit=${limit}`
  try {
    const resp = await fetch(url)
    const json = await resp.json()
    if (!json.candles) return []
    // Map backend format to chart format using the candle's original timestamp string
    return json.candles
      .map(c => ({
        time: Number(c.epoch), // Use epoch directly, which is UTC and matches the true candle time
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        volume: c.volume,
        timestamp: c.timestamp // Pass original timestamp string for tooltip
      }))
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('Failed to fetch chart data', e)
    return []
  }
}

async function loadAllChartData() {
  for (const chart of charts.value) {
    chart.data = await fetchChartData(chart.symbol, chart.timeframe)
    chart.latestEpoch = chart.data.length > 0 ? chart.data[chart.data.length - 1].time : null
    chart.oldestEpoch = chart.data.length > 0 ? chart.data[0].time : null
  }
}

// Fetch older candles for pagination (prepend to chart)
async function fetchOlderCandles(chart, count = 1000) {
  if (!chart.oldestEpoch) return
  const tfSeconds = {
    '1m': 60,
    '5m': 300,
    '15m': 900,
    '1h': 3600,
    '1d': 86400
  }[chart.timeframe] || 60

  let attempts = 0
  let found = false
  let oldestEpoch = chart.oldestEpoch
  // Increase skip window to 5x count per retry to jump over large gaps
  const skipMultiplier = 1
  const maxAttempts = 20

  while (attempts < maxAttempts && !found && oldestEpoch > 0) {
    const end = oldestEpoch - tfSeconds
    const start = end - tfSeconds * count * skipMultiplier
    const moreData = await fetchChartData(chart.symbol, chart.timeframe, start, end, count * skipMultiplier)
    // Debug: log the fetch range and result
    // eslint-disable-next-line no-console
    console.log(`[fetchOlderCandles] symbol=${chart.symbol}, timeframe=${chart.timeframe}, start=${start}, end=${end}, fetched=${moreData.length}`)
    if (moreData.length > 0) {
      // Merge, sort, and deduplicate by time (strictly ascending)
      const merged = [...moreData, ...chart.data]
        .sort((a, b) => a.time - b.time)
        .filter((item, idx, arr) => idx === 0 || item.time > arr[idx - 1].time)
      chart.data = merged
      // Set oldestEpoch to the minimum time in the merged data
      chart.oldestEpoch = Math.min(...chart.data.map(d => d.time))
      found = true
    } else {
      // No data found, skip further back
      oldestEpoch = end - tfSeconds * count * skipMultiplier
      attempts += 1
    }
  }
}

async function startPollingLatest(chart) {
  // Clear any existing interval for this chart
  if (pollingIntervals.value[chart.id]) {
    clearInterval(pollingIntervals.value[chart.id])
  }
  // Poll for latest data
  pollingIntervals.value[chart.id] = setInterval(async () => {
    // Get today's midnight (UTC, since backend uses epoch UTC)
    const now = new Date();
    const todayMidnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
    const todayEpoch = Math.floor(todayMidnight.getTime() / 1000);

    // Fetch all candles for today (from midnight to now)
    const latestData = await fetchChartData(chart.symbol, chart.timeframe, todayEpoch, null, 1000);

    // Remove all candles for today from chart.data
    const oldData = chart.data.filter(c => c.time < todayEpoch);

    // Upsert: combine old data with today's latest data, sort, dedupe
    let merged = [...oldData, ...latestData]
      .sort((a, b) => a.time - b.time)
      .filter((item, idx, arr) => idx === 0 || item.time > arr[idx - 1].time);

    chart.data = merged;
    chart.latestEpoch = chart.data.length > 0 ? chart.data[chart.data.length - 1].time : null;
    chart.oldestEpoch = chart.data.length > 0 ? chart.data[0].time : null;
  }, pollInterval.value)
}

// Initial load
async function initializeCharts() {
  await loadAllChartData()
  // Start polling for each chart
  for (const chart of charts.value) {
    startPollingLatest(chart)
  }
}

onMounted(() => {
  initializeCharts()
})

onBeforeUnmount(() => {
  // Clear all polling intervals
  Object.values(pollingIntervals.value).forEach(id => clearInterval(id))
})

// Watch for symbol/timeframe changes and reload data
async function onChartConfigChange(chart) {
  chart.data = await fetchChartData(chart.symbol, chart.timeframe)
  // Restart polling for this chart with new config
  startPollingLatest(chart)
}
</script>

<template>
  <!-- <header>
    <img alt="Vue logo" class="logo" src="./assets/logo.svg" width="125" height="125" />
    <h1>Algo Trading Dashboard</h1>
  </header> -->

  <main>
    <button @click="addChart">Add Chart</button>
    <div class="charts-grid">
      <div v-for="chart in charts" :key="chart.id" class="chart-card">
        <div class="chart-controls">
          <label>
            Symbol:
            <select v-model="chart.symbol" @change="onChartConfigChange(chart)">
              <option v-for="s in availableSymbols" :key="s" :value="s">{{ s }}</option>
            </select>
          </label>
          <label>
            Timeframe:
            <select v-model="chart.timeframe" @change="onChartConfigChange(chart)">
              <option v-for="tf in availableTimeframes" :key="tf" :value="tf">{{ tf }}</option>
            </select>
          </label>
          <button @click="removeChart(chart.id)">Remove</button>
        </div>
        <TradingViewChart
          :symbol="chart.symbol"
          :timeframe="chart.timeframe"
          :data="chart.data"
          @fetch-older="fetchOlderCandles(chart)"
        />
      </div>
    </div>
  </main>
</template>

<style scoped>
header {
  line-height: 1.5;
  text-align: center;
  margin-bottom: 2rem;
}

.logo {
  display: block;
  margin: 0 auto 1rem;
}

.charts-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 2rem;
  justify-content: flex-start;
  
}

.chart-card {
  background: #fafbfc;
  border: 1px solid #e0e0e0;
  border-radius: 10px;
  padding: 1rem;
  /* min-width: 350px; */
  /* max-width: 420px; */
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
  display: flex;
  flex-direction: column;
  align-items: stretch;
}

.chart-controls {
  display: flex;
  gap: 1rem;
  align-items: center;
  margin-bottom: 0.5rem;
}

button {
  background: #1976d2;
  color: #fff;
  border: none;
  border-radius: 4px;
  padding: 0.4rem 1rem;
  cursor: pointer;
  font-weight: 500;
  transition: background 0.2s;
}
button:hover {
  background: #125ea2;
}
</style>
