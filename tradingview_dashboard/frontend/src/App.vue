<script setup>
import { ref, onMounted, watch } from 'vue'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'

// Example instrument and timeframe options
const instrumentOptions = [
  { label: 'NIFTY', value: 256265 },
  { label: 'BANKNIFTY', value: 260105 },
  { label: 'RELIANCE', value: 2885 }
]
const timeframeOptions = [
  { label: '1m', value: '1m' },
  { label: '5m', value: '5m' },
  { label: '15m', value: '15m' },
  { label: '1h', value: '1h' }
]

const chartContainer = ref(null)
const chart = ref(null)
const mainSeries = ref(null)
const indicatorSeries = ref([])

const instrumentToken = ref(256265) // Default: NIFTY
const selectedTimeframes = ref(['1m']) // Default: 1m, multi-select
const apiBase = 'http://localhost:8000' // adjust if needed

const chartStates = ref([]) // [{ timeframe, candles, indicators, chart, mainSeries, indicatorSeries }]

async function fetchCandlesForTimeframe(tf) {
  const now = Math.floor(Date.now() / 1000)
  const start = now - 60 * 60 * 6 * 20 // last 6 hours
  const end = now
  const res = await fetch(
    `${apiBase}/candles?instrument_token=${instrumentToken.value}&timeframe=${tf}&start=${start}&end=${end}&limit=2000`
  )
  const data = await res.json()
  return data.candles || []
}

async function fetchIndicatorsForTimeframe(tf) {
  const now = Math.floor(Date.now() / 1000)
  const start = now - 60 * 60 * 6 // last 6 hours
  const end = now
  const res = await fetch(
    `${apiBase}/indicators?instrument_token=${instrumentToken.value}&timeframe=${tf}&start=${start}&end=${end}&limit=200`
  )
  const data = await res.json()
  return data.indicators || []
}


function drawLastCandlePriceLines() {
  if (!mainSeries.value || candles.value.length === 0) return;

  const lastCandle = candles.value[candles.value.length - 1];
  const { close, high, low } = lastCandle;

  const existingLines = mainSeries.value.priceLines?.() || [];

  // Remove existing price lines
  existingLines.forEach(line => mainSeries.value.removePriceLine(line));

  // Create new price lines
  const closeLine = mainSeries.value.createPriceLine({
    price: close,
    color: close >= lastCandle.open ? '#00c853' : '#d50000', // green/red
    lineWidth: 1,
    lineStyle: 2, // dashed
    axisLabelVisible: true,
    title: `Close: ${close.toFixed(2)}`,
  });

  const highLine = mainSeries.value.createPriceLine({
    price: high,
    color: '#2196f3',
    lineWidth: 1,
    lineStyle: 0,
    axisLabelVisible: true,
    title: `High: ${high.toFixed(2)}`,
  });

  const lowLine = mainSeries.value.createPriceLine({
    price: low,
    color: '#ff9800',
    lineWidth: 1,
    lineStyle: 0,
    axisLabelVisible: true,
    title: `Low: ${low.toFixed(2)}`,
  });

  mainSeries.value.priceLines = () => [closeLine, highLine, lowLine];
}


function addIndicatorToChart(chart, indicator, color) {
  const seriesList = []
  indicator.columns.forEach((col, idx) => {
    if (
      col === 'value' ||
      col.startsWith('kc_') ||
      col.endsWith('ema') ||
      col.endsWith('rsi') ||
      col.endsWith('sma')
    ) {
      const scaleId = `ind-${indicator.name}-${col}`
      chart.applyOptions({
        rightPriceScale: {
          visible: true,
        },
        localization: {


          timeFormatter: (timestamp) => {
            // const date = new Date((timestamp + IST_OFFSET_MINUTES * 60) * 1000); // offset applied
            const date = new Date((timestamp + 330 * 60) * 1000); // IST offset

            return date.toLocaleString('en-IN', {
              hour: '2-digit',
              minute: '2-digit',
              day: '2-digit',
              month: 'short',
              year: 'numeric',
            });
          },

          priceFormatter: price => price.toFixed(2),
        },
      })

      const series = chart.addSeries(LineSeries, {
        color: color || ['#e91e63', '#2196f3', '#4caf50', '#ff9800', '#9c27b0'][idx % 5],
        lineWidth: 2,
        priceScaleId: scaleId,
        title: `${indicator.name.toUpperCase()}${col !== 'value' ? ' ' + col : ''}`,
      })

      chart.priceScale(scaleId).applyOptions({
        scaleMargins: { top: 0.15, bottom: 0.15 },
        borderVisible: false,
      })

      const data = indicator.values
        .filter(v => v[col] !== undefined && v[col] !== null)
        .map(v => ({
          time: Math.floor(v.timestamp),
          value: v[col],
        }))
      series.setData(data)


      seriesList.push(series)
    }
  })
  return seriesList
}

const tooltip = ref({
  visible: false,
  text: '',
  style: {}
})

function convertISTToUTC(istDate) {
  // Assuming istDate is in JS Date or string format
  const date = new Date(istDate);
  return Math.floor((date.getTime() - 5.5 * 60 * 60 * 1000) / 1000); // in seconds
}

let hoverLines = [];

function drawHoverPriceLines(candle) {
  if (!mainSeries.value) return;

  // Remove previous lines
  hoverLines.forEach(line => mainSeries.value.removePriceLine(line));
  hoverLines = [];

  if (!candle) return;

  const { high, low, close, open } = candle;

  hoverLines.push(mainSeries.value.createPriceLine({
    price: close,
    color: '#d50000',
    lineWidth: 1,
    lineStyle: 2,
    axisLabelVisible: true,
    title: `Close: ${close.toFixed(2)}`,
  }));
  hoverLines.push(mainSeries.value.createPriceLine({
    price: open,
    color: '#00c853',
    lineWidth: 1,
    lineStyle: 2,
    axisLabelVisible: true,
    title: `Open: ${open.toFixed(2)}`,
  }));

  hoverLines.push(mainSeries.value.createPriceLine({
    price: high,
    color: '#2196f3',
    lineWidth: 1,
    lineStyle: 0,
    axisLabelVisible: true,
    title: `High: ${high.toFixed(2)}`,
  }));

  hoverLines.push(mainSeries.value.createPriceLine({
    price: low,
    color: '#ff9800',
    lineWidth: 1,
    lineStyle: 0,
    axisLabelVisible: true,
    title: `Low: ${low.toFixed(2)}`,
  }));
}


function getChartLayout(n) {
  // Returns {rows, cols} for n charts
  if (n === 1) return { rows: 1, cols: 1 }
  if (n === 2) return { rows: 1, cols: 2 }
  if (n === 3 || n === 4) return { rows: 2, cols: 2 }
  return { rows: 1, cols: n }
}

async function loadDataAndRenderMulti() {
  // Remove previous chart states
  chartStates.value.forEach(state => {
    if (state.chart) state.chart.remove()
  })
  chartStates.value = []

  for (const tf of selectedTimeframes.value) {
    const candles = await fetchCandlesForTimeframe(tf)
    const indicators = await fetchIndicatorsForTimeframe(tf)
    chartStates.value.push({
      timeframe: tf,
      candles,
      indicators,
      chart: null,
      mainSeries: null,
      indicatorSeries: indicatorSeries
    })
  }

  // Render charts after DOM update
  setTimeout(() => {
    // --- Synchronization state ---
    let isSyncingTimeRange = false
    let isSyncingCrosshair = false
    let lastTimeRange = null
    let lastCrosshair = null

    chartStates.value.forEach((state, idx) => {
      const container = document.getElementById(`chart-container-${state.timeframe}`)
      if (!container) return
      // Ensure the container has width/height
      container.style.width = "100%";
      container.style.height = "100%";
      const chart = createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: { background: { color: '#fff' }, textColor: '#222' },
        grid: { vertLines: { color: '#eee' }, horzLines: { color: '#eee' } },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          tickMarkFormatter: (timestamp) => {
            const date = new Date(timestamp * 1000)
            const istString = date.toLocaleString('en-IN', {
              timeZone: 'Asia/Kolkata',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false
            })
            return istString;
          },
          timeFormatter: (timestamp) => {
            const date = new Date((timestamp + 330 * 60) * 1000); // IST offset
            const istString = date.toLocaleString('en-IN', {
              timeZone: 'Asia/Kolkata',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false
            })
            return istString;
          }
        },
        rightPriceScale: {
          scaleMargins: { top: 0.1, bottom: 0.1 },
          borderVisible: true,
        },
      })
      state.chart = chart
      state.mainSeries = chart.addSeries(CandlestickSeries, {})
      if (state.candles.length > 0) {
        const first = state.candles[0];
        console.log('First candle for', state.timeframe, 'timestamp:', first.timestamp, 'type:', typeof first.timestamp);
      }
      state.mainSeries.setData(
        state.candles.map(c => ({
          time: typeof c.timestamp === 'string' ? Math.floor(new Date(c.timestamp).getTime() / 1000) : Number(c.timestamp),
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }))
      )
      state.indicatorSeries = []
      state.indicators.forEach((ind, idx) => {
        if (ind.values && ind.values.length > 0) {
          state.indicatorSeries.push(...addIndicatorToChart(chart, ind))
        }
      })

      // --- Time range sync ---
      chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
        if (isSyncingTimeRange) return
        isSyncingTimeRange = true
        lastTimeRange = range
        chartStates.value.forEach((other, j) => {
          if (j !== idx && other.chart && range) {
            other.chart.timeScale().setVisibleRange(range)
          }
        })
        isSyncingTimeRange = false
      })

      // --- Crosshair sync ---
      chart.subscribeCrosshairMove((param) => {
        if (isSyncingCrosshair) return
        if (!param || !param.time) return
        isSyncingCrosshair = true
        lastCrosshair = param.time
        chartStates.value.forEach((other, j) => {
          if (j !== idx && other.chart && param.time) {
            other.chart.crosshair().moveTo(param.time, undefined)
          }
        })
        isSyncingCrosshair = false
      })
    })
  }, 0)
}

onMounted(async () => {
  await loadDataAndRenderMulti()
  window.addEventListener('resize', loadDataAndRenderMulti)
})

watch([instrumentToken, selectedTimeframes], async () => {
  await loadDataAndRenderMulti()
})

</script>

<template>
  <div class="app-vertical-layout">
    <!-- Multi-chart split layout -->
    <div
      class="multi-chart-section"
      :style="{
        gridTemplateRows: `repeat(${getChartLayout(selectedTimeframes.length).rows}, 1fr)`,
        gridTemplateColumns: `repeat(${getChartLayout(selectedTimeframes.length).cols}, 1fr)`
      }"
    >
      <div
        v-for="(tf, idx) in selectedTimeframes"
        :key="tf"
        class="chart-cell"
      >
        <div class="chart-title">{{ tf }} Chart</div>
        <div
          class="chart-inner-container"
          :id="`chart-container-${tf}`"
        ></div>
      </div>
    </div>
    <!-- Bottom: Tools area (blank for now) -->
    <div class="tools-section"></div>
    <!-- Sticky Bottom Nav Bar -->
    <nav class="bottom-nav">
      <div class="nav-content">
        <label>
          Instrument:
          <select v-model="instrumentToken">
            <option v-for="opt in instrumentOptions" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </option>
          </select>
        </label>
        <label>
          Timeframes:
          <select
            v-model="selectedTimeframes"
            multiple
            :size="timeframeOptions.length"
            style="min-width: 120px; max-width: 180px;"
          >
            <option
              v-for="opt in timeframeOptions"
              :key="opt.value"
              :value="opt.value"
              :disabled="selectedTimeframes.length >= 4 && !selectedTimeframes.includes(opt.value)"
            >
              {{ opt.label }}
            </option>
          </select>
          <span class="tf-hint">(max 4)</span>
        </label>
      </div>
    </nav>
  </div>
</template>

<style scoped>
.app-vertical-layout {
  display: flex;
  flex-direction: column;
  height: 100vh;
  width: 100vw;
  margin: 0;
  padding: 0;
  overflow: hidden;
  background: #f8f9fa;
}

.multi-chart-section {
  display: grid;
  width: 100vw;
  height: 60vh;
  min-height: 300px;
  background: #fff;
  border-bottom: 1px solid #e0e0e0;
  box-sizing: border-box;
  position: relative;
  z-index: 1;
  gap: 0.5rem;
  padding: 0.5rem;
}

.chart-cell {
  background: #fff;
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  position: relative;
  min-width: 0;
  min-height: 200px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  flex: 1 1 0%;
}

.chart-inner-container {
  flex: 1 1 0%;
  width: 100%;
  height: 100%;
  min-height: 120px;
}

.chart-title {
  font-size: 1rem;
  font-weight: 600;
  color: #1976d2;
  padding: 0.25rem 0.5rem;
  background: #f4f6f8;
  border-bottom: 1px solid #e0e0e0;
}

.tools-section {
  width: 100vw;
  height: 35vh;
  background: #f4f6f8;
  box-sizing: border-box;
  position: relative;
  z-index: 1;
}

.tools-section {
  width: 100vw;
  height: 50vh;
  background: #f4f6f8;
  box-sizing: border-box;
  position: relative;
  z-index: 1;
}

.bottom-nav {
  position: fixed;
  left: 0;
  bottom: 0;
  width: 100vw;
  background: #fff;
  border-top: 1px solid #e0e0e0;
  box-shadow: 0 -2px 8px rgba(0, 0, 0, 0.04);
  z-index: 10;
  padding: 0.5rem 0;
}

.nav-content {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 2rem;
}

label {
  font-size: 1rem;
  color: #222;
  margin: 0 1rem;
}

select {
  margin-left: 0.5rem;
  padding: 0.3rem 1rem 0.3rem 0.5rem;
  font-size: 1rem;
  border-radius: 4px;
  border: 1px solid #ccc;
  background: #fafbfc;
  color: #222;
}

.custom-tooltip {
  pointer-events: none;
  background: #fff;
  border: 1px solid #888;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 13px;
  color: #222;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  z-index: 1000;
  position: absolute;
}

.tf-hint {
  font-size: 0.9em;
  color: #888;
  margin-left: 0.5em;
}
</style>
