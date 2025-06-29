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
const timeframe = ref('1m')
const apiBase = 'http://localhost:8000' // adjust if needed

const candles = ref([])
const indicators = ref([])

function fetchCandles() {
  const now = Math.floor(Date.now() / 1000)
  const start = now - 60 * 60 * 6 * 20 // last 6 hours
  const end = now
  return fetch(
    `${apiBase}/candles?instrument_token=${instrumentToken.value}&timeframe=${timeframe.value}&start=${start}&end=${end}&limit=2000`
  )
    .then(res => res.json())
    .then(data => {
      candles.value = data.candles || []
      return candles.value
    })
}

function fetchIndicators() {
  const now = Math.floor(Date.now() / 1000)
  const start = now - 60 * 60 * 6 // last 6 hours
  const end = now
  return fetch(
    `${apiBase}/indicators?instrument_token=${instrumentToken.value}&timeframe=${timeframe.value}&start=${start}&end=${end}&limit=200`
  )
    .then(res => res.json())
    .then(data => {
      indicators.value = data.indicators || []
      return indicators.value
    })
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


function renderChart() {
  if (!chartContainer.value) return
  // Remove previous chart if exists
  if (chart.value) {
    chart.value.remove()
    chart.value = null
  }
  chart.value = createChart(chartContainer.value, {
    width: chartContainer.value.clientWidth,
    height: chartContainer.value.clientHeight,
    layout: { background: { color: '#fff' }, textColor: '#222' },
    grid: { vertLines: { color: '#eee' }, horzLines: { color: '#eee' } },
    timeScale: {
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: (timestamp) => {

        // timestamp is in seconds
        const date = new Date(timestamp * 1000)
        const istString = date.toLocaleString('en-IN', {
          timeZone: 'Asia/Kolkata',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false
        })
        // For debug: log a sample conversion
        if (timestamp % 3600 === 0) {
          console.log('IST tick label for', timestamp, 'is', istString)
        }
        return istString;
      },
      timeFormatter: (timestamp) => {
        // const date = new Date(timestamp * 1000);
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

  mainSeries.value = chart.value.addSeries(CandlestickSeries, {})
  // Debug: log last candle timestamp and conversions
  if (candles.value.length > 0) {
    const last = candles.value[candles.value.length - 1];
    const ts = last.epoch !== undefined ? Number(last.epoch) : Math.floor(last.timestamp);
    const utc = new Date(ts * 1000).toUTCString();
    const ist = new Date(ts * 1000).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    console.log('Last candle timestamp:', ts, 'UTC:', utc, 'IST:', ist);
  }
  mainSeries.value.setData(
    candles.value.map(c => ({
      // time: c.epoch !== undefined ? Number(c.epoch) : Math.floor(c.timestamp),
      // time: convertISTToUTC("2025-06-29T09:15:00+05:30"),
      // time: ist,
      time: c.timestamp,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }))
  )

  // drawLastCandlePriceLines()

  indicatorSeries.value = []
  indicators.value.forEach((ind, idx) => {
    if (ind.values && ind.values.length > 0) {
      indicatorSeries.value.push(...addIndicatorToChart(chart.value, ind))
    }
  })

  // Custom crosshair tooltip for IST
  chart.value.subscribeCrosshairMove(param => {
    if (!param || !param.time || !param.point) {
      tooltip.value.visible = false
      return
    }
    const ts = typeof param.time === 'object' && param.time.unix ? param.time.unix : param.time
    // const date = new Date(ts * 1000);
    const date = new Date((ts + 330 * 60) * 1000); // IST offset

    const istString = date.toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      year: '2-digit',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    })



    for (let [series, data] of param.seriesData) {

      if (!data) {
        drawHoverPriceLines(null);
        return;
      }

      drawHoverPriceLines(data);
      const { high, low, close, open } = data;

      tooltip.value = {
        visible: true,
        text: `High ${high}<br>Low ${low}<br>Close ${close}<br>Open ${open}`,
        style: {
          position: 'absolute',
          left: (param.point.x + 20) + 'px',
          top: (param.point.y + 40) + 'px',
          background: '#fff',
          border: '1px solid #888',
          padding: '6px 10px',
          borderRadius: '6px',
          pointerEvents: 'none',
          zIndex: 1000,
          fontSize: '13px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.08)'
        }
      }

    }

    return;
    tooltip.value = {
      visible: true,
      text: istString,
      style: {
        position: 'absolute',
        left: (param.point.x + 20) + 'px',
        top: (param.point.y + 40) + 'px',
        background: '#fff',
        border: '1px solid #888',
        padding: '6px 10px',
        borderRadius: '6px',
        pointerEvents: 'none',
        zIndex: 1000,
        fontSize: '13px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)'
      }
    }
  })
}

async function loadDataAndRender() {
  await fetchCandles()
  await fetchIndicators()
  renderChart()
}

onMounted(async () => {
  await loadDataAndRender()
  window.addEventListener('resize', renderChart)
})

watch([instrumentToken, timeframe], async () => {
  await loadDataAndRender()
})

</script>

<template>
  <div class="app-vertical-layout">
    <!-- Top: Chart (50vh) -->
    <div class="chart-section" ref="chartContainer">
      <div 
      v-html="tooltip.text" 
      v-if="tooltip.visible" :style="tooltip.style" class="custom-tooltip">

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
          Timeframe:
          <select v-model="timeframe">
            <option v-for="opt in timeframeOptions" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </option>
          </select>
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

.chart-section {
  width: 100vw;
  height: 50vh;
  min-height: 250px;
  background: #fff;
  border-bottom: 1px solid #e0e0e0;
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
</style>
