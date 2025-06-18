<template>
  <div ref="chartContainer" class="tv-chart-container"></div>
  <div v-if="tooltip.visible" :style="tooltip.style" class="custom-tooltip">
    <div><strong>IST:</strong> {{ tooltip.ist }}</div>
    <div><strong>Raw:</strong> {{ tooltip.raw }}</div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts'

const props = defineProps({
  symbol: { type: String, required: true },
  timeframe: { type: String, required: true }, // e.g. '1m', '5m', '1h', '1d'
  data: { type: Array, required: true }, // [{ time, open, high, low, close, volume }]
  chartOptions: { type: Object, default: () => ({}) }
})

const emit = defineEmits(['ready', 'update'])

const chartContainer = ref(null)
let chart = null
let series = null
let volumeSeries = null

// Custom tooltip state
const tooltip = ref({
  visible: false,
  ist: '',
  raw: '',
  style: {}
})

function _mapDataToSeries(data) {
  // Lightweight Charts expects { time, open, high, low, close }
  return data.map(d => ({
    time: d.time, // UNIX timestamp (seconds)
    open: d.open,
    high: d.high,
    low: d.low,
    close: d.close
  }))
}

function mapDataToSeries(data) {
  return data
    .filter(d => d.time && !isNaN(d.time))
    .sort((a, b) => a.time - b.time) // make sure data is ordered
    .map(d => ({
      time: d.time, // already UNIX timestamp (in seconds)
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close
    }));
}

// Map volume data to { time, value, color }
function mapVolumeData(data) {
  return data
    .filter(d => d.time && !isNaN(d.time))
    .map(d => ({
      time: d.time,
      value: d.volume,
      color: d.close >= d.open ? '#26a69a' : '#ef5350'
    }));
}



onMounted(() => {
  if (!chartContainer.value) {
    console.error('Chart container element is null');
    return;
  }
  chart = createChart(chartContainer.value, {
    width: chartContainer.value.clientWidth,
    height: 350,
    
    localization: {

      timeFormatter: (timestamp) => {

          const date = new Date(timestamp * 1000)
          return date.toLocaleTimeString('en-IN', {
            timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: 'short',   // or '2-digit' if you want MM
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
          })
        }
    },
    layout: { background: { color: '#fff' }, textColor: '#222' },
    ...props.chartOptions
  });


  // ✅ Apply timeFormatter *before* adding series
  chart.timeScale().applyOptions({
    timeVisible: true,
    secondsVisible: true,
  
    
  })

  // ✅ Add series AFTER timeScale options
  series = chart.addSeries(CandlestickSeries, { timeVisible: true })
  series.setData(mapDataToSeries(props.data))

  // Add volume histogram series at the bottom (for lightweight-charts v5+)
  // volumeSeries = chart.addSeries(HistogramSeries, {
  //   priceFormat: { type: 'volume' },
  //   color: '#26a69a',
  //   priceScaleId: '', // separate scale
  //   scaleMargins: { top: 0.8, bottom: 0 }, // push to bottom
  // });
  

  // volumeSeries.setData(mapVolumeData(props.data));

  // Custom tooltip: show original timestamp string in IST if available
  chart.subscribeCrosshairMove(param => {
    if (!param || !param.seriesPrices || !param.point) {
      tooltip.value.visible = false
      return
    }
    const priceData = param.seriesPrices.get(series)
    if (!priceData) {
      tooltip.value.visible = false
      return
    }
    const hoveredTime = Number(priceData.time)
    const candle = props.data.find(d => Number(d.time) === hoveredTime)
    if (candle && candle.timestamp) {
      let ts = candle.timestamp.replace(' ', 'T')
      let date = new Date(ts)
      if (isNaN(date.getTime())) {
        date = new Date(ts + 'Z')
      }

      const istString = date.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false })
      tooltip.value = {
        visible: true,
        ist: istString,
        raw: candle.timestamp,
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
    } else {
      tooltip.value.visible = false
    }
  })

  emit('ready', { chart, series })

  // Listen for visible time range changes to trigger fetch for older candles
  chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
    if (!props.data || props.data.length === 0) return;
    const oldest = props.data[0].time;
    // If user scrolls within 10 bars of the oldest loaded candle, emit event to fetch more
    if (range && range.from !== undefined && range.from <= oldest + 10) {
      emit('fetch-older');
    }
  });

  // Responsive resize
  window.addEventListener('resize', resizeChart)
})

onBeforeUnmount(() => {
  if (chart) {
    chart.remove()
    chart = null
  }
  window.removeEventListener('resize', resizeChart)
})

function resizeChart() {
  if (chart && chartContainer.value) {
    chart.applyOptions({ width: chartContainer.value.clientWidth })
  }
}

let lastDataLength = 0

// Watch for data updates
watch(() => props.data, (newData, oldData) => {
  if (series) {
    // Save current logical range before updating data
    let prevLogicalRange = null;
    let barsAdded = 0;
    if (chart && oldData && newData && newData.length > oldData.length) {
      prevLogicalRange = chart.timeScale().getVisibleLogicalRange();
      barsAdded = newData.length - oldData.length;
    }

    series.setData(mapDataToSeries(newData));
    if (volumeSeries) {
      volumeSeries.setData(mapVolumeData(newData));
    }

    emit('update', newData);

    // If new data was prepended (older candles), preserve scroll position
    if (prevLogicalRange && barsAdded > 0) {
      // Shift the logical range right by the number of new bars added
      chart.timeScale().setVisibleLogicalRange({
        from: prevLogicalRange.from + barsAdded,
        to: prevLogicalRange.to + barsAdded
      });
    }

    lastDataLength = newData.length;
  }
}, { deep: true })

// Watch for timeframe changes (could trigger data reload in parent)
watch(() => props.timeframe, (newTf, oldTf) => {
  // Parent should handle data fetching, this just updates chart
  if (series && props.data) {
    series.setData(mapDataToSeries(props.data))
  }
})
</script>

<style scoped>
.tv-chart-container {
  width: 100vw;
  /* min-width: 300px; */
  height: 100vh;
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  background: #fff;
  margin-bottom: 1rem;
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
  overflow: hidden;
}
.custom-tooltip {
  pointer-events: none;
  background: #fff;
  border: 1px solid #888;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 13px;
  color: #222;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  z-index: 1000;
  position: absolute;
}
</style>
