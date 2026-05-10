import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueDevTools from 'vite-plugin-vue-devtools'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueDevTools(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
  },
  server: {
    proxy: {
      '/candles': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/_candles': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/indicator': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/indicators': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/chart-data': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/signals': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/universe': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      },
      '/accounts': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false
      }
    }
  }
})
