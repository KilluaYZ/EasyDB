import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import {resolve} from "path";
const pathSrc = resolve(__dirname, 'src')

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  css: {
    preprocessorOptions: {
      scss: {
        additionalData: '@import "@/styles/common.scss";'
      }
    }
  },
  resolve: {
    alias: {
      '@':resolve(__dirname, './src')
    }
  },
  server: {
    open: true,
    port: 2000,
    host: '0.0.0.0',
    proxy: {
      '/api': {
        target: 'http://localhost:5000/',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
