import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/hubspot': {
        target: 'https://api.hubapi.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/hubspot/, ''),
      },
      '/api/apollo': {
        target: 'https://api.apollo.io',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/apollo/, ''),
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.removeHeader('origin');
            proxyReq.removeHeader('referer');
          });
        },
      },
      '/api/anthropic': {
        target: 'https://api.anthropic.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/anthropic/, ''),
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.removeHeader('origin');
            proxyReq.removeHeader('referer');
          });
        },
      },
    },
  },
})
