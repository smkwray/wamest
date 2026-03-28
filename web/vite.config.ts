import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/wamest/',
  build: {
    chunkSizeWarningLimit: 1600,
  },
})
