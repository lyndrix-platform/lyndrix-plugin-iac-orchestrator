import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'

export default defineConfig({
  plugins: [react({ jsxRuntime: 'classic' })],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/ui/index.tsx'),
      name: '__lyndrix_plugin_lyndrix_plugin_iac_orchestrator',
      formats: ['iife'],
      fileName: () => 'ui_bundle.js',
    },
    outDir: 'ui_static',
    emptyOutDir: true,
    rollupOptions: {
      external: ['react', 'react-dom', 'react-dom/client'],
      output: {
        globals: {
          react: '__lyndrix_react',
          'react-dom': '__lyndrix_react',
          'react-dom/client': '__lyndrix_react_dom_client',
        },
      },
    },
  },
})
