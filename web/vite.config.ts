import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const api = process.env.DEMO_API_URL || "http://localhost:58080";
export default defineConfig({
  root: "web",
  plugins: [react()],
  build: { outDir: "../dist/web", emptyOutDir: true },
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api": {
        target: api,
        changeOrigin: true,
        headers: { Origin: new URL(api).origin },
      },
    },
  },
});
