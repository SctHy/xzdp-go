import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import tailwind from "@tailwindcss/vite";
export default defineConfig({
  plugins: [vue(), tailwind()],
  server: {
    proxy: {
      "^/(shops|categories|slots|bookings|user|vouchers|orders|wallet|notes|following|feed|notifications)(/|$|\\?)": "http://127.0.0.1:8080",
    },
  },
  build: { outDir: "../internal/httpapi/web", emptyOutDir: true },
});
