/// <reference types="vitest/config" />
import { configDefaults } from "vitest/config";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": { target: "http://localhost:8080", changeOrigin: true },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    // this repo lives on an exFAT drive; macOS scatters AppleDouble ._* files
    exclude: [...configDefaults.exclude, "**/._*"],
  },
});
