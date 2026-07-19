/** @type {import('tailwindcss').Config} */
export default {
  prefix: "xf-",
  important: "#xflow-root",
  corePlugins: {
    preflight: false,
  },
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {},
  },
  plugins: [],
};
