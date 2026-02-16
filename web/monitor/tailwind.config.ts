import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{svelte,ts}'],
  theme: {
    extend: {},
  },
  plugins: [],
} satisfies Config;
