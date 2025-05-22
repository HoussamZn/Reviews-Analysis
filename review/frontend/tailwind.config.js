/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: {
          darkBlue: '#1E2761',
          mediumBlue: '#408CFF',
          lightBlue: '#7EB2FF',
          brightBlue: '#00BFFF',
        },
        secondary: {
          purple: '#7B68EE',
          teal: '#40E0D0',
          red: '#FF5757',
        },
        background: {
          darkNavy: '#0A1929',
          light: '#F8F9FA',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      backgroundImage: {
        'gradient-blue-purple': 'linear-gradient(90deg, #408CFF 0%, #7B68EE 100%)',
        'gradient-teal-blue': 'linear-gradient(90deg, #40E0D0 0%, #00BFFF 100%)',
      },
      boxShadow: {
        card: '0 4px 6px -1px rgba(10, 25, 41, 0.1), 0 2px 4px -1px rgba(10, 25, 41, 0.06)',
        'card-hover': '0 10px 15px -3px rgba(10, 25, 41, 0.1), 0 4px 6px -2px rgba(10, 25, 41, 0.05)',
      },
    },
  },
  plugins: [],
}
