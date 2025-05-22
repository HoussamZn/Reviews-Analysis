// Application-wide configuration settings
const config = {
  // API configuration
  api: {
    url: import.meta.env.VITE_API_URL || 'http://localhost:5000/api',
    timeout: 10000, // 10 seconds
  },
  
  // Socket.io configuration
  socket: {
    url: import.meta.env.VITE_SOCKET_URL || 'http://localhost:5000',
    options: {
      reconnectionDelay: 1000,
      reconnection: true,
      reconnectionAttempts: 10,
      transports: ['websocket'],
      agent: false,
      upgrade: false,
      rejectUnauthorized: false,
    },
  },
  
  // Feature flags
  features: {
    useMockData: import.meta.env.VITE_USE_MOCK === 'true',
    enableRealTimeUpdates: true,
    enableNotifications: true,
    enableFiltering: true,
  },
    // UI configuration
  ui: {
    theme: 'light', // 'light' or 'dark'
    animationsEnabled: true,
    maxReviewsPerPage: 10,
    colors: {
      // Primary colors
      primary: {
        darkBlue: '#1E2761',
        mediumBlue: '#408CFF',
        lightBlue: '#7EB2FF',
        brightBlue: '#00BFFF',
      },
      // Secondary colors
      secondary: {
        purple: '#7B68EE',
        teal: '#40E0D0',
        red: '#FF5757',
      },
      // Background colors
      background: {
        darkNavy: '#0A1929',
        light: '#F8F9FA',
      },
      // Gradients
      gradients: {
        bluePurple: 'linear-gradient(90deg, #408CFF 0%, #7B68EE 100%)',
        tealBlue: 'linear-gradient(90deg, #40E0D0 0%, #00BFFF 100%)',
      },
    },
    sentimentColors: {
      Positive: '#40E0D0', // teal
      Neutral: '#7EB2FF',  // light blue
      Negative: '#FF5757', // red
    },
  },
};

export default config;
