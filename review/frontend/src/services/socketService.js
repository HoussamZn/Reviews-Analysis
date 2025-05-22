// Socket.io service for real-time communication
import { io } from 'socket.io-client';
import config from '../config';

// Get configuration from config file
const SOCKET_URL = config.socket.url;
const SOCKET_OPTIONS = config.socket.options;

// Create a singleton socket instance
let socket;

export const initializeSocket = () => {
  socket = io(SOCKET_URL, SOCKET_OPTIONS);
  
  console.log('Socket initialized');
  
  socket.on('connect', () => {
    console.log('Connected to socket server');
  });
  
  socket.on('connect_error', (err) => {
    console.error('Socket connection error:', err);
  });
  
  return socket;
};

// Get the socket instance
export const getSocket = () => {
  if (!socket) {
    return initializeSocket();
  }
  return socket;
};

// Disconnect socket
export const disconnectSocket = () => {
  if (socket) {
    socket.disconnect();
    socket = null;
    console.log('Socket disconnected');
  }
};

// Listen for real-time reviews
export const subscribeToReviews = (callback) => {
  if (!socket) {
    initializeSocket();
  }
  
  socket.on('new_review', (review) => {
    callback(review);
  });
  
  return () => {
    socket.off('new_review');
  };
};
