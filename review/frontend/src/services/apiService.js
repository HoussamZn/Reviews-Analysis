// API service for making HTTP requests to the backend
import axios from 'axios';
import config from '../config';

// Base URL from config
const API_URL = config.api.url;

// Create axios instance with default config
const api = axios.create({
  baseURL: API_URL,
  timeout: config.api.timeout,
  headers: {
    'Content-Type': 'application/json',
  },
});

// API endpoints for reviews
export const reviewsApi = {
  // Get all reviews
  getReviews: async (params = {}) => {
    try {
      const response = await api.get('/reviews', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching reviews:', error);
      throw error;
    }
  },
  
  // Get review by ID
  getReviewById: async (id) => {
    try {
      const response = await api.get(`/reviews/${id}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching review ${id}:`, error);
      throw error;
    }
  },
  
  // Get latest reviews
  getLatestReviews: async (limit = 10) => {
    try {
      const response = await api.get('/reviews/latest', { params: { limit } });
      return response.data;
    } catch (error) {
      console.error('Error fetching latest reviews:', error);
      throw error;
    }
  }
};

// API endpoints for analytics
export const analyticsApi = {
  // Get analytics summary
  getSummary: async () => {
    try {
      const response = await api.get('/analytics/summary');
      return response.data;
    } catch (error) {
      console.error('Error fetching analytics summary:', error);
      throw error;
    }
  },
  
  // Get sentiment distribution
  getSentimentDistribution: async () => {
    try {
      const response = await api.get('/analytics/sentiment');
      return response.data;
    } catch (error) {
      console.error('Error fetching sentiment distribution:', error);
      throw error;
    }  },
  
  // Get all reviews for static analytics
  getReviews: async () => {
    try {
      const response = await api.get('/reviews');
      return response.data;
    } catch (error) {
      console.error('Error fetching reviews for static analytics:', error);
      throw error;
    }
  }
};
