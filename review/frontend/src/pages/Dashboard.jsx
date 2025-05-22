import React, { useState, useEffect } from 'react';
import LiveReviewCard from '../components/LiveReviewCard';
import mockData from '../data/mockData.json';
import { reviewsApi } from '../services/apiService';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';

const Dashboard = () => {
  const [reviews, setReviews] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const { theme } = useTheme();

  useEffect(() => {
    const socket = new WebSocket("ws://localhost:8001/ws/kafka");

    socket.onopen = () => {
      setLoading(false);
      setError(false);
    };

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const newReviews = Array.isArray(data) ? data : [data];
        setReviews(prev => [...newReviews, ...prev].slice(0, 10)); // Keep latest 10
      } catch (err) {
        console.error("Error parsing WebSocket message:", err);
      }
    };

    socket.onerror = (err) => {
      console.error("WebSocket error", err);
      setError("WebSocket connection error");
      setLoading(false);
    };

    socket.onclose = () => {
      console.warn("WebSocket closed");
      setError("WebSocket connection closed");
    };

    return () => {
      socket.close();
    };
  }, []);


  if (loading) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="flex justify-center items-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-t-4 border-b-4 border-primary-mediumBlue"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="bg-red-50 border border-secondary-red rounded-lg p-6 text-center">
          <p className="text-secondary-red">{error}</p>
          <button 
            className="mt-4 px-4 py-2 bg-gradient-blue-purple text-white rounded-lg hover:opacity-90 transition-opacity"
            onClick={() => window.location.reload()}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }
  
  return (
    <div className="container mx-auto px-4 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-primary-darkBlue dark:text-primary-brightBlue">Review Analytics Dashboard</h1>
        <p className="text-gray-600 dark:text-gray-300 mt-2">Real-time review monitoring</p>
      </div>
      
      <div className="mb-8">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-2xl font-bold text-primary-darkBlue dark:text-primary-brightBlue">Live Reviews</h2>
        </div>
        {reviews.length > 0 ? (
          <div className="space-y-4">
            {reviews.map((review, index) => (
              <LiveReviewCard key={`${review.reviewerID}-${index}`} review={review} />
            ))}
          </div>
        ) : (
          <div className="bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6 text-center">
            <p className="text-gray-500 dark:text-gray-400">No reviews available</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;
