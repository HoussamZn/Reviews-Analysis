import React, { useState, useEffect } from 'react';
import { Bar, Pie, Line } from 'react-chartjs-2';
import { analyticsApi } from '../services/apiService';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';
import mockStaticData from '../data/mockStaticData.json';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

const StaticAnalytics = () => {
  const { theme } = useTheme();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // State for different analytics data
  const [reviewsData, setReviewsData] = useState([]);
  const [summaryStats, setSummaryStats] = useState({
    totalReviews: 0,
    avgRating: 0,
    positivePercentage: 0,
    negativePercentage: 0,
    neutralPercentage: 0,
  });
  
  // State for charts data
  const [sentimentDistribution, setSentimentDistribution] = useState({
    labels: ['Positive', 'Negative', 'Neutral'],
    datasets: [{
      data: [0, 0, 0],
      backgroundColor: ['#4ade80', '#f87171', '#a3a3a3'],
    }]
  });
  
  const [reviewerActivity, setReviewerActivity] = useState({
    labels: [],
    datasets: [{
      label: 'Reviews by Reviewer',
      data: [],
      backgroundColor: '#60a5fa',
    }]
  });
  
  const [productPopularity, setProductPopularity] = useState({
    labels: [],
    datasets: [{
      label: 'Reviews by Product',
      data: [],
      backgroundColor: '#8b5cf6',
    }]
  });
  
  const [timeSeriesData, setTimeSeriesData] = useState({
    labels: [],
    datasets: [{
      label: 'Reviews Over Time',
      data: [],
      borderColor: '#f59e0b',
      backgroundColor: 'rgba(245, 158, 11, 0.1)',
      tension: 0.3,
    }]
  });
  // Fetch static analytics data
  useEffect(() => {
    const fetchAnalyticsData = async () => {
      try {
        setLoading(true);
        
        // Use mock data if enabled in config
        if (config.features.useMockData) {
          // Process the mock data
          processAnalyticsData(mockStaticData);
          setReviewsData(mockStaticData);
          setLoading(false);
          return;
        }
        
        // Fetch reviews data from API
        const reviewsResponse = await analyticsApi.getReviews();
        setReviewsData(reviewsResponse);
        
        // Process the data for various analytics
        processAnalyticsData(reviewsResponse);
        
        setLoading(false);
      } catch (err) {
        console.error('Error fetching analytics data:', err);
        setError('Failed to load analytics data. Please try again later.');
        setLoading(false);
      }
    };
    
    fetchAnalyticsData();
  }, []);
  
  // Process the raw data into chart data
  const processAnalyticsData = (data) => {
    if (!data || data.length === 0) return;
    
    // Process summary statistics
    const totalReviews = data.length;
    const sentimentCounts = {
      Positive: 0,
      Negative: 0,
      Neutral: 0
    };
    
    // Process reviewer and product data
    const reviewerMap = new Map();
    const productMap = new Map();
    
    // Process time series data
    const timeMap = new Map();
    
    // Process each review
    data.forEach(review => {
      // Count sentiments
      if (review.prediction) {
        sentimentCounts[review.prediction] = (sentimentCounts[review.prediction] || 0) + 1;
      }
      
      // Count reviews by reviewer
      if (review.reviewerID) {
        reviewerMap.set(review.reviewerID, (reviewerMap.get(review.reviewerID) || 0) + 1);
      }
      
      // Count reviews by product
      if (review.asin) {
        productMap.set(review.asin, (productMap.get(review.asin) || 0) + 1);
      }
      
      // Group by time (month/year)
      if (review.unixReviewTime) {
        const date = new Date(parseInt(review.unixReviewTime) * 1000);
        const monthYear = `${date.getMonth() + 1}/${date.getFullYear()}`;
        timeMap.set(monthYear, (timeMap.get(monthYear) || 0) + 1);
      }
    });
    
    // Calculate percentages for sentiment
    const positivePercentage = (sentimentCounts.Positive / totalReviews) * 100;
    const negativePercentage = (sentimentCounts.Negative / totalReviews) * 100;
    const neutralPercentage = (sentimentCounts.Neutral / totalReviews) * 100;
    
    // Update summary stats
    setSummaryStats({
      totalReviews,
      avgRating: 0, // Need to calculate if you have rating data
      positivePercentage,
      negativePercentage,
      neutralPercentage
    });
    
    // Update sentiment distribution chart
    setSentimentDistribution({
      labels: ['Positive', 'Negative', 'Neutral'],
      datasets: [{
        data: [sentimentCounts.Positive, sentimentCounts.Negative, sentimentCounts.Neutral],
        backgroundColor: ['#4ade80', '#f87171', '#a3a3a3'],
      }]
    });
    
    // Get top reviewers
    const topReviewers = [...reviewerMap.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    
    setReviewerActivity({
      labels: topReviewers.map(([id]) => id),
      datasets: [{
        label: 'Reviews by Reviewer',
        data: topReviewers.map(([, count]) => count),
        backgroundColor: '#60a5fa',
      }]
    });
    
    // Get top products
    const topProducts = [...productMap.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    
    setProductPopularity({
      labels: topProducts.map(([id]) => id),
      datasets: [{
        label: 'Reviews by Product',
        data: topProducts.map(([, count]) => count),
        backgroundColor: '#8b5cf6',
      }]
    });
    
    // Time series data
    const sortedTimeEntries = [...timeMap.entries()]
      .sort((a, b) => {
        const [monthA, yearA] = a[0].split('/');
        const [monthB, yearB] = b[0].split('/');
        return new Date(yearA, monthA - 1) - new Date(yearB, monthB - 1);
      });
    
    setTimeSeriesData({
      labels: sortedTimeEntries.map(([date]) => date),
      datasets: [{
        label: 'Reviews Over Time',
        data: sortedTimeEntries.map(([, count]) => count),
        borderColor: '#f59e0b',
        backgroundColor: 'rgba(245, 158, 11, 0.1)',
        tension: 0.3,
        fill: true,
      }]
    });
  };

  // Chart options
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: theme === 'dark' ? '#e5e7eb' : '#374151',
        },
      },
      title: {
        display: true,
        color: theme === 'dark' ? '#e5e7eb' : '#374151',
      },
    },
    scales: {
      x: {
        ticks: {
          color: theme === 'dark' ? '#e5e7eb' : '#374151',
        },
        grid: {
          color: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
        },
      },
      y: {
        ticks: {
          color: theme === 'dark' ? '#e5e7eb' : '#374151',
        },
        grid: {
          color: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
        },
      },
    },
  };

  const pieOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'right',
        labels: {
          color: theme === 'dark' ? '#e5e7eb' : '#374151',
        },
      },
      title: {
        display: true,
        text: 'Sentiment Distribution',
        color: theme === 'dark' ? '#e5e7eb' : '#374151',
      },
    },
  };

  if (loading) {
    return (
      <div className="container mx-auto p-4">
        <div className="flex justify-center items-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto p-4">
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative" role="alert">
          <strong className="font-bold">Error!</strong>
          <span className="block sm:inline"> {error}</span>
        </div>
      </div>
    );
  }
  return (
    <div className={`container mx-auto p-4 ${theme === 'dark' ? 'bg-gray-800 text-white' : 'bg-white text-gray-800'}`}>
      <h1 className="text-2xl md:text-3xl font-bold mb-6 text-center">Review Analytics Dashboard</h1>
      
      {/* Summary Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-2">Total Reviews</h2>
          <p className="text-2xl md:text-3xl font-bold">{summaryStats.totalReviews}</p>
        </div>
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-2">Positive Reviews</h2>
          <p className="text-2xl md:text-3xl font-bold text-green-500">{summaryStats.positivePercentage.toFixed(1)}%</p>
        </div>
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-2">Neutral Reviews</h2>
          <p className="text-2xl md:text-3xl font-bold text-blue-500">{summaryStats.neutralPercentage.toFixed(1)}%</p>
        </div>
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-2">Negative Reviews</h2>
          <p className="text-2xl md:text-3xl font-bold text-red-500">{summaryStats.negativePercentage.toFixed(1)}%</p>
        </div>
      </div>
        {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-4">Sentiment Distribution</h2>
          <div className="h-64 md:h-80">
            <Pie data={sentimentDistribution} options={pieOptions} />
          </div>
        </div>
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-4">Reviews Over Time</h2>
          <div className="h-64 md:h-80">
            <Line 
              data={timeSeriesData} 
              options={{
                ...chartOptions,
                plugins: {
                  ...chartOptions.plugins,
                  title: {
                    ...chartOptions.plugins.title,
                    text: 'Reviews Over Time',
                  },
                },
              }} 
            />
          </div>
        </div>
      </div>
      
      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-4">Top Reviewers</h2>
          <div className="h-64 md:h-80">
            <Bar 
              data={reviewerActivity} 
              options={{
                ...chartOptions,
                plugins: {
                  ...chartOptions.plugins,
                  title: {
                    ...chartOptions.plugins.title,
                    text: 'Top Reviewers by Number of Reviews',
                  },
                },
                indexAxis: 'y',
              }} 
            />
          </div>
        </div>
        <div className={`p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
          <h2 className="text-lg md:text-xl font-semibold mb-4">Top Products</h2>
          <div className="h-64 md:h-80">
            <Bar 
              data={productPopularity} 
              options={{
                ...chartOptions,
                plugins: {
                  ...chartOptions.plugins,
                  title: {
                    ...chartOptions.plugins.title,
                    text: 'Top Products by Number of Reviews',
                  },
                },
                indexAxis: 'y',
              }} 
            />
          </div>
        </div>
      </div>
      
      {/* Recent Reviews Table */}
      <div className={`mt-8 p-4 rounded-lg shadow-md ${theme === 'dark' ? 'bg-gray-700' : 'bg-gray-100'}`}>
        <h2 className="text-xl font-semibold mb-4">Recent Reviews</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className={theme === 'dark' ? 'bg-gray-600' : 'bg-gray-50'}>
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Reviewer</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Product ASIN</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Sentiment</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">Review Text</th>
              </tr>
            </thead>
            <tbody className={theme === 'dark' ? 'bg-gray-800 divide-y divide-gray-700' : 'bg-white divide-y divide-gray-200'}>
              {reviewsData.slice(0, 100).map((review, index) => (
                <tr key={index}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">{review.reviewerName || review.reviewerID}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">{review.asin}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                      review.prediction === 'Positive' ? 'bg-green-100 text-green-800' :
                      review.prediction === 'Negative' ? 'bg-red-100 text-red-800' :
                      'bg-gray-100 text-gray-800'
                    }`}>
                      {review.prediction}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-sm">
                    <div className="line-clamp-2">{review.reviewText}</div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default StaticAnalytics;
