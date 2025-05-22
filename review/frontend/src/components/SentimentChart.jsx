import React from 'react';
import { Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
} from 'chart.js';
import config from '../config';
import { useTheme } from '../contexts/ThemeContext';

ChartJS.register(ArcElement, Tooltip, Legend, CategoryScale, LinearScale);

const SentimentChart = ({ data }) => {
  const { theme } = useTheme();
  const isDark = theme === 'dark';
  
  if (!data) {
    return (
      <div className="dashboard-card p-6">
        <h2 className="text-xl font-semibold mb-4 text-primary-darkBlue dark:text-white">Sentiment Analysis</h2>
        <div className="w-full h-64 flex items-center justify-center bg-gray-50 dark:bg-background-darkNavy rounded-lg">
          <p className="text-gray-500 dark:text-gray-400">No sentiment data available</p>
        </div>
      </div>
    );
  }

  const total = (data.positive || 0) + (data.neutral || 0) + (data.negative || 0);
  
  const chartData = {
    labels: ['Positive', 'Neutral', 'Negative'],
    datasets: [
      {
        data: [data.positive || 0, data.neutral || 0, data.negative || 0],
        backgroundColor: [
          config.ui.sentimentColors.positive,
          config.ui.sentimentColors.neutral,
          config.ui.sentimentColors.negative,
        ],
        borderColor: [
          config.ui.colors.secondary.teal,
          config.ui.colors.primary.lightBlue,
          config.ui.colors.secondary.red,
        ],
        borderWidth: 2,
      },
    ],
  };

  const options = {
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'bottom',
        labels: {
          font: {
            size: 12,
            family: "'Inter', sans-serif",
          },
          usePointStyle: true,
          padding: 20,
          color: isDark ? '#F8F9FA' : '#1E2761',
        }
      },
      tooltip: {
        callbacks: {
          label: (context) => {
            const value = context.raw;
            const percentage = ((value / total) * 100).toFixed(1);
            return `${context.label}: ${value} (${percentage}%)`;
          }
        },
        titleFont: {
          size: 14,
          family: "'Inter', sans-serif",
        },
        bodyFont: {
          size: 14,
          family: "'Inter', sans-serif",
        },
        backgroundColor: isDark ? 'rgba(30, 39, 97, 0.9)' : 'rgba(10, 25, 41, 0.8)',
        padding: 12,
        boxPadding: 6,
        borderWidth: 0,
        cornerRadius: 8,
      }
    }
  };

  // Calculate percentages for summary
  const positivePercentage = total > 0 ? Math.round((data.positive / total) * 100) : 0;
  const neutralPercentage = total > 0 ? Math.round((data.neutral / total) * 100) : 0;
  const negativePercentage = total > 0 ? Math.round((data.negative / total) * 100) : 0;

  return (
    <div className="dashboard-card p-6">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-xl font-semibold text-primary-darkBlue dark:text-white">Sentiment Analysis</h2>
        <div className="text-sm text-gray-500 dark:text-gray-400 font-medium">
          Total Reviews: {total}
        </div>
      </div>
      
      <div className="w-full h-64">
        <Pie data={chartData} options={options} />
      </div>
      
      <div className="grid grid-cols-3 gap-3 mt-6">
        <div className="text-center p-3 rounded-lg" style={{backgroundColor: isDark ? 'rgba(64, 224, 208, 0.2)' : 'rgba(64, 224, 208, 0.15)'}}>
          <p className="text-sm text-gray-600 dark:text-gray-300">Positive</p>
          <p className="text-2xl font-bold text-secondary-teal">{positivePercentage}%</p>
        </div>
        <div className="text-center p-3 rounded-lg" style={{backgroundColor: isDark ? 'rgba(126, 178, 255, 0.2)' : 'rgba(126, 178, 255, 0.15)'}}>
          <p className="text-sm text-gray-600 dark:text-gray-300">Neutral</p>
          <p className="text-2xl font-bold text-primary-lightBlue">{neutralPercentage}%</p>
        </div>
        <div className="text-center p-3 rounded-lg" style={{backgroundColor: isDark ? 'rgba(255, 87, 87, 0.2)' : 'rgba(255, 87, 87, 0.15)'}}>
          <p className="text-sm text-gray-600 dark:text-gray-300">Negative</p>
          <p className="text-2xl font-bold text-secondary-red">{negativePercentage}%</p>
        </div>
      </div>
    </div>
  );
};

export default SentimentChart;
