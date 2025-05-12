# --- Custom cleaner
from sklearn.base import BaseEstimator, TransformerMixin
import string
import numpy as np
import re

class TextCleaner(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        if isinstance(X, np.ndarray):
            X = X.flatten()
        return np.array([self.clean_text(text) for text in X])
    
    def clean_text(self, text):
        text = text.lower()
        text = re.sub(f"[{re.escape(string.punctuation)}]", "", text)
        text = re.sub(r'\d+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text