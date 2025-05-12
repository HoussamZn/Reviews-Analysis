import json
import pandas as pd
import numpy as np
import re
import string
import joblib
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, confusion_matrix
from imblearn.over_sampling import SMOTE
import matplotlib.pyplot as plt

# --- Custom cleaner
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
    

# --- Load and prepare data
def load_json(file_path):
    with open(file_path, 'r') as f:
        data = [json.loads(line) for line in f]
    return pd.DataFrame(data)


def label_sentiment(df):
    def get_sentiment(score):
        if score < 3:
            return 'Negative'
        elif score == 3:
            return 'Neutral'
        else:
            return 'Positive'
    df['sentiment'] = df['overall'].apply(get_sentiment)
    return df


df = load_json("/Data.json")[["asin","reviewText","overall"]]
df = label_sentiment(df)
X_train, X_test, y_train, y_test = train_test_split(
    df['reviewText'].values, df['sentiment'].values, test_size=0.2, random_state=42, stratify=df['sentiment']
)

preprocess_pipeline = Pipeline([
    ('cleaner', TextCleaner()),
    ('tfidf', TfidfVectorizer(stop_words='english')),
])

X_train_vec = preprocess_pipeline.fit_transform(X_train)
X_test_vec = preprocess_pipeline.transform(X_test)

smote = SMOTE(random_state=42)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train_vec, y_train)

clf = LogisticRegression(max_iter=1000)
clf.fit(X_train_resampled, y_train_resampled)

fitted_vectorizer = preprocess_pipeline.named_steps['tfidf']

final_pipeline = Pipeline([
    ('cleaner', TextCleaner()),
    ('tfidf', fitted_vectorizer),
    ('clf', clf)
])

joblib.dump(final_pipeline, "/tmp/pipeline.pkl")
print("Saved pipeline to pipeline.pkl")

pipeline = joblib.load("/tmp/pipeline.pkl")
x = np.array(["If you are not use to houssam a large sustaining pedal while playing the piano, it may appear little awkward."])
new_predictions = pipeline.predict(x)
print(new_predictions)