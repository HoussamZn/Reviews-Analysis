import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import warnings
warnings.filterwarnings('ignore')

# Import des bibliothèques PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, rand
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Téléchargement des ressources NLTK nécessaires
def download_nltk_resources():
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('punkt_tab')  # Conservé comme demandé

def initialize_spark():
    spark = SparkSession.builder \
        .appName("AmazonReviewsSentimentAnalysis") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    print("Spark version:", spark.version)
    return spark

def load_data(file_path):
    print("Chargement des données...")
    with open(file_path, 'r') as f:
        data = [json.loads(line) for line in f]
    df = pd.DataFrame(data)
    print("Aperçu des données:")
    print(df.head())
    return df

def preprocess_dataframe(df):
    print("\nPrétraitement des données...")
    columns_to_keep = ['reviewText', 'summary', 'overall']
    df_processed = df[columns_to_keep].copy()
    
    df_processed['reviews'] = df_processed['reviewText'] + ' ' + df_processed['summary']
    df_processed = df_processed.drop(['reviewText', 'summary'], axis=1)

    df_processed['sentiment'] = df_processed['overall'].apply(lambda x: 'Negative' if x < 3 else ('Neutral' if x == 3 else 'Positive'))
    df_processed = df_processed.drop('overall', axis=1)

    return df_processed

def clean_text(text):
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def preprocess_text(text):
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    tokens = nltk.word_tokenize(text)
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word.isalpha() and word not in stop_words]
    return ' '.join(tokens)

def apply_text_preprocessing(df):
    print("\nApplication du prétraitement de texte...")
    df['cleaned_reviews'] = df['reviews'].apply(clean_text)
    df['processed_reviews'] = df['cleaned_reviews'].apply(preprocess_text)
    return df

def visualize_data(df):
    print("\nVisualisation des données...")

    print("\nDistribution des sentiments:")
    sentiment_count = df['sentiment'].value_counts()
    print(sentiment_count)

    # Visualisation de la distribution des sentiments
    plt.figure(figsize=(10, 6))
    sns.countplot(x='sentiment', data=df, palette='viridis')
    plt.title('Distribution des Sentiments')
    plt.ylabel('Nombre d\'avis')
    plt.savefig('/review/sentiment_distribution.png')
    plt.close()

def convert_to_spark_df(spark, df):
    print("\nConversion des données en DataFrame Spark...")
    spark_df = spark.createDataFrame(df)
    print("\nAperçu du DataFrame Spark:")
    spark_df.show(5, truncate=False)
    return spark_df

def split_data(spark_df, train_ratio=0.8, val_ratio=0.1, test_ratio=0.1):
    print("\nDivision des données...")

    train_spark_df, temp_df = spark_df.randomSplit([train_ratio, val_ratio + test_ratio], seed=42)
    val_spark_df, test_spark_df = temp_df.randomSplit([val_ratio / (val_ratio + test_ratio),test_ratio / (val_ratio + test_ratio)], seed=42)

    test_spark_df.write.json("/review/test_data.json")
    return train_spark_df, val_spark_df, test_spark_df

def balance_classes(train_spark_df):
    print("\nÉquilibrage des classes par sur-échantillonnage...")

    # Compter les occurrences de chaque classe
    class_counts = train_spark_df.groupBy("sentiment").count().collect()
    class_counts_dict = {row["sentiment"]: row["count"] for row in class_counts}

    # Identifier la classe majoritaire
    majority_class = max(class_counts_dict.items(), key=lambda x: x[1])[0]
    majority_count = class_counts_dict[majority_class]

    print(f"Classe majoritaire: {majority_class} avec {majority_count} observations")

    # Sur-échantilloner les classes minoritaires
    balanced_dfs = []
    for sentiment in class_counts_dict.keys():
        class_df = train_spark_df.filter(col("sentiment") == sentiment)

        if sentiment != majority_class:
            # Calculer le ratio pour atteindre l'équilibre avec la classe majoritaire
            ratio = float(majority_count) / float(class_counts_dict[sentiment])
            # Arrondir au nombre entier supérieur
            num_repeats = int(np.ceil(ratio))

            # Sur-échantillonnage par réplication
            oversampled_df = class_df
            for _ in range(num_repeats - 1):
                oversampled_df = oversampled_df.union(class_df)

            # Limiter au nombre exact dont nous avons besoin
            oversampled_df = oversampled_df.limit(majority_count)
            balanced_dfs.append(oversampled_df)

        else:
            balanced_dfs.append(class_df)

    balanced_train_df = balanced_dfs[0]
    for df in balanced_dfs[1:]:
        balanced_train_df = balanced_train_df.union(df)

    balanced_train_df = balanced_train_df.orderBy(rand())

    print(f"Ensemble d'entraînement après équilibrage: {balanced_train_df.count()} observations")

    # Vérifier la distribution après équilibrage
    print("\nDistribution des classes après équilibrage:")
    balanced_class_counts = balanced_train_df.groupBy("sentiment").count().collect()
    for row in balanced_class_counts:
        print(f"{row['sentiment']}: {row['count']}")

    return balanced_train_df

def create_feature_pipeline():
    tokenizer = Tokenizer(inputCol="processed_reviews", outputCol="words")
    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    count_vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", maxDF=0.8, minDF=3.0)
    idf = IDF(inputCol="raw_features", outputCol="all_features")

    # Encodage de la cible
    string_indexer = StringIndexer(inputCol="sentiment", outputCol="label")

    return [tokenizer, stopwords_remover, count_vectorizer, idf, string_indexer]

def create_model_pipelines(feature_pipeline):
    # Définition des modèles
    lr = LogisticRegression(maxIter=100, regParam=0.1, elasticNetParam=0.0, featuresCol="all_features")
    dt = DecisionTreeClassifier(maxDepth=10, featuresCol="all_features")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, featuresCol="all_features")

    # Création des pipelines
    lr_pipeline = Pipeline(stages=feature_pipeline + [lr])
    dt_pipeline = Pipeline(stages=feature_pipeline + [dt])
    rf_pipeline = Pipeline(stages=feature_pipeline + [rf])

    return {
        #"Logistic Regression": lr_pipeline,
        "Decision Tree": dt_pipeline,
        #"Random Forest": rf_pipeline
    }

def train_and_evaluate_model(pipeline, name, train_data, val_data, test_data):
    print(f"\nEntraînement du modèle {name}...")

    # Définition de l'évaluateur
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    # Entraînement du modèle
    model = pipeline.fit(train_data)

    # Évaluation sur l'ensemble de validation
    print(f"Évaluation du modèle {name} sur l'ensemble de validation...")
    val_predictions = model.transform(val_data)
    val_accuracy = evaluator.evaluate(val_predictions)

    print(f"{name} - Accuracy sur validation: {val_accuracy:.4f}")

    # Calcul de la matrice de confusion sur l'ensemble de validation
    predictions_pandas = val_predictions.select("label", "prediction").toPandas()
    conf_matrix = pd.crosstab(
        predictions_pandas['label'],
        predictions_pandas['prediction'],
        rownames=['Actual'],
        colnames=['Predicted']
    )

    # Affichage de la matrice de confusion
    plt.figure(figsize=(8, 6))
    sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues')
    plt.title(f'Matrice de confusion - {name}')
    plt.savefig(f'/review/confusion_matrix_{name}.png')
    plt.close()

    return val_accuracy, model

def train_all_models(model_pipelines, balanced_train_df, val_spark_df, test_spark_df):
    print("\n====== Entraînement et évaluation des modèles PySpark ======")

    model_results = {}

    # Entraînement et évaluation des modèles
    for name, pipeline in model_pipelines.items():
        model_results[name] = train_and_evaluate_model(pipeline, name, balanced_train_df, val_spark_df, test_spark_df)

    # Sélection du meilleur modèle
    best_model_name = max(model_results.items(), key=lambda x: x[1][0])[0]
    best_accuracy, best_model = model_results[best_model_name]

    print('*'*50)
    print(f"\nMeilleur modèle PySpark: {best_model_name} avec une précision de {best_accuracy:.4f}")

    return best_model_name, best_model

def save_model(model, path):
    print("\n====== Sauvegarde du meilleur modèle ======")
    model.save(path)
    print(f"Meilleur modèle sauvegardé avec succès à {path}!")

def create_prediction_function(spark, model):
    from pyspark.sql.functions import col

    # Définition des UDFs pour les fonctions de nettoyage
    clean_text_udf = udf(clean_text, StringType())
    preprocess_text_udf = udf(preprocess_text, StringType())

    def predict_sentiment(text):
        # Création d'un DataFrame avec une seule ligne
        data = [(text,)]
        schema = ["text"]
        test_df = spark.createDataFrame(data, schema)

        # Application des mêmes transformations que dans le pipeline
        test_df = test_df.withColumn("reviews", col("text"))
        test_df = test_df.withColumn("cleaned_reviews", clean_text_udf("reviews"))
        test_df = test_df.withColumn("processed_reviews", preprocess_text_udf("cleaned_reviews"))

        # Prédiction
        prediction = model.transform(test_df)
        result = prediction.select("prediction").collect()[0][0]

        # Conversion de la prédiction numérique en étiquette
        sentiment_labels = {0: "Negative", 1: "Neutral", 2: "Positive"}
        return sentiment_labels[result]

    return predict_sentiment

def test_examples(predict_sentiment):
    """Test avec des exemples de différentes classes"""
    print("\n====== Test avec des exemples de différentes classes ======")

    # Exemples positifs
    positive_examples = [
        "This product is absolutely amazing! I love it so much.",
        "Best purchase I've made this year, highly recommended!",
        "The quality exceeded my expectations, worth every penny."
    ]

    # Exemples neutres
    neutral_examples = [
        "The product is okay, nothing special but gets the job done.",
        "It's fine, I expected more for the price but it's acceptable.",
        "Not bad, not great - just average."
    ]

    # Exemples négatifs
    negative_examples = [
        "Terrible product, broke after just one use!",
        "Waste of money, completely disappointed with this purchase.",
        "The worst quality I've ever seen, do not recommend."
    ]

    # Test des exemples
    print("\nPrédictions pour les exemples positifs:")
    for example in positive_examples:
        print(f"'{example[:50]}...' → {predict_sentiment(example)}")

    print("\nPrédictions pour les exemples neutres:")
    for example in neutral_examples:
        print(f"'{example[:50]}...' → {predict_sentiment(example)}")

    print("\nPrédictions pour les exemples négatifs:")
    for example in negative_examples:
        print(f"'{example[:50]}...' → {predict_sentiment(example)}")

def main():
    download_nltk_resources()
    spark = initialize_spark()
    try:
        df = load_data('/review/Data.json')
        df_processed = preprocess_dataframe(df)
        df_processed = apply_text_preprocessing(df_processed)
        visualize_data(df_processed)
        spark_df = convert_to_spark_df(spark, df_processed)
        train_spark_df, val_spark_df, test_spark_df = split_data(spark_df)
        balanced_train_df = balance_classes(train_spark_df)
        feature_pipeline = create_feature_pipeline()
        model_pipelines = create_model_pipelines(feature_pipeline)
        best_model_name, best_model = train_all_models(model_pipelines, balanced_train_df, val_spark_df, test_spark_df)
        print(f"\n====== {best_model_name} ======")
        save_model(best_model,"/review/models")
        predict_sentiment = create_prediction_function(spark, best_model)
        test_examples(predict_sentiment)

    finally:
        spark.stop()
        print("\n====== Fin du script ======")

if __name__ == "__main__":
    main()