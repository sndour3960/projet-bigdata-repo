from pyspark.ml.feature import StringIndexer 
from pyspark.ml.classification import NaiveBayes 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.ml import Transformer
import logging
from typing import Optional

class ModelNaiveBayes:
    def __init__(self):
        logging.info("Apprentissage avec mon modéle")
        
        
    def train(self,df:DataFrame,nbModel:NaiveBayes) -> Transformer:
        model = nbModel.fit(df)
        return model
    
    def predict(self,df:DataFrame, tf:Transformer) -> DataFrame:
        predicted_df = tf.transform(df)
        return predicted_df
    
    def evaluateModel(self,df:DataFrame, evaluator:MulticlassClassificationEvaluator) -> float:
         accuracy = evaluator.evaluate(df)
         return accuracy
    