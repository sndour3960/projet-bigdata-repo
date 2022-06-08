import pyspark
from pyspark.sql import SparkSession,DataFrame
from typing import Optional
import logging

class SparkClass:
    def __init__(self):
        logging.info("Initialisation de spark !")
    
    def createSession(self,master:Optional['str']="local[*]", app_name:Optional['str']="BigData Machine Learning") -> SparkSession:
        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
        print("Démarrage de la session")
        return spark
    
    def destroySession(self,spark:SparkSession):
        spark.stop() if isinstance(spark, SparkSession) else None

        
    