from preprocessing.sparkClass import SparkClass
from preprocessing.prepare import DataPreprocessing
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml import Pipeline
from models.model import ModelNaiveBayes
from pyspark.ml.classification import NaiveBayes 

classSpark = SparkClass()
session = classSpark.createSession()
print(session)
print("Session démarrée")
"""
Chargement des données
"""
fileCSV = "Algerian_forest_fires_dataset_UPDATE.csv"
fileJSON = "people.json"
dataPrep = DataPreprocessing()
df_l = dataPrep.createDataFrame(session,fileCSV)
"""
    Transformation des données catégorielles en données numériques
"""
indexers = dataPrep.indexerColumns(df_l.columns)
pipeline = Pipeline(stages=indexers) 
indexed_df = pipeline.fit(df_l).transform(df_l)
indexed_df.show(8)

cols = ["day_indexed","month_indexed","year_indexed","Temperature_indexed","RH_indexed","Ws_indexed","Rain_indexed","FFMC_indexed","DMC_indexed","DC_indexed","ISI_indexed","BUI_indexed","FWI_indexed"]
vec = VectorAssembler(inputCols=cols,outputCol="features")
df_ass = dataPrep.groupeColumns(indexed_df,vec)
df_ass.select("features","label").show()
df_ass = df_ass.na.drop(how="all")
"""
 Entrainement du modéle
"""

df_ass.select("features","label").show(18)
train, test= df_ass.select("features","label").randomSplit([0.8, 0.2])

nb = NaiveBayes(modelType="multinomial")
nb_model = ModelNaiveBayes()
transformer = nb_model.train(train,nb)

prediction_df = nb_model.predict(test,transformer)

prediction_df.show(8)

"""
    evaluator = MulticlassClassificationEvaluator(labelCol="Classes_indexed", predictionCol="prediction", metricName="accuracy")
    nb_model.evaluateModel(prediction_df,evaluator)  
"""