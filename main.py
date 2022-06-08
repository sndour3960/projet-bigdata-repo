from preprocessing.sparkClass import SparkClass
from preprocessing.prepare import DataPreprocessing

classSpark = SparkClass()
session = classSpark.createSession()
print(session)
print("Session démarrée")
fileCSV = "Algerian_forest_fires_dataset_UPDATE.csv"
fileJSON = "people.json"
dataPrep = DataPreprocessing()
df_l = dataPrep.createDataFrame(session,fileCSV)
df_indexed = dataPrep.transformToNumeric(df_l,'Classes','IsBurning')
df_indexed.show(6,truncate=False)
type(df_l)

