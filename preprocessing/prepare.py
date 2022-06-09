from typing import List
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import StringIndexer,VectorAssembler 
import pathlib, os, logging



class DataPreprocessing:
    
    def __init__(self):
        logging.info("Pré-traitement des données !")
    
    def createDataFrame(self,spark:SparkSession,filePath:str) -> DataFrame:
        if os.path.exists(filePath) and os.path.isfile(filePath) :
            extension = pathlib.Path(filePath).suffix
        else: raise Exception('Erreur, essayer encore')
        
        def fromCSV(filePath:str) -> DataFrame:
            df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                .csv(filePath)
            return df
        
        def fromJSON(filePath:str) -> DataFrame:
            df = spark.read.json(filePath)
            return df
        return fromCSV(filePath) if extension==".csv" else fromJSON(filePath) 
    
    def transformToNumeric(self,df:DataFrame, input:str, output:str) -> DataFrame:
        indexer = StringIndexer(inputCol=input, outputCol=output)
        df_transformed = indexer.fit(df).transform(df)
        df_transformed = df_transformed.drop(input)
        return df_transformed
    
    def indexerColumns(self,colums:List['str']) -> List[StringIndexer]:
        columnsIndexed = []
        for column in colums:
            col=""
            if column=="Classes":
                col="label"
            else:
                col = column+"_indexed"
            st = StringIndexer(inputCol=column,outputCol=col)
            st.setHandleInvalid("skip")
            columnsIndexed.append(st)
        return columnsIndexed
    
    def groupeColumns(self,df:DataFrame,vectorAssembler:VectorAssembler) -> DataFrame:
        assembled_df = vectorAssembler.transform(df)
        return assembled_df
    
    def renameCol(self,df:DataFrame,exist:str,newCol:str) -> DataFrame:
        n_df = df.withColumnRenamed(exist,newCol )
        return n_df