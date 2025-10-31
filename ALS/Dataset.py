import sys
import os

sys.path.append(os.path.abspath("../"))
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Model.DTI_Model import DTIModel
import pandas as pd
from Services.Graphs.Graph import Graph
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode
from pyspark.sql.functions import first
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import expr
from pyspark.sql.functions import abs
from ALS.ALSModel import ALSModel
from ALS.FMModel import FMModel
from pyspark.sql import functions as F
from pyspark.sql.functions import first,asc, desc
from configuration import Configuration
import random

class Dataset():
    def __init__(self):
        self.config = Configuration()
        self.allProtein = self.config["takeAllProtein"]
        self.proteinsForTest = self.config["proteinsToAnalyse"]
        self.spark = SparkSession.builder \
                        .appName("Collaborative_Filtering")\
                        .config("spark.driver.host", "localhost") \
                        .config("spark.driver.bindAddress", "127.0.0.1") \
                        .getOrCreate()
        
    def _takeDTI(self, query = None):
        repositoryMongo = RepositoryMongo()
        DTIs = repositoryMongo.readDTIs(query)
        repositoryMongo.close_connection()
        self.DTIs = DTIs
    
    def _takePPI(self):
        repositoryMongo = RepositoryMongo()
        PPIs = repositoryMongo.readPPIs()
        repositoryMongo.close_connection()
        self.PPIs = PPIs
    
    def takePPIForAnalyses(self):
        PPIs = []
        repositoryMongo = RepositoryMongo()
        if(self.allProtein):
            PPIs =  repositoryMongo.readPPIs()
        else:       
            for protein in self.proteinsForTest:
                query = '{ "proteinBId": "'+protein+'"}'
                PPIsToAdd = repositoryMongo.readPPIs(query)
                if(PPIsToAdd != None and len(PPIsToAdd)>0):
                    for ppi in PPIsToAdd:
                        query = '{ "proteinBId": "'+ppi._proteinAId+'"}'
                        PPIsToAdd_Link = repositoryMongo.readPPIs(query)
                        if(PPIsToAdd_Link != None and len(PPIsToAdd_Link)>0):
                            PPIs.extend(PPIsToAdd_Link)
                
                    PPIs.extend(PPIsToAdd)
                    
        repositoryMongo.close_connection()
        
        self.PPIs = PPIs
        
    def takeDrugsForAnlyses(self):
        DTIs = []
        repositoryMongo = RepositoryMongo()

        for ppi in self.PPIs:
            query = '{ "proteinId": "'+ppi._proteinAId+'"}'
            DTIsToAdd = repositoryMongo.readDTIs(query)
            if(DTIsToAdd != None or len(DTIsToAdd) != 0):
                DTIs.extend(DTIsToAdd)
            
            query = '{ "proteinId": "'+ppi._proteinBId+'"}'
            DTIsToAdd = repositoryMongo.readDTIs(query)
            if(DTIsToAdd != None or len(DTIsToAdd) != 0):
                DTIs.extend(DTIsToAdd)   
        
        repositoryMongo.close_connection()
        
        self.DTIs = DTIs
    
    def toGraph(self):
        dfPPI = pd.DataFrame().from_records(ppi.toDict() for ppi in self.PPIs)
        dfDTI = pd.DataFrame().from_records(dti.toDict() for dti in self.DTIs)
        Graph().printBiologicalNetwork(dfDTI,dfPPI)
        
    def _DTIToDataFrame(self):
        rows = [Row(drugId=d._drugId,
                    proteinId=d._proteinId) for d in self.DTIs]

        return self.spark.createDataFrame(rows)
    
    def _PPIToDataFrame(self, noFilter = True):
        
        PPIs = []
        if(noFilter):
            PPIs = self.PPIs
        else:
            for ppi in self.PPIs:
                if(ppi._score >= 0.5):
                    PPIs.append(ppi)
                
        rows = [Row(proteinAId=p._proteinAId,
                    proteinBId=p._proteinBId,
                    score = p._score) for p in PPIs]

        return  self.spark.createDataFrame(rows)
    
    def dataframeForTestALS(self):            
        dfDTI_sp = self._DTIToDataFrame()
        dfPPI_sp = self._PPIToDataFrame()

        joinedDF = dfDTI_sp.join(dfPPI_sp, (dfDTI_sp.proteinId == dfPPI_sp.proteinAId) & (dfPPI_sp.score >= 0.5))

        DTIsGrouped = dfDTI_sp.groupBy("drugId").agg(collect_list("proteinId").alias("proteins")).orderBy("drugId")
        DTIsGrouped = DTIsGrouped.withColumnRenamed("drugId", "drugId_group")
                
        joinedDF = joinedDF.join(DTIsGrouped, joinedDF.drugId == DTIsGrouped.drugId_group)
        joinedDF = joinedDF.select(joinedDF["drugId"], joinedDF["proteinId"], joinedDF["proteinAId"], joinedDF["proteinBId"],joinedDF["Proteins"])
        joinedDF = joinedDF.withColumn("interactor_drug_target", expr("array_contains(Proteins, proteinBId)"))
        joinedDF = joinedDF.filter(joinedDF.interactor_drug_target == False)

        columns_to_group_by = ["drugId", "proteinBId"]
        joinedDF= joinedDF.groupBy(columns_to_group_by).count()
        joinedDF= joinedDF.withColumnRenamed("count", "amount_interactions")   
        joinedDF= joinedDF.withColumnRenamed("proteinBId", "proteinId")

        return joinedDF


dataset = Dataset()
dataset.takePPIForAnalyses()
dataset.takeDrugsForAnlyses()
#dataset.toGraph()
df = dataset.dataframeForTestALS()
df.show()

df_DTI = dataset._DTIToDataFrame()
df_DTI = df_DTI.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.lit(1)).fillna(0)
df_PPI = dataset._PPIToDataFrame(False)
df_PPI_Weight = df_PPI.orderBy(F.col('proteinBId').asc(), F.col('score').desc()).groupBy("proteinBId").pivot("proteinAId").agg(first("score")).fillna(0)

df_PPI = df_PPI.orderBy('proteinBId').groupBy("proteinBId").pivot("proteinAId").agg(F.lit(1)).fillna(0)

df_PPI_Weight = df_PPI_Weight.withColumnRenamed("proteinBId", "proteinId")
df_PPI = df_PPI.withColumnRenamed("proteinBId", "proteinId")
# result = (df.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("amount_interactions")).fillna(0))

# result.write.mode("overwrite").option("header", True).csv("interactions_DP")

# modelAls = ALSModel(df)
# modelFM = FMModel(df, False)

#modelFM_Weight = FMModel(df,df_DTI, df_PPI, True)
# modelFM_Weight.predictions.show()
# for i in range(5):
#     modelFM_Weight = FMModel(df,df_DTI, df_PPI, True)
#     results.append(modelFM_Weight.data.columns)

# for i in range(len(results)):
#     for j in range(i, len(results)):
#         flag = all(x == y for x, y in zip(results[i], results[j]))
#         if(not flag):
#             print(flag)


seeds = random.sample(range(1, 101), 10)

# modelFM_Weight = FMModel(df,df_DTI, df_PPI_Weight, True)
modelFM_No_Weight = FMModel(df,df_DTI, df_PPI, True)
modelFM_No_Weight.train()
# modelFM_No_Weight.crossValidation()
# modelAls = ALSModel(df)
# modelAls.crossValidation()
# resultsWeight = []
# resultsNoWeight = []
# resultAls = []
# for seed in seeds:
#     modelAls.train(seed)
#     resultAls.append("Chosen parameters: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(modelAls.aus_regParam, modelAls.aus_rank, modelAls.aus_alpha, modelAls.aus_rmse))  
    
#     # modelFM_Weight.train(seed)
#     # resultsWeight.append("Chosen parameters for FM WEIGHT: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM_Weight.aus_regParam, modelFM_Weight.aus_maxIter, modelFM_Weight.aus_initStd,modelFM_Weight.aus_factorSize, modelFM_Weight.aus_rmse))

#     modelFM_No_Weight.train(seed)
#     resultsNoWeight.append("Chosen parameters for FM NO WEIGHT: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM_No_Weight.aus_regParam, modelFM_No_Weight.aus_maxIter, modelFM_No_Weight.aus_initStd,modelFM_No_Weight.aus_factorSize, modelFM_No_Weight.aus_rmse))

# print("results ALS")
# for result in resultAls:
#     print(result)

# print("results weight")
# for result in resultsWeight:
#     print(result)
    
# print("results no weight")
# for result in resultsNoWeight:
#     print(result)
    

    
# modelAls.calculate_recommended_proteins()

# testDf = modelAls.drug_proteins_recommended.join(modelFM.predictions, (modelFM.predictions.proteinId_int == modelAls.drug_proteins_recommended.proteinId ) & (modelFM.predictions.drugId_int == modelAls.drug_proteins_recommended.drugId ))
# testDf = testDf.withColumn("error", abs(testDf.prediction - testDf.amount_interactions))
# testDf = testDf.withColumn("differences_rating_prediction",  abs(testDf.rating - testDf.prediction))
# result = (testDf.select("drugId","proteinId","rating", "amount_interactions", "prediction","error", "differences_rating_prediction").orderBy("differences_rating_prediction", ascending=False))
# # testDf.select("drugId","proteinId","rating", "amount_interactions", "prediction","error", "differences_rating_prediction").orderBy("differences_rating_prediction", ascending=False).show()
# result.write.mode("overwrite").option("header", True).csv("analyses_model_interactions_DP")

# testDf = modelAls.drug_proteins_recommended.join(modelFM_Weight.predictions, (modelFM_Weight.predictions.proteinId_int == modelAls.drug_proteins_recommended.proteinId ) & (modelFM_Weight.predictions.drugId_int == modelAls.drug_proteins_recommended.drugId ))
# testDf = testDf.withColumn("error", abs(testDf.prediction - testDf.amount_interactions))
# testDf = testDf.withColumn("differences_rating_prediction",  abs(testDf.rating - testDf.prediction))
# result = (testDf.select("drugId","proteinId","rating", "amount_interactions", "prediction","error", "differences_rating_prediction").orderBy("differences_rating_prediction", ascending=False))
# result.write.mode("overwrite").option("header", True).csv("analyses_model_interactions_DP_weight")
# print("Chosen parameters for ALS: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(modelAls.aus_regParam, modelAls.aus_rank, modelAls.aus_alpha, modelAls.aus_rmse))          
# print("Chosen parameters for FM: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM.aus_regParam, modelFM.aus_maxIter, modelFM.aus_initStd,modelFM.aus_factorSize, modelFM.aus_rmse))          
# print("Chosen parameters for FM WEIGHT: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM_Weight.aus_regParam, modelFM_Weight.aus_maxIter, modelFM_Weight.aus_initStd,modelFM_Weight.aus_factorSize, modelFM_Weight.aus_rmse))          
