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
from ALS.ALSModel import ALSModel
from pyspark.sql.functions import first,asc, desc


class ALSDataset():
    def __init__(self):
        pass
    
    def _takeDTI(self, query = None):
        repositoryMongo = RepositoryMongo()
        DTIs = repositoryMongo.readDTIs(query)
        repositoryMongo.close_connection()
        return DTIs
    
    def _takePPI(self):
        repositoryMongo = RepositoryMongo()
        PPIs = repositoryMongo.readPPIs()
        repositoryMongo.close_connection()
        return PPIs
    
    def dataframeForTestALS(self): 
        PPIs = []
        repositoryMongo = RepositoryMongo()


        proteins = ["O54824", "Q00403", "Q12933", "Q09472", "Q92793", "Q9WU01"]
        PPIs = []
        PPIs =  repositoryMongo.readPPIs()
        # for protein in proteins:
        #     query = '{ "proteinBId": "'+protein+'"}'
        #     PPIsToAdd = repositoryMongo.readPPIs(query)
        #     if(PPIsToAdd != None and len(PPIsToAdd)>0):
        #         for ppi in PPIsToAdd:
        #             query = '{ "proteinBId": "'+ppi._proteinAId+'"}'
        #             PPIsToAdd_Link = repositoryMongo.readPPIs(query)
        #             if(PPIsToAdd_Link != None and len(PPIsToAdd_Link)>0):
        #                 PPIs.extend(PPIsToAdd_Link)
            
        #         PPIs.extend(PPIsToAdd)
            

        DTIs = []
        for ppi in PPIs:
            query = '{ "proteinId": "'+ppi._proteinAId+'"}'
            DTIsToAdd = repositoryMongo.readDTIs(query)
            if(DTIsToAdd != None or len(DTIsToAdd) != 0):
                DTIs.extend(DTIsToAdd)
            
            query = '{ "proteinId": "'+ppi._proteinBId+'"}'
            DTIsToAdd = repositoryMongo.readDTIs(query)
            if(DTIsToAdd != None or len(DTIsToAdd) != 0):
                DTIs.extend(DTIsToAdd)
                
            
        repositoryMongo.close_connection()
            

        # Create a SparkSession
        spark = SparkSession.builder \
                        .appName("Collaborative_Filtering")\
                        .config("spark.driver.host", "localhost") \
                        .config("spark.driver.bindAddress", "127.0.0.1") \
                        .getOrCreate()
        # objects to a list of Row objects

        rows = [Row(drugId=d._drugId,
                    proteinId=d._proteinId) for d in DTIs]

        dfDTI_sp = spark.createDataFrame(rows)
        dfDTI_sp.show()

        rows = [Row(proteinAId=p._proteinAId,
                    proteinBId=p._proteinBId,
                    score = p._score) for p in PPIs]

        dfPPI_sp = spark.createDataFrame(rows)
        dfPPI_sp.show()

        joinedDF = dfDTI_sp.join(dfPPI_sp, (dfDTI_sp.proteinId == dfPPI_sp.proteinAId) & (dfPPI_sp.score >= 0.5))
        joinedDF.show()

        DTIsGrouped = dfDTI_sp.groupBy("drugId").agg(collect_list("proteinId").alias("proteins")).orderBy("drugId")
        DTIsGrouped = DTIsGrouped.withColumnRenamed("drugId", "drugId_group")
        DTIsGrouped.show()
                

        joinedDF = joinedDF.join(DTIsGrouped, joinedDF.drugId == DTIsGrouped.drugId_group)
        joinedDF = joinedDF.select(joinedDF["drugId"], joinedDF["proteinId"], joinedDF["proteinAId"], joinedDF["proteinBId"],joinedDF["Proteins"])
        joinedDF.show()

        joinedDF = joinedDF.withColumn("interactor_drug_target", expr("array_contains(Proteins, proteinBId)"))
        joinedDF.show()
        joinedDF = joinedDF.filter(joinedDF.interactor_drug_target == False)
        joinedDF.show()

        columns_to_group_by = ["drugId", "proteinBId"]
        joinedDF= joinedDF.groupBy(columns_to_group_by).count()
        joinedDF= joinedDF.withColumnRenamed("count", "amount_interactions")   
        joinedDF= joinedDF.withColumnRenamed("proteinBId", "proteinId")
        joinedDF.show()

        return joinedDF


df = ALSDataset().dataframeForTestALS()

model = ALSModel(df)
model.calculate_recommended_proteins()
result = (df.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("amount_interactions")))
result.write.mode("overwrite").option("header", True).csv("output_folder_no_model")

result = (model.drug_proteins_recommended.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("rating")))
result.write.mode("overwrite").option("header", True).csv("output_folder_model")

## Lines for graphs
# dfPPI = pd.DataFrame().from_records(ppi.toDict() for ppi in PPIs)
# dfDTI = pd.DataFrame().from_records(dti.toDict() for dti in DTIs)
# Graph().printBiologicalNetwork(dfDTI,dfPPI)

