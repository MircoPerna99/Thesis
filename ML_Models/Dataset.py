import sys
import os

sys.path.append(os.path.abspath("../"))
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Repository.RepositoryFile import RepositoryFile
import pandas as pd
from Services.Graphs.Graph import Graph
from pyspark.sql.functions import first
from pyspark.sql import Row
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import expr
from pyspark.sql.functions import abs
from pyspark.sql import functions as F
from pyspark.sql.functions import first,asc, desc
from Services.configuration import Configuration

class Dataset():
    def __init__(self, sparkSession):
        self.config = Configuration()
        self.allProtein = self.config["takeAllProtein"]
        self.proteinsForTest = self.config["proteinsToAnalyse"]
        self.spark =  sparkSession
        
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
    
    def getPPIForAnalyses(self):
        PPIs = []
        repositoryMongo = RepositoryMongo()
        if(self.allProtein):
            PPIs =  repositoryMongo.readPPIs()
        else:       
            for protein in self.proteinsForTest:
                query = '{ "proteinAId": "'+protein+'"}'
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
        
    def getPPIForAnalysesTemp(self):
        PPIs = []
        repositoryFile = RepositoryFile("NomeNonEsistente")
        PPIs =  repositoryFile.readPPIs()
        self.PPIs = PPIs    
    
    def getDTIForAnlysesTemp(self):
        DTIs = []
        repositoryFile = RepositoryFile("NomeNonEsistente")
        DTIs =  repositoryFile.readDTIs()          
        self.DTIs = DTIs   
    
    def getDTIForAnlyses(self):
        DTIs = []
        repositoryMongo = RepositoryMongo()

        for ppi in self.PPIs: 
            query = '{"$or": [ { "proteinId": "'+ppi._proteinAId+'"},{ "proteinId": "'+ppi._proteinBId+'"}]}'
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
        dfDTI_sp = self.spark.createDataFrame(rows)
        dfDTI_sp = dfDTI_sp.select("drugId", "proteinId").distinct()
        return  dfDTI_sp
    
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

        dfPPI_sp = self.spark.createDataFrame(rows)
        dfPPI_sp = dfPPI_sp.groupBy("proteinAId", "proteinBId") \
                    .agg(F.max("score").alias("score"))
        return  dfPPI_sp
    
    def getDTAmountInteractions(self):            
        dfDTI_sp = self._DTIToDataFrame()
        dfPPI_sp = self._PPIToDataFrame()
        dfDTI_sp.show()
        
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
    
    def getDTInteractionsTable(self):
        df_DTI = self._DTIToDataFrame()
        df_DTI = df_DTI.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.lit(1)).fillna(0)
        return df_DTI
    
    def getPPInteractionsTable(self, weight = False, noFilter = False):
        
        df_PPI = self._PPIToDataFrame(noFilter)
        
        if(weight):
            df_PPI = df_PPI.orderBy(F.col('proteinBId').asc(), F.col('score').desc()).groupBy("proteinBId").pivot("proteinAId").agg(first("score")).fillna(0)
        else:
            df_PPI = df_PPI.orderBy('proteinBId').groupBy("proteinBId").pivot("proteinAId").agg(F.lit(1)).fillna(0)

        df_PPI = df_PPI.withColumnRenamed("proteinBId", "proteinId")
        return df_PPI