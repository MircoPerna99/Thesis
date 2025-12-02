import sys
import os

sys.path.append(os.path.abspath("../"))
from pyspark.sql import functions as F
from ML_Models.Dataset import Dataset
from ML_Models.ALSModel import ALSModel
from ML_Models.FMModel import FMModel
from pyspark.sql import SparkSession
from Services.configuration import Configuration
from pyspark.sql.functions import col
import random
import json 
import numpy as np
from Services.Graphs.Histogram import Histogram


class Analyzer():
    
    def __init__(self):   
        self._initProperties()
        self.ALSModel : ALSModel = None
        self.FMModel : FMModel = None
    
    def _saveDataframeOnCSV(self,df, nameFile):
        print("Save {0} on file".format(nameFile))
        df.write.mode("overwrite").option("header", True).csv("ML_Models/"+nameFile)
        print("Saving completed")
    
    def _saveResultsOnFile(self,list, nameFile):
        print("Save {0} on file".format(nameFile))
        with open("ML_Models/"+nameFile, 'w') as f:
            for item in list:
                f.write(f"{item}\n")
        print("Saving completed")
    
    def _initProperties(self):
        print("Initialization of config")
        self.config = Configuration()
        print("Initialization of config completed")
        print("Open spark session")
        self.sparkSession = SparkSession.builder \
                                .appName("Collaborative_Filtering")\
                                .config("spark.driver.host", "localhost") \
                                .config("spark.ui.showConsoleProgress", "false") \
                                .config("spark.driver.bindAddress", "127.0.0.1") \
                                .config("spark.driver.memory", "16g") \
                                .config("spark.sql.pivotMaxValues", "1000000")\
                                .config("spark.driver.extraJavaOptions", "-Xss4m")\
                                .getOrCreate()
                                
        self.sparkSession.sparkContext.setLogLevel("ERROR")
        self.sparkSession.conf.set("spark.sql.debug.maxToStringFields", 10000)
        print("Starting initialization dataset")
        self.dataset = Dataset(self.sparkSession)
        print("Finished initialization dataset")
        print("Started to get PPI")
        if(self.config["takeDataFromFile"]):
            self.dataset.getPPIForAnalysesTemp()
        else:
            self.dataset.getPPIForAnalyses()
        print("Amount PPI:{0}".format(len(self.dataset.PPIs)))
        print("Finished to get PPI\nStarted to get DTI")
        if(self.config["takeDataFromFile"]):
            self.dataset.getDTIForAnlysesTemp()
        else:
            self.dataset.getDTIForAnlyses()
        print("Amount DTI:{0}".format(len(self.dataset.DTIs)))
        print("Finished to get DTI")
            
        if(self.config['showGraph']):
            self.dataset.toGraph()
    
        print("Started elaboration amount interactions")
        self.amountInteraction = self.dataset.getDTAmountInteractions()
        print("Completed elaboration amount interactions")

        print("Start getting DTI")
        self.df_DTI = self.dataset.getDTInteractionsTable()
        print("Operation completed")

        print("Start getting PPI")
        self.df_PPI = self.dataset.getPPInteractionsTable(weight=self.config['PPIWeighted'], noFilter=self.config['PPINotFiltered'])
        print("Operation completed")
        
        print("Count proteins which interact with drugs")
        print(len(self.df_DTI.columns)-1)
        print("Count proteins")
        print(len(self.df_PPI.columns)-1)
        
        if(self.config["saveDataOnFile"]):
            result = (self.amountInteraction.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0))
            self._saveDataframeOnCSV(result, self.config['nameFileAmountInteractions'])
            self._saveDataframeOnCSV(self.df_DTI, self.config['nameFileDTI'])
            self._saveDataframeOnCSV(self.df_PPI, self.config['nameFilePPI'])

    
    def initALSModel(self):
        print("Started initialization ALS model")
        self.ALSModel = ALSModel(self.amountInteraction,self.sparkSession)
        print("Completed initialization ALS model")
    
    def initFMMModel(self):
        print("Started initialization FM model alternative")
        self.FMModel= FMModel(self.amountInteraction,self.sparkSession ,self.df_DTI, self.df_PPI, self.config["isInteractionsMatrix"])
        print("Completed initialization FM model alternative")
        
    def areModelInitialized(self):
        if(self.ALSModel == None):
            self.initALSModel()
        
        if(self.FMModel == None):
            self.initFMMModel()

    def compareALSAndFM(self):
        self.areModelInitialized()
        
        seeds = random.sample(range(1, 101), self.config['amountOfSeed'])
        for seed in seeds:
            self.amountInteraction.groupBy("amount_interactions").count().show()
            (training, test) = self.amountInteraction.randomSplit([0.8, 0.2], seed=seed)
            print("Analyses for seed:{0}".format(seed))
            self.ALSModel.train(training=training, test = test, seed=seed)
            self.FMModel.train(training=training, test = test, seed=seed)
        
    def compareALSAndFMCrossValidation(self):
        self.areModelInitialized()
            
        self.ALSModel.crossValidationWithTest()
        self.FMModel.crossValidationWithTest()
    
    def compareRanking(self):
        self.areModelInitialized()
        self._saveDataframeOnCSV(self.amountInteraction, "amountInteraction")
        self.ALSModel.calculate_recommended_proteins(30)
        self.ALSModel.drug_proteins_recommended.show()
        self.FMModel.calculateRecommendedProteins(30)
        self.FMModel.drug_proteins_recommended.show()
        self._saveDataframeOnCSV(self.ALSModel.drug_proteins_recommended, "DPR_ALS_Comp")
        self._saveDataframeOnCSV(self.ALSModel.drug_proteins_recommended, "DPR_FMM_Comp")
        # self._saveDataframeOnCSV(self.FMModel.drug_proteins_recommended, "DPR_FMM_OLD")
        # print(self.FMModel.drug_proteins_recommended.count())
        # join = self.FMModel.drug_proteins_recommended.join(self.ALSModel.drug_proteins_recommended, \
        #                                             (self.ALSModel.drug_proteins_recommended.drugId == self.FMModel.drug_proteins_recommended.drugId_int) \
        #                                             & (self.ALSModel.drug_proteins_recommended.proteinId == self.FMModel.drug_proteins_recommended.proteinId_int)
        #                                             )
        # self._saveDataframeOnCSV(join, "DPR_JOIN")
        
    
    def _calculateDictionaryFrequency(dict : dict):
        dictToReturn = {}
        amountValue = sum(dict.values())
        for key in dict.keys():
            dictToReturn[key] =  float(dict[key]/amountValue)
        return dictToReturn

    def _calculateDictionaryAmountDegree(dict : dict):
        dictToReturn = {}
        for value in dict.values():
            if(value in dictToReturn):
                dictToReturn[value] = dictToReturn[value] + 1
            else:
                dictToReturn[value] = 1
        return dictToReturn

    def analysysBiologicalNetwork(self):
        print("Save DTI on file")
        df_DTI = self.dataset._DTIToDataFrame()

        print("Save PPI on file")
        df_PPI = self.dataset._PPIToDataFrame()

        print("Amount DTI:{0}".format(df_DTI.count()))
        print("Amount PPI:{0}".format(df_PPI.count()))
        
    
        print("Amount drugs:{0}".format(len(df_DTI.select('drugId').distinct().collect())) )
        
        proteins_degree = {}
        
        for ppi in df_PPI.select('proteinAId', 'proteinBId').distinct().collect():
            if(ppi.proteinAId in proteins_degree):
                proteins_degree[ppi.proteinAId] = proteins_degree[ppi.proteinAId] + 1
            else:
                proteins_degree[ppi.proteinAId] = 1
            
            if(ppi.proteinBId in proteins_degree):
                proteins_degree[ppi.proteinBId] = proteins_degree[ppi.proteinBId] + 1
            else:
                proteins_degree[ppi.proteinBId] = 1

        print("Amount proteins:{0}".format(len(proteins_degree.keys())))
        
        print("Calculate degree distrubution PPI")
        degree_distribution_protein = self._calculateDictionaryAmountDegree(proteins_degree)            
        degree_distribution_protein_f = self._calculateDictionaryFrequency(degree_distribution_protein)
        Histogram.show("Degree distribution PPI", "Degree", "Frequency", degree_distribution_protein_f.keys(), degree_distribution_protein_f.values())   
                
        
        drug_degree = {} 
        for drug in df_DTI.select('drugId').collect():
            if(drug in drug_degree):
                drug_degree[drug] = drug_degree[drug] + 1
            else:
                drug_degree[drug] = 1
            

        print("Amount drugs:{0}".format(len(drug_degree.keys())))
        degree_distribution_drug = self._calculateDictionaryAmountDegree(drug_degree)            
        degree_distribution_drug_f = self._calculateDictionaryFrequency(degree_distribution_drug)
        Histogram.show("Degree distribution Drugs in DTI", "Degree", "Frequency", degree_distribution_drug_f.keys(), degree_distribution_drug_f.values())   
                
            
            
        proteins_drug_degree = {}
        print(len(df_DTI.select('proteinId').collect()))
        for protein in df_DTI.select('proteinId').orderBy('proteinId').collect():
            proteinToAdd = str(protein.proteinId)
            if(proteinToAdd in proteins_drug_degree):
                proteins_drug_degree[proteinToAdd] = proteins_drug_degree[proteinToAdd] + 1
            else:
                proteins_drug_degree[proteinToAdd] = 1

        print("Amount proteins linked with drugs:{0}".format(len(proteins_drug_degree.keys())))
        degree_distribution_protein_drug = self._calculateDictionaryAmountDegree(proteins_drug_degree)            
        degree_distribution_protein_drug_F = self._calculateDictionaryFrequency(degree_distribution_protein_drug)
        Histogram.show("Degree distribution Proteins in DTI", "Degree", "Frequency", degree_distribution_protein_drug_F.keys(), degree_distribution_protein_drug_F.values())   
                
                
                
        degree_distribution_protein_drug_total = {}
        print("Degree total protein_drug")    
        for key in proteins_drug_degree.keys():
            total = proteins_drug_degree[key] + proteins_degree[key]
            if total in degree_distribution_protein_drug_total:
                degree_distribution_protein_drug_total[total] = degree_distribution_protein_drug_total[total] +1
            else:
                degree_distribution_protein_drug_total[total] = 1
        
        degree_distribution_protein_drug_total_f = self._calculateDictionaryFrequency(degree_distribution_protein_drug_total)
        Histogram.show("Degree distribution Proteins in DTI and PPI", "Degree", "Frequency", degree_distribution_protein_drug_total_f.keys(), degree_distribution_protein_drug_total_f.values())              

    def disposeSparkSession(self):
        self.sparkSession.stop()
