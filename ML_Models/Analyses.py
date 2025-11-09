import sys
import os

sys.path.append(os.path.abspath("../"))
from pyspark.sql import functions as F
from ML_Models.Dataset import Dataset
from ML_Models.ALSModel import ALSModel
from ML_Models.FMModel import FMModel
from pyspark.sql import SparkSession
from Services.configuration import Configuration
import random
import json 
import numpy as np
from Services.Graphs.Histogram import Histogram



def saveDataframeOnCSV(df, nameFile):
    df.write.mode("overwrite").option("header", True).csv("ML_Models/"+nameFile)

def saveResultsOnFile(list, file_name):
    with open("ML_Models/"+file_name, 'w') as f:
        for item in list:
            f.write(f"{item}\n")
            
def applyAnlyses():
    print("Initialization of config")
    config = Configuration()
    print("Initialization of config completed")
    print("Open spark session")
    sparkSession = SparkSession.builder \
                            .appName("Collaborative_Filtering")\
                            .config("spark.driver.host", "localhost") \
                            .config("spark.ui.showConsoleProgress", "false") \
                            .config("spark.driver.bindAddress", "127.0.0.1") \
                            .config("spark.driver.memory", "16g") \
                            .getOrCreate()
                            
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.debug.maxToStringFields", 10000)
    print("Starting initialization dataset")
    dataset = Dataset(sparkSession)
    print("Finished initialization dataset")
    print("Started to get PPI")
    dataset.getPPIForAnalysesTemp()
    print("Amount PPI:{0}".format(len(dataset.PPIs)))
    print("Finished to get PPI\nStarted to get DTI")
    dataset.getDTIForAnlysesTemp()
    print("Amount DTI:{0}".format(len(dataset.DTIs)))
    print("Finished to get DTI")
        
    if(config['showGraph']):
        dataset.toGraph()

    print("Started elaboration amount interactions")
    df = dataset.getDTAmountInteractions()
    print("Completed elaboration amount interactions")

    print("Save amount interactions on file")
    result = (df.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0))
    saveDataframeOnCSV(result, config['nameFileAmountInteractions'])
    print("Saving completed")

    print("Save DTI on file")
    df_DTI = dataset.getDTInteractionsTable()
    saveDataframeOnCSV(df_DTI, config['nameFileDTI'])
    print("Saving completed")

    print("Save PPI on file")
    df_PPI = dataset.getPPInteractionsTable(weight=config['PPIWeighted'], noFilter=config['PPINotFiltered'])
    saveDataframeOnCSV(df_PPI, config['nameFilePPI'])
    print("Saving completed")

    # print("Started initialization ALS model")
    # modelAls = ALSModel(df)
    # print("Completed initialization ALS model")


    print("Started initialization FM model with same dataframe of ALS model")
    modelFM = FMModel(df, sparkSession)
    print("Completed initialization FM model with same dataframe of ALS model")
    # print("Started initialization FM model alternative")
    # modelFM_Alternative = FMModel(df,sparkSession ,df_DTI, df_PPI, True)
    # print("Completed initialization FM model alternative")

    # print("Started analys with 10 different seed for create the dataset")
    seeds = random.sample(range(1, 101), config['amountOfSeed'])
    # resultAls = []
    resultsFM = []
    #resultsFMAlternative = []

    for seed in seeds:
        print("Analyses for seed:{0}".format(seed))
        # modelAls.train(seed)
        # resultAls.append("Chosen parameters for seed{4}: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(modelAls.aus_regParam, modelAls.aus_rank, modelAls.aus_alpha, modelAls.aus_rmse,seed))  
        
        modelFM.train(seed)
        resultsFM.append("Chosen parameters for FM and for seed {5}: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM.aus_regParam, modelFM.aus_maxIter, modelFM.aus_initStd,modelFM.aus_factorSize, modelFM.aus_rmse,seed))

        # modelFM_Alternative.train(seed)
        # resultsFMAlternative.append("Chosen parameters for FM alternartive{5}: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM_Alternative.aus_regParam, modelFM_Alternative.aus_maxIter, modelFM_Alternative.aus_initStd,modelFM_Alternative.aus_factorSize, modelFM_Alternative.aus_rmse,seed))
        # print("Completed for seed:{0}".format(seed))


    # print("Results ALS")
    # for result in resultAls:
    #     print(result)

    # print("Results FM")
    # for result in resultsFM:
    #     print(result)
        
    # print("results FM alterntive")
    # for result in resultsFMAlternative:
    #     print(result)

    # print("Save result on file")
    # saveResultsOnFile(resultsFMAlternative, config['nameFileResultALS'])
    # saveResultsOnFile(resultAls, config['nameFileResultFM'])
    # print("Completed saving result on file")
    sparkSession.stop()


def applyCrossValidation():
    print("Initialization of config")
    config = Configuration()
    print("Initialization of config completed")
    print("Open spark session")
    sparkSession = SparkSession.builder \
                            .appName("Cross_validation")\
                            .config("spark.driver.host", "localhost") \
                            .config("spark.ui.showConsoleProgress", "false") \
                            .config("spark.driver.bindAddress", "127.0.0.1") \
                            .config("spark.driver.memory", "32g") \
                            .config("spark.executor.memory", "16g") \
                            .config("spark.executor.cores", "8") \
                            .config("spark.driver.extraJavaOptions", "-Xss4m") \
                            .config("spark.executor.extraJavaOptions", "-Xss4m") \
                            .getOrCreate()
                            
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.debug.maxToStringFields", 10000)
    print("Starting initialization dataset")
    dataset = Dataset(sparkSession)
    print("Finished initialization dataset")
    print("Started to get PPI")
    dataset.getPPIForAnalysesTemp()
    print("Amount PPI:{0}".format(len(dataset.PPIs)))
    print("Finished to get PPI\nStarted to get DTI")
    dataset.getDTIForAnlysesTemp()
    print("Amount DTI:{0}".format(len(dataset.DTIs)))
    print("Finished to get DTI")

    if(config['showGraph']):
        dataset.toGraph()

    print("Started elaboration amount interactions")
    df = dataset.getDTAmountInteractions()
    print("Completed elaboration amount interactions")

    print("Save amount interactions on file")
    result = (df.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0))
    saveDataframeOnCSV(result, config['nameFileAmountInteractions']+"cv")
    print("Saving completed")

    print("Save DTI on file")
    df_DTI = dataset.getDTInteractionsTable()
    saveDataframeOnCSV(df_DTI, config['nameFileDTI']+"cv")
    print("Saving completed")

    print("Save PPI on file")
    df_PPI = dataset.getPPInteractionsTable(weight=config['PPIWeighted'], noFilter=config['PPINotFiltered'])
    saveDataframeOnCSV(df_PPI, config['nameFilePPI']+"cv")
    print("Saving completed")
    
    print("Save PPI weight on file")
    df_PPI_weigth = dataset.getPPInteractionsTable(weight=True, noFilter=config['PPINotFiltered'])
    saveDataframeOnCSV(df_PPI, config['nameFilePPI']+"weight_cv")
    print("Saving completed")
    

    print("Started initialization ALS model")
    modelAls = ALSModel(df)
    print("Completed initialization ALS model")


    print("Started initialization FM model with same dataframe of FM model")
    modelFM = FMModel(df, sparkSession)
    print("Completed initialization FM model with same dataframe of FM model")
    
    # print("Started initialization FM model alternative weight")
    # modelFM_Alternative_weigth = FMModel(df,sparkSession ,df_DTI, df_PPI_weigth, True)
    # print("Completed initialization FM model alternative weight")

    # print("Started initialization FM model alternative")
    # modelFM_Alternative = FMModel(df,sparkSession ,df_DTI, df_PPI, True)
    # print("Completed initialization FM model alternative")

    resultAls = []

    # print("Start cross validation ALS")
    # resultAls = modelAls.avgCrossvalidation()
    # avg = np.mean(resultAls)
    # resultAls.append("The mean is:{0}".format(avg))
    # print("Finish cross validation ALS")
    # print("Save result on file")
    # saveResultsOnFile(resultAls, "result_cross_als.txt")
    # print("Completed saving result on file")
    
    
    results = []
    print("Start cross validation FM")
    results = modelFM.avgCrossvalidation()
    avg = np.mean(results)
    results.append(avg)
    print("Finish cross validation FM")
    print("Save result on file")
    saveResultsOnFile(results, "result_cross_fm.txt")
    print("Completed saving result on file")
    # print("Start cross validation FM")
    # modelFM.crossValidation()
    # map_hyper = modelFM.cvModel.getEstimatorParamMaps()                       
    # results.append("The best rmse FM is:{0}".format(modelFM.cvModel.avgMetrics[modelFM.index_best]))
    # results.append("The best hyperparameters FM are:{0}".format(map_hyper[modelFM.index_best]))
    # print("Finish cross validation FM")
    # print("Save result on file")
    # saveResultsOnFile(results, "results_FM")
    # print("Completed result on file")
    
    # resultsFMAlternativeWeight = []
    # print("Start cross validation FM alternative")
    # resultsFMAlternativeWeight = modelFM_Alternative_weigth.avgCrossvalidation()
    # avg = np.mean(resultsFMAlternativeWeight)
    # resultsFMAlternativeWeight.append(avg)
    # print("Finish cross validation FM alternative weight")
    # print("Save result on file")
    # saveResultsOnFile(resultsFMAlternativeWeight, "result_cross_fm_weight.txt")
    # print("Completed saving result on file")
    # print("Start cross validation FM alternative weight")
    # modelFM_Alternative_weigth.crossValidation()
    # map_hyper = modelFM_Alternative_weigth.cvModel.getEstimatorParamMaps()                       
    # resultsFMAlternativeWeight.append("The best rmse FM alternative weight is:{0}".format(modelFM_Alternative_weigth.cvModel.avgMetrics[modelFM_Alternative_weigth.index_best]))
    # resultsFMAlternativeWeight.append("The best hyperparameters FM  alternative weight are:{0}".format(map_hyper[modelFM_Alternative_weigth.index_best]))
    # print("Finish cross validation FM alternative weight")
    # print("Save result on file")
    # saveResultsOnFile(resultsFMAlternativeWeight, "resultsFMAlternativeWeight")
    # print("Completed result on file")
    
    
    # resultsFMAlternative = []
    # print("Start cross validation FM alternative")
    # modelFM_Alternative.crossValidation()
    # map_hyper = modelFM_Alternative.cvModel.getEstimatorParamMaps()                       
    # resultsFMAlternative.append("The best rmse FM alternative  is:{0}".format(modelFM_Alternative.cvModel.avgMetrics[modelFM_Alternative.index_best]))
    # resultsFMAlternative.append("The best hyperparameters FM  alternative are:{0}".format(map_hyper[modelFM_Alternative.index_best]))
    # print("Finish cross validation FM alternative")
    # print("Save result on file")
    # saveResultsOnFile(resultsFMAlternative, "resultsFMAlternative")
    # print("Completed result on file")
    # print("Start cross validation FM alternative")
    # resultsFMAlternative = modelFM_Alternative.avgCrossvalidation()
    # avg = np.mean(resultsFMAlternative)
    # resultsFMAlternative.append(avg)
    # print("Finish cross validation FM alternative")
    # print("Save result on file")
    # saveResultsOnFile(resultsFMAlternative, "result_cross_fm.txt")
    # print("Completed saving result on file")
    
    # print("Take predictions from CV")
    # print("Divide training set and test set")
    # (training, test) = modelFM_Alternative.data.randomSplit([0.8, 0.2])
    # print("Start cross validation FM alternative")
    # modelFM_Alternative.crossValidation(training)
    # print("Finish cross validation FM alternative")
    # print("Start predictions")
    # predictions = modelFM_Alternative.cvModel.transform(test)
    # predictions.show()
    # predictions = predictions.select("proteinId_int","drugId_int", "amount_interactions", "prediction")
    # print("Save result on file")
    # saveDataframeOnCSV(predictions, config['nameFilePredictionsFMAlternative'])
    # print("Completed result on file")
    sparkSession.stop()
    
def calculateDictionaryFrequency(dict : dict):
    dictToReturn = {}
    amountKeys = sum(dict.values())
    for key in dict.keys():
       dictToReturn[key] =  float(dict[key]/amountKeys)
    return dictToReturn

def calculateDictionaryAmountDegree(dict : dict):
    dictToReturn = {}
    for value in dict.values():
        if(value in dictToReturn):
            dictToReturn[value] = dictToReturn[value] + 1
        else:
            dictToReturn[value] = 1
    return dictToReturn

def analysysBiologicalNetwork():
    print("Initialization of config")
    config = Configuration()
    print("Initialization of config completed")
    print("Open spark session")
    sparkSession = SparkSession.builder \
                            .appName("Cross_validation")\
                            .config("spark.driver.host", "localhost") \
                            .config("spark.ui.showConsoleProgress", "false") \
                            .config("spark.driver.bindAddress", "127.0.0.1") \
                            .config("spark.driver.memory", "32g") \
                            .config("spark.executor.memory", "16g") \
                            .config("spark.executor.cores", "8") \
                            .config("spark.driver.extraJavaOptions", "-Xss4m") \
                            .config("spark.executor.extraJavaOptions", "-Xss4m") \
                            .getOrCreate()
                            
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.debug.maxToStringFields", 10000)
    print("Starting initialization dataset")
    dataset = Dataset(sparkSession)
    print("Finished initialization dataset")
    print("Started to get PPI")
    dataset.getPPIForAnalysesTemp()
    print("Amount PPI:{0}".format(len(dataset.PPIs)))
    print("Finished to get PPI\nStarted to get DTI")
    dataset.getDTIForAnlysesTemp()
    print("Amount DTI:{0}".format(len(dataset.DTIs)))
    print("Finished to get DTI")

    if(config['showGraph']):
        dataset.toGraph()

    print("Started elaboration amount interactions")
    df = dataset.getDTAmountInteractions()
    print("Completed elaboration amount interactions")


    print("Save DTI on file")
    df_DTI = dataset._DTIToDataFrame()

    print("Save PPI on file")
    df_PPI = dataset._PPIToDataFrame()

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
    degree_distribution_protein = calculateDictionaryAmountDegree(proteins_degree)            
    degree_distribution_protein_f = calculateDictionaryFrequency(degree_distribution_protein)
    Histogram.show("Degree distribution PPI", "Degree", "Frequency", degree_distribution_protein_f.keys(), degree_distribution_protein_f.values())   
            
       
    drug_degree = {} 
    for drug in df_DTI.select('drugId').collect():
        if(drug in drug_degree):
            drug_degree[drug] = drug_degree[drug] + 1
        else:
            drug_degree[drug] = 1
        

    print("Amount drugs:{0}".format(len(drug_degree.keys())))
    degree_distribution_drug = calculateDictionaryAmountDegree(drug_degree)            
    degree_distribution_drug_f = calculateDictionaryFrequency(degree_distribution_drug)
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
    degree_distribution_protein_drug = calculateDictionaryAmountDegree(proteins_drug_degree)            
    degree_distribution_protein_drug_F = calculateDictionaryFrequency(degree_distribution_protein_drug)
    Histogram.show("Degree distribution Proteins in DTI", "Degree", "Frequency", degree_distribution_protein_drug_F.keys(), degree_distribution_protein_drug_F.values())   
            
               
            
    degree_distribution_protein_drug_total = {}
    print("Degree total protein_drug")    
    for key in proteins_drug_degree.keys():
        total = proteins_drug_degree[key] + proteins_degree[key]
        if total in degree_distribution_protein_drug_total:
            degree_distribution_protein_drug_total[total] = degree_distribution_protein_drug_total[total] +1
        else:
            degree_distribution_protein_drug_total[total] = 1
    
    degree_distribution_protein_drug_total_f = calculateDictionaryFrequency(degree_distribution_protein_drug_total)
    Histogram.show("Degree distribution Proteins in DTI and PPI", "Degree", "Frequency", degree_distribution_protein_drug_total_f.keys(), degree_distribution_protein_drug_total_f.values())              
            
    sparkSession.stop()

