import sys
import os

sys.path.append(os.path.abspath("../"))
from pyspark.sql import functions as F
from ML_Models.Dataset import Dataset
from ML_Models.ALSModel import ALSModel
from ML_Models.FMModel import FMModel
from pyspark.sql import SparkSession
from configuration import Configuration
import random

def saveDataframeOnCSV(df, nameFile):
    df.write.mode("overwrite").option("header", True).csv(nameFile)

print("Initialization of config")
config = Configuration()
print("Initialization of config completed")
print("Open spark session")
sparkSession = SparkSession.builder \
                        .appName("Collaborative_Filtering")\
                        .config("spark.driver.host", "localhost") \
                        .config("spark.driver.bindAddress", "127.0.0.1") \
                        .getOrCreate()
sparkSession.conf.set("spark.sql.debug.maxToStringFields", 1000)

print("Starting initialization dataset")
dataset = Dataset(sparkSession)
print("Finished initialization dataset")
print("Started to get PPI")
dataset.getPPIForAnalyses()
print("Finished to get PPI\nStarted to get DTI")
dataset.getDTIForAnlyses()
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
df_PPI = dataset.getPPInteractionsTable(weight=config['PPIWeighted'], noFilter=config['PPIFiltered'])
saveDataframeOnCSV(df_PPI, config['nameFilePPI'])
print("Saving completed")

print("Started initialization ALS model")
modelAls = ALSModel(df)
print("Completed initialization ALS model")


print("Started initialization FM model with same dataframe of ALS model")
modelFM = FMModel(df, sparkSession)
print("Completed initialization FM model with same dataframe of ALS model")

print("Started initialization FM model alternative")
modelFM_Alternative = FMModel(df,sparkSession ,df_DTI, df_PPI, True)
print("Completed initialization FM model alternative")

print("Started analys with 10 different seed for create the dataset")
seeds = random.sample(range(1, 101), 2)
resultAls = []
resultsFM = []
resultsFMAlternative = []

for seed in seeds:
    print("Analyses for seed:{0}".format(seed))
    modelAls.train(seed)
    resultAls.append("Chosen parameters for seed{4}: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(modelAls.aus_regParam, modelAls.aus_rank, modelAls.aus_alpha, modelAls.aus_rmse,seed))  
    
    modelFM.train(seed)
    resultsFM.append("Chosen parameters for FM and for seed {5}: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM.aus_regParam, modelFM.aus_maxIter, modelFM.aus_initStd,modelFM.aus_factorSize, modelFM.aus_rmse,seed))

    modelFM_Alternative.train(seed)
    resultsFMAlternative.append("Chosen parameters for FM alternartive{5}: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(modelFM_Alternative.aus_regParam, modelFM_Alternative.aus_maxIter, modelFM_Alternative.aus_initStd,modelFM_Alternative.aus_factorSize, modelFM_Alternative.aus_rmse,seed))
    print("Completed for seed:{0}".format(seed))


print("Results ALS")
for result in resultAls:
    print(result)

print("Results FM")
for result in resultsFM:
    print(result)
    
print("results FM alterntive")
for result in resultsFMAlternative:
    print(result)
    

sparkSession.stop()

