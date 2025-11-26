import sys
import os

sys.path.append(os.path.abspath("../"))

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer,IndexToString
from pyspark.ml.regression import FMRegressor
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import rank, col
from pyspark.sql.types import StringType
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
import pandas as pd
import numpy as np
from Services.configuration import Configuration
import random
import time

class FMModel():
    def __init__(self, data, sparkSession, DTI_fm = None, PPI_fm = None, isInteractionsMatrix = False):
        self.saveMatrixOnFile = False
        self.spark = sparkSession
        self.data = data
        self._config = Configuration()
        self.DTI_fm = None
        self.PPI_fm = None
        self.model = None
        if(isInteractionsMatrix):
            self.createInteractionsMatrix(DTI_fm,PPI_fm)
        else:
            self.createFeedbackMatrix()
            
    def _saveMatrixOnFile(self, namefile = "dataset"):
        if(self.saveMatrixOnFile):
            print("Save result on file")
            self.data.write.mode("overwrite").option("header", True).csv("ML_Models/"+namefile)
            print("Completed result on file")

    def _createOneHotCodeDF(self, isDrug : bool):
        if(isDrug):
            columns_name = [str(row.drugId) for row in self.data.select("drugId").distinct().collect()]
        else:
            columns_name = [str(row.proteinId) for row in self.data.select("proteinId").distinct().collect()]

        columns_name.sort()
        matrix = []
        for name in columns_name:
            listToAdd = [0]*len(columns_name)
            index = columns_name.index(name)
            listToAdd[index] = 1
            listToAdd.append(name)
            matrix.append(listToAdd)

        if(isDrug):
            columns_name.append("drugId_one_hot")
        else:
            columns_name.append("proteinId_one_hot")

        return self.spark.createDataFrame(pd.DataFrame(data=matrix,columns=columns_name))

    def _addSuffixToColumns(self, suffix, columnNoToChange, dataframe):
        column_map = {c: c + suffix if c != columnNoToChange else c for c in dataframe.columns}

        dataframe = dataframe.withColumnsRenamed(column_map)
        return dataframe

    def _clean_data(self, data):
        data = data.drop('drugId')
        data = data.drop('proteinId')
        data = data.drop('drugId_one_hot')
        data = data.drop('proteinId_one_hot')
        data = data.drop('drugId_Tot')
        return data
    
    def _createFinalDataSet(self,data, withLabels = True):
        columnsToSave = ["amount_interactions", "proteinId_int", "drugId_int"]
        self.columnsToRemove = [col for col in data.columns if col not in columnsToSave]
        assembler = VectorAssembler(inputCols=self.columnsToRemove, outputCol="features")

        data = assembler.transform(data)
        if(withLabels):
            data = data.select("proteinId_int", "drugId_int","features" ,"amount_interactions")
        else:
            data = data.select("proteinId_int", "drugId_int","features")
        return data

    def createInteractionsMatrix(self, DTI_fm, PPI_fm):
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")

        DTI_fm = self._addSuffixToColumns("_DTI","drugId", DTI_fm)
        PPI_fm = self._addSuffixToColumns("_PPI","proteinId", PPI_fm)

        self.DTI_fm = DTI_fm.orderBy("drugId")
        self.PPI_fm = PPI_fm.orderBy("proteinId")
        df_table = self.DTI_fm.join(df_inter, self.DTI_fm.drugId == df_inter.drugId_int)
        df_table = self.PPI_fm.join(df_table, self.PPI_fm.proteinId == df_table.proteinId_int)
        self.data = df_table.orderBy("drugId_int", "proteinId_int")
        self.data = self._clean_data(self.data)
        self._saveMatrixOnFile("datasetInteractionsMatrix")
        self.data = self._createFinalDataSet(self.data )
    
    def dataframeOthersInteraction(self):
        df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0)
        targets = []
        for column in df_table.columns:
                if column != "drugId":
                    targets.append(column)
                df_table = df_table.withColumnRenamed(column, f"{column}_Tot")

        df_table = df_table.join(self.data, (self.data.drugId == df_table.drugId_Tot))
        for target in targets:
            df_table = df_table.withColumn(f"{target}_Tot", F.when(F.col('proteinId') == target, 0).otherwise(F.col(f"{target}_Tot")))

        df_table.show()
        
        return df_table
        
    def createFeedbackMatrix(self):
        df_drugs_ps = self._createOneHotCodeDF(True)
        df_target_ps = self._createOneHotCodeDF(False)

        df_drugs_ps = df_drugs_ps.orderBy("drugId_one_hot")
        df_target_ps = df_target_ps.orderBy("proteinId_one_hot")

        df_table = self.dataframeOthersInteraction()
        
        df_table = df_table.withColumnRenamed("drugId", "drugId_int")
        df_table = df_table.withColumnRenamed("proteinId", "proteinId_int")
        df_table = df_target_ps.join(df_table, df_table.proteinId_int == df_target_ps.proteinId_one_hot)
        df_table = df_drugs_ps.join(df_table, df_table.drugId_int == df_drugs_ps.drugId_one_hot)
        self.data = df_table.orderBy("drugId_int", "proteinId_int")
        
        self.data = self._clean_data(self.data)
        self._saveMatrixOnFile("datasetFeedbackMatrix")
        self.data = self._createFinalDataSet(self.data)
    
    def _defineSets(self, test, training, seed):
        if(test == None and training == None):
            return self.data.randomSplit([0.8, 0.2], seed=seed)
        else:
            training = training.withColumnRenamed("drugId", "drugId_int")
            training = training.withColumnRenamed("proteinId", "proteinId_int")
            test = test.withColumnRenamed("drugId", "drugId_int")
            test = test.withColumnRenamed("proteinId", "proteinId_int")
            training = self.data.join(training, on=["drugId_int", "proteinId_int"], how="semi")
            test = self.data.join(test, on=["drugId_int", "proteinId_int"], how="semi")

            return (training, test)
    
    def _compareTrainingTest(self, model, test, training): 
        predictionsTraining = model.transform(training)
        predictionsTraining.orderBy("amount_interactions", ascending=[False]).show()
        predictionsTest = model.transform(test)
        predictionsTest.orderBy("amount_interactions", ascending=[False]).show()
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
           
        print(evaluator.evaluate(predictionsTraining))
        print(evaluator.evaluate(predictionsTest))

    def train(self, test = None , training = None,seed = 42):
        (training, test) = self._defineSets(test, training, seed)    
        regParams = self._config['hyperpameters_FM']['regParams']
        maxIters = self._config['hyperpameters_FM']['maxIters']
        initStds = self._config['hyperpameters_FM']['initStds']
        factorSizes = self._config['hyperpameters_FM']['factorSizes']

        self.aus_regParam = 0.0
        self.aus_maxIter = 0
        self.aus_initStd = 0.0
        self.aus_factorSize = 0
        self.aus_rmse = 0.0

        for regParam in regParams:
            for maxIter in maxIters:
                for initStd in initStds:
                    for factorSize in factorSizes:

                        fm = FMRegressor(featuresCol='features', labelCol='amount_interactions', maxIter=maxIter, initStd = initStd, factorSize=factorSize, regParam = regParam)
                        start_time = time.time()
                        fm_model = fm.fit(training)
                        print("--- Time required %s seconds ---" % (time.time() - start_time))

                        predictions = fm_model.transform(test)
                        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
                        rmse = evaluator.evaluate(predictions)
                        if(self.aus_rmse == 0.0 or rmse < self.aus_rmse):
                            self.aus_regParam = regParam
                            self.aus_maxIter = maxIter
                            self.aus_initStd = initStd
                            self.aus_factorSize = factorSize
                            self.aus_rmse = rmse
                            self.model = fm_model
                            self.predictions = predictions

                        print("For regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3} , RMSE:{4}".format(regParam, maxIter, initStd, factorSize,rmse))

        print("Chosen parameters: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(self.aus_regParam, self.aus_maxIter, self.aus_initStd,self.aus_factorSize, self.aus_rmse))

        if(self._config['toCompareTrainingTest']):
            self._compareTrainingTest(self.model, test, training)
    
    def crossValidation(self, dataset = None):
        if(dataset == None):
            dataset = self.data
        
        fm = FMRegressor(featuresCol='features', labelCol='amount_interactions')
        grid = ParamGridBuilder()\
                .addGrid(fm.regParam, self._config['hyperpameters_FM']['regParams'])\
                .addGrid(fm.maxIter, self._config['hyperpameters_FM']['maxIters'])\
                .addGrid(fm.initStd, self._config['hyperpameters_FM']['initStds'])\
                .addGrid(fm.factorSize, self._config['hyperpameters_FM']['factorSizes'] )\
                .build()

        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")

        cv = CrossValidator(estimator=fm, estimatorParamMaps=grid, evaluator=evaluator,parallelism=6, numFolds=5)

        start_time = time.time()
        self.cvModel = cv.fit(dataset)
        print("--- Time required %s seconds ---" % (time.time() - start_time))

        self.index_best = np.argmin(self.cvModel.avgMetrics)
        map_hyper = self.cvModel.getEstimatorParamMaps()
        print("The best rmse is:{0}".format(self.cvModel.avgMetrics[ self.index_best]))
        print("The best hyperparameters are:{0}".format(map_hyper[ self.index_best]))   
    
    def crossValidationWithTest(self,test = None , training = None):
        (training, test) = self._defineSets(test, training,42) 
        
        self.crossValidation(training)
        
        if(self._config['toCompareTrainingTestCV']):
            self._compareTrainingTest(self.cvModel, test, training)
    
    def _createRanking(self, predictions, amountProteinsToSee):
        window = Window.partitionBy(predictions['drugId_int']).orderBy(predictions['prediction'].desc())
        return predictions.select('drugId_int','proteinId_int','prediction', rank().over(window).alias('rank')) \
                                                    .filter(col('rank') <= amountProteinsToSee) \
                                                    .orderBy("drugId_int","prediction", ascending=[True,False])\
    
    def calculateRecommendedProteins(self, amountProteinsToSee = 10):
        if(self.model == None):
            self.train()
       
        drugIds_frame = self.data.select("drugId_int").distinct()
  
        proteinIds_frame = self.data.select("proteinId_int").distinct()    
        df_inter = drugIds_frame.crossJoin(proteinIds_frame)
        df_table = self.DTI_fm.join(df_inter, self.DTI_fm.drugId == df_inter.drugId_int)
        df_table = self.PPI_fm.join(df_table, self.PPI_fm.proteinId == df_table.proteinId_int)
        data = df_table.orderBy("drugId_int", "proteinId_int")
        
        data = self._clean_data(data)
        data = self._createFinalDataSet(data,False)
        
        predictions = self.model.transform(data)
        
        self.drug_proteins_recommended = self._createRanking(predictions,amountProteinsToSee)
        
    def calculateRecommendedProteinsForOneDrug(self,drugId, amountProteinsToSee = 10):
        if(self.model == None):
            self.train()
       
        drugIds_frame = self.data.select("drugId_int").distinct().filter(col('drugId_int') == drugId) 

        proteinIds_frame = self.data.select("proteinId_int").distinct()    
        df_inter = drugIds_frame.crossJoin(proteinIds_frame)
        df_table = self.DTI_fm.join(df_inter, self.DTI_fm.drugId == df_inter.drugId_int)
        df_table = self.PPI_fm.join(df_table, self.PPI_fm.proteinId == df_table.proteinId_int)
        data = df_table.orderBy("drugId_int", "proteinId_int")
        
        data = self._clean_data(data)
        data = self._createFinalDataSet(data,False)
        
        predictions = self.model.transform(data)
        
        self.drug_proteins_recommended = self._createRanking(predictions,amountProteinsToSee)
                  
    def calculateRecommendedSpecificProteinsForOneDrug(self,drugId, proteins:list, amountProteinsToSee = 10):
        if(self.model == None):
            self.train()
       
        drugIds_frame = self.data.select("drugId_int").distinct().filter(col('drugId_int') == drugId) 
        
        proteinIds_frame = self.spark.createDataFrame(proteins, StringType())
        proteinIds_frame = proteinIds_frame.withColumnRenamed("value", "proteinId_int")
        df_inter = drugIds_frame.crossJoin(proteinIds_frame)
        df_table = self.DTI_fm.join(df_inter, self.DTI_fm.drugId == df_inter.drugId_int)
        df_table = self.PPI_fm.join(df_table, self.PPI_fm.proteinId == df_table.proteinId_int)
        data = df_table.orderBy("drugId_int", "proteinId_int")
        
        data = self._clean_data(data)
        data = self._createFinalDataSet(data,False)
        
        predictions = self.model.transform(data)
        
        self.drug_proteins_recommended = self._createRanking(predictions,amountProteinsToSee)
        

        
        