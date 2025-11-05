import sys
import os

sys.path.append(os.path.abspath("../"))

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer,IndexToString
from pyspark.ml.regression import FMRegressor
from pyspark.sql import SparkSession
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
import pandas as pd
import numpy as np
from Services.configuration import Configuration

class FMModel():
    def __init__(self, data, sparkSession, DTI_fm = None, PPI_fm = None, isAlternative = False):
        self.spark = sparkSession
        self.data = data
        self._config = Configuration()
        if(isAlternative):
            self.createMatrixAlternative(DTI_fm, PPI_fm)
        else:
            self.createMatrix()

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
        for column in dataframe.columns:
            if column != columnNoToChange: 
                dataframe = dataframe.withColumnRenamed(column, column+suffix)
                
        return dataframe
    
    def _clean_data(self):
        self.data = self.data.drop('drugId')
        self.data = self.data.drop('proteinId')
        self.data = self.data.drop('drugId_one_hot')
        self.data = self.data.drop('proteinId_one_hot')
        
    def _createFinalDataSet(self):
        columnsToSave = ["amount_interactions", "proteinId_int", "drugId_int"]
        self.columnsToRemove = [col for col in self.data.columns if col not in columnsToSave]
        assembler = VectorAssembler(inputCols=self.columnsToRemove, outputCol="features")
        
        self.data = assembler.transform(self.data)
        self.data = self.data.select("proteinId_int", "drugId_int","features" ,"amount_interactions")
        self.data.show(2)
        
    def createMatrixAlternative(self, DTI_fm, PPI_fm):
        df_drugs_ps = self._createOneHotCodeDF(True)
        df_target_ps = self._createOneHotCodeDF(False)
                
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")
        
        DTI_fm = self._addSuffixToColumns("_DTI","drugId",DTI_fm)
        PPI_fm = self._addSuffixToColumns("_PPI","proteinId",PPI_fm)
        
        DTI_fm = DTI_fm.orderBy("drugId")  
        PPI_fm = PPI_fm.orderBy("proteinId")   
        
        df_table = df_inter.join(DTI_fm, DTI_fm.drugId == df_inter.drugId_int)
        df_table = df_table.join(PPI_fm, PPI_fm.proteinId == df_inter.proteinId_int)
        # df_table = df_table.join(df_drugs_ps, df_table.drugId_int == df_drugs_ps.drugId_one_hot)
        # df_table = df_table.join(df_target_ps, df_table.proteinId_int == df_target_ps.proteinId_one_hot)
        self.data = df_table.orderBy("drugId_int", "proteinId_int")
        self._clean_data()
        self.data.write.mode("overwrite").option("header", True).csv("ML_Models/dati_dataset_training")
        self._createFinalDataSet()

    
    def createMatrix(self):
        df_drugs_ps = self._createOneHotCodeDF(True)
        df_target_ps = self._createOneHotCodeDF(False)
        
        df_drugs_ps = df_drugs_ps.orderBy("drugId_one_hot")
        df_target_ps = df_target_ps.orderBy("proteinId_one_hot")
        
        df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0)
        for column in df_table.columns:
            if column != "drugId": 
                df_table = df_table.withColumnRenamed(column, f"{column}_Int")
        
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")

        df_table = df_table.join(df_inter, df_table.drugId == df_inter.drugId_int)
        df_table = df_table.join(df_drugs_ps, df_table.drugId == df_drugs_ps.drugId_one_hot)
        df_table = df_table.join(df_target_ps, df_table.proteinId_int == df_target_ps.proteinId_one_hot)
        self.data = df_table.orderBy("drugId_int", "proteinId_int")
        self._clean_data()
        self.data.show(1)
        self._createFinalDataSet()

    def train(self, seed = 42):
        (training, test) = self.data.randomSplit([0.8, 0.2], seed=seed)
        
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
                        fm_model = fm.fit(training)
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
    
    ## da vedere
    # def dataframeOthersInteraction(self):
    #     df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(F.first("amount_interactions")).fillna(0)
    #     drugs = []
    #     for column in df_table.columns:
    #             if column != "drugId": 
    #                 drugs.append(column)
    #             df_table = df_table.withColumnRenamed(column, f"{column}_Tot")
                
    #     df_table = df_table.join(self.data, self.data.drugId == df_table.drugId_Tot)
    #     # df_table.show()
    #     for drug in drugs:
    #         df_table = df_table.withColumn(f"{drug}_Tot", F.when(F.col('proteinId') == drug, 0).otherwise(F.col(f"{drug}_Tot")))
        
    #     # df_table.show()
        
    #     df_table = df_table.drop("drugId_Tot")
    #     df_table = df_table.drop("amount_interactions")
    #     df_table = df_table.withColumnRenamed("drugId","drugId_Tot")
    #     df_table = df_table.withColumnRenamed("proteinId","proteinId_Tot")
    #     # df_table.show()
    #     return df_table
    
    def crossValidation(self, data = None):
        fm = FMRegressor(featuresCol='features', labelCol='amount_interactions')
        grid = ParamGridBuilder()\
                .addGrid(fm.regParam, self._config['hyperpameters_FM']['regParams'])\
                .addGrid(fm.maxIter, self._config['hyperpameters_FM']['maxIters'])\
                .addGrid(fm.initStd, self._config['hyperpameters_FM']['initStds'])\
                .addGrid(fm.factorSize, self._config['hyperpameters_FM']['factorSizes'] )\
                .build()
        
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
        
        cv = CrossValidator(estimator=fm, estimatorParamMaps=grid, evaluator=evaluator,parallelism=2, numFolds=5)
        if(data == None):
            self.cvModel = cv.fit(self.data)
        else:
            self.cvModel = cv.fit(data)    
                         
        self.index_best = np.argmin(self.cvModel.avgMetrics)
        map_hyper = self.cvModel.getEstimatorParamMaps()                       
        print("The best rmse is:{0}".format(self.cvModel.avgMetrics[ self.index_best]))
        print("The best hyperparameters are:{0}".format(map_hyper[ self.index_best]))
        return self.cvModel.avgMetrics[ self.index_best]
        
    def avgCrossvalidation(self):
        avgMetrics = []
        for i in range(10):
            result = self.crossValidation()
            avgMetrics.append(result)
        
        return avgMetrics