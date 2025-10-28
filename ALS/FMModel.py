import sys
import os

sys.path.append(os.path.abspath("../"))

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode
from pyspark.sql.functions import first
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import FMRegressor
from pyspark.sql import SparkSession
from pyspark.ml.functions import vector_to_array


import pandas as pd

class FMModel():
    def __init__(self, data, DTI_fm = None, PPI_fm = None, isAlternative = False):
        self.data = data
        if(isAlternative):
            self.createMatrixAlternative(DTI_fm, PPI_fm)
        else:
            self.createMatrix()
        self.indexing_name()
        self.train()

        
    def indexing_name(self):
        #self.data = self.data.drop('proteinId_int')
        self.data = self.data.drop('drugId')
        self.data = self.data.drop('proteinId')
        #self.data = self.data.drop('drugId_int')
        self.data = self.data.drop('drugId_one_hot')
        self.data = self.data.drop('proteinId_one_hot')
        columnsToSave = ["amount_interactions", "proteinId_int", "drugId_int"]
        self.columnsToRemove = [col for col in self.data.columns if col not in columnsToSave]
        assembler = VectorAssembler(inputCols=self.columnsToRemove, outputCol="features")
        
        self.data = assembler.transform(self.data)
        self.data = self.data.select("proteinId_int", "drugId_int","features" ,"amount_interactions")

    def _createOneHotCodeDF(self, isDrug : bool):
        if(isDrug):
            columns_name = [str(row.drugId) for row in self.data.select("drugId").distinct().collect()] 
        else:
            columns_name = [str(row.proteinId) for row in self.data.select("proteinId").distinct().collect()] 

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
        
        return pd.DataFrame(data=matrix,columns=columns_name)
        
    def createMatrix(self):
        df_drugs = self._createOneHotCodeDF(True)
        df_target = self._createOneHotCodeDF(False)
        
        spark = SparkSession.builder \
                .appName("FMModel")\
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()

        df_drugs_ps = spark.createDataFrame(df_drugs)
        df_target_ps = spark.createDataFrame(df_target)
        
        df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("amount_interactions")).fillna(0)
        for column in df_table.columns:
            if column != "drugId": 
                df_table = df_table.withColumnRenamed(column, f"{column}_Int")
        
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")

        df_table = df_table.join(df_inter, df_table.drugId == df_inter.drugId_int)
        df_table = df_table.join(df_drugs_ps, df_table.drugId == df_drugs_ps.drugId_one_hot)
        df_table = df_table.join(df_target_ps, df_table.proteinId_int == df_target_ps.proteinId_one_hot)
        self.data = df_table
        
    def createMatrixAlternative(self, DTI_fm, PPI_fm):
        df_drugs = self._createOneHotCodeDF(True)
        df_target = self._createOneHotCodeDF(False)
        
        spark = SparkSession.builder \
                .appName("FMModel")\
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()

        df_drugs_ps = spark.createDataFrame(df_drugs)
        df_target_ps = spark.createDataFrame(df_target)
        
        # df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("amount_interactions")).fillna(0)
        
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")
        
        #Change name drug-target
        for column in DTI_fm.columns:
            if column != "drugId": 
                DTI_fm = DTI_fm.withColumnRenamed(column, f"{column}_DTI")
        
        #Change name Protein-Protein
        for column in PPI_fm.columns:
            if column != "proteinId": 
                PPI_fm = PPI_fm.withColumnRenamed(column, f"{column}_PPI")
                
            
        
        df_table = df_inter.join(DTI_fm, DTI_fm.drugId == df_inter.drugId_int)
        df_table = df_table.join(PPI_fm, PPI_fm.proteinId == df_inter.proteinId_int)
        df_table = df_table.join(df_drugs_ps, df_table.drugId_int == df_drugs_ps.drugId_one_hot)
        df_table = df_table.join(df_target_ps, df_table.proteinId_int == df_target_ps.proteinId_one_hot)
        self.data = df_table

    def train(self):
        (training, test) = self.data.randomSplit([0.9, 0.1], seed=42)
        
        print("Amount of test:"+ str(test.count()))        
        regParams = [0.1]
        maxIters = [1000]
        initStds = [0.1]
        factorSizes = [2]
        
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
                        # self.predictions.show()
                        # df.write.mode("overwrite").option("header", True).csv("output_fm_prediction")
                        print("For regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3} , RMSE:{4}".format(regParam, maxIter, initStd, factorSize,rmse))

        print("Chosen parameters: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(self.aus_regParam, self.aus_maxIter, self.aus_initStd,self.aus_factorSize, self.aus_rmse))          
