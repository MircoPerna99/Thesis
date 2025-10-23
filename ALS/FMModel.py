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

class FMModel():
    def __init__(self, data):
        self.data = data
        self.createMatrix()
        self.indexing_name()
        self.train()

        
    def indexing_name(self):
        self.drug_indexer = StringIndexer(inputCol = "drugId", outputCol = "ID_Drug_Index").fit(self.data)
        self.prontein_indexer = StringIndexer(inputCol = "proteinId_int", outputCol = "ID_Protein_Index").fit(self.data)
        pipeline = Pipeline(stages = [self.drug_indexer, self.prontein_indexer])
        self.data = pipeline.fit(self.data).transform(self.data)
        self.data = self.data.drop('proteinId_int')
        self.data = self.data.drop('drugId')
        self.data = self.data.drop('ID_Drug_Index')
        self.data = self.data.drop('ID_Protein_Index')
        # self.data.show(1, truncate=False)
        columnsToSave = ["amount_interactions"]
        columnsToRemove = [col for col in self.data.columns if col not in columnsToSave]
        assembler = VectorAssembler(inputCols=columnsToRemove, outputCol="features")
        
        self.data = assembler.transform(self.data)
        self.data = self.data.select("features" ,"amount_interactions")
        # self.data.show(5, truncate=False)

        
    def createMatrix(self):
        df_table = self.data.orderBy("drugId").groupBy("drugId").pivot("proteinId").agg(first("amount_interactions")).fillna(0)
        df_inter = self.data.withColumnRenamed("drugId", "drugId_int")
        df_inter = df_inter.withColumnRenamed("proteinId", "proteinId_int")

        df_table = df_table.join(df_inter, df_table.drugId == df_inter.drugId_int)
        self.data = df_table.drop('drugId_int')

    def train(self):
        (training, test) = self.data.randomSplit([0.9, 0.1], seed=42)
        
        
        regParams = [0.01, 0.1]
        maxIters = [4500,5000,5500]
        initStds = [0.1,0.2,0.5]
        factorSizes = [2,3,4,8]
        
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
                            
                        print("For regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3} , RMSE:{4}".format(regParam, maxIter, initStd, factorSize,rmse))

        print("Chosen parameters: regParam: {0}, maxIter:{1}, initStd:{2},factorSize:{3}, RMSE:{4}".format(self.aus_regParam, self.aus_maxIter, self.aus_initStd,self.aus_factorSize, self.aus_rmse))          
