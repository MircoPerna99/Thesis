import sys
import os

sys.path.append(os.path.abspath("../"))

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import numpy as np
from Services.configuration import Configuration
import random
import time


class ALSModel():
    def __init__(self, data):
        self._config = Configuration()
        self.indexing_name(data)
        
    def indexing_name(self,data):
        self.drug_indexer = StringIndexer(inputCol = "drugId", outputCol = "ID_Drug_Index").fit(data)
        self.prontein_indexer = StringIndexer(inputCol = "proteinId", outputCol = "ID_Protein_Index").fit(data)
        pipeline = Pipeline(stages = [self.drug_indexer, self.prontein_indexer])
        self.data = pipeline.fit(data).transform(data)
    
    def _defineSets(self, test, training, seed):
        if(test == None and training == None):
            return self.data.randomSplit([0.8, 0.2], seed=seed)
        else:
            training = self.data.join(training, on=["drugId", "proteinId"], how="semi")
            test = self.data.join(test, on=["drugId", "proteinId"], how="semi")

            return (training, test)
    
    def _compareTrainingTest(self, model, test, training): 
        predictionsTraining = model.transform(training)
        predictionsTraining.orderBy("amount_interactions", ascending=[False]).show()
        predictionsTest = model.transform(test)
        predictionsTest.orderBy("amount_interactions", ascending=[False]).show()
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
           
        print(evaluator.evaluate(predictionsTraining))
        print(evaluator.evaluate(predictionsTest))
    
    def train(self, test = None , training = None, seed = 42):
        (training, test) = self._defineSets(test, training,seed)
            
        regParams = self._config['hyperpameters_ALS']['regParams']
        ranks = self._config['hyperpameters_ALS']['ranks']
        alphas = self._config['hyperpameters_ALS']['alphas']

        self.aus_regParam = 0.0
        self.aus_rank = 0
        self.aus_alpha = 0.0
        self.aus_rmse = 0.0
        
        for regParam in regParams:
            for rank in ranks:
                for alpha in alphas:
                    aus_als = ALS(maxIter = 10, regParam = regParam, rank = rank, alpha = alpha, userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "amount_interactions",coldStartStrategy = "drop")
                    
                    start_time = time.time()
                    aus_model = aus_als.fit(training)
                    print("--- Time required %s seconds ---" % (time.time() - start_time))

                    predictions = aus_model.transform(test)
                    evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
                    rmse = evaluator.evaluate(predictions)
                    
                    if(self.aus_rmse == 0.0 or rmse < self.aus_rmse):
                        self.aus_regParam = regParam
                        self.aus_rank = rank
                        self.aus_alpha = alpha
                        self.aus_rmse = rmse
                        self.model = aus_model
                        self.predictions = predictions
                        
                    print("For regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(regParam, rank, alpha, rmse))
                     
        print("Chosen parameters: regParam: {0}, rank:{1}, alpha:{2}, RMSE:{3}".format(self.aus_regParam, self.aus_rank, self.aus_alpha, self.aus_rmse))          
        
        if(self._config['toCompareTrainingTest']):
            self._compareTrainingTest(self.model, test, training)


    def from_index_to_name(self, proteins_recommended):
            drug_name = IndexToString(inputCol = "ID_Drug_Index", outputCol = "drugId", labels = self.drug_indexer.labels)
            prontein_name= IndexToString(inputCol = "ID_Protein_Index", outputCol = "proteinId", labels = self.prontein_indexer.labels)
            pipeline = Pipeline(stages = [drug_name, prontein_name])
            self.drug_proteins_recommended = pipeline.fit(self.data).transform(proteins_recommended)
            self.drug_proteins_recommended =   self.drug_proteins_recommended.select("drugId","proteinId","rating")\
                                                                               .orderBy("drugId","rating")
                                                                               
    def calculate_recommended_proteins(self):
            amount_proteins_for_drug = 7
            proteins_recommended = self.model.recommendForAllUsers(amount_proteins_for_drug)
            proteins_recommended = proteins_recommended.withColumn("proteinAndRating", explode(proteins_recommended.recommendations))\
                                                            .select("ID_Drug_Index", "proteinAndRating.*")
            self.from_index_to_name(proteins_recommended)
    
    def crossValidation(self , dataset = None):
        if(dataset == None):
            dataset = self.data
            
        aus_als = ALS(userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "amount_interactions",coldStartStrategy = "drop")       
        grid = ParamGridBuilder()\
                .addGrid(aus_als.regParam, self._config['hyperpameters_ALS']['regParams'])\
                .addGrid(aus_als.rank, self._config['hyperpameters_ALS']['ranks'])\
                .addGrid(aus_als.alpha, self._config['hyperpameters_ALS']['alphas'])\
                .addGrid(aus_als.maxIter, self._config['hyperpameters_ALS']['maxIter'])\
                .build()
        
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
        
        cv = CrossValidator(estimator=aus_als, estimatorParamMaps=grid, evaluator=evaluator,parallelism=1, numFolds=5)
        self.cvModel = cv.fit(self.data)
        index_best = np.argmin(self.cvModel.avgMetrics)
        map_hyper = self.cvModel.getEstimatorParamMaps()                       
        print("The best rmse is:{0}".format(self.cvModel.avgMetrics[index_best]))
        print("The best hyperparameters are:{0}".format(map_hyper[index_best]))

    def crossValidationWithTest(self, test = None , training = None):
        (training, test) = self._defineSets(test, training, 42) 
       
        self.crossValidation(training)
        
        if(self._config['toCompareTrainingTestCV']):
            self._compareTrainingTest(self.cvModel, test, training)