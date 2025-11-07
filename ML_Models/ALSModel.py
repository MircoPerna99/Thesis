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

class ALSModel():
    def __init__(self, data):
        self._confing = Configuration()
        self.indexing_name(data)
        
    def indexing_name(self,data):
        self.drug_indexer = StringIndexer(inputCol = "drugId", outputCol = "ID_Drug_Index").fit(data)
        self.prontein_indexer = StringIndexer(inputCol = "proteinId", outputCol = "ID_Protein_Index").fit(data)
        pipeline = Pipeline(stages = [self.drug_indexer, self.prontein_indexer])
        self.data = pipeline.fit(data).transform(data)
    
    def train(self, seed = 42):
        (training, test) = self.data.randomSplit([0.8, 0.2], seed=seed)

        regParams = self._confing['hyperpameters_ALS']['regParams']
        ranks = self._confing['hyperpameters_ALS']['ranks']
        alphas = self._confing['hyperpameters_ALS']['alphas']

        self.aus_regParam = 0.0
        self.aus_rank = 0
        self.aus_alpha = 0.0
        self.aus_rmse = 0.0
        
        for regParam in regParams:
            for rank in ranks:
                for alpha in alphas:
                    aus_als = ALS(maxIter = 10, regParam = regParam, rank = rank, alpha = alpha, userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "amount_interactions",coldStartStrategy = "drop")
                    
                    aus_model = aus_als.fit(training)
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
    
    def crossValidation(self):
        aus_als = ALS(userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "amount_interactions",coldStartStrategy = "drop", seed=42)       
        grid = ParamGridBuilder()\
                .addGrid(aus_als.regParam, self._confing['hyperpameters_ALS']['regParams'])\
                .addGrid(aus_als.rank, self._confing['hyperpameters_ALS']['ranks'])\
                .addGrid(aus_als.alpha, self._confing['hyperpameters_ALS']['alphas'])\
                .addGrid(aus_als.maxIter, self._confing['hyperpameters_ALS']['maxIter'])\
                .build()
        
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
        
        cv = CrossValidator(estimator=aus_als, estimatorParamMaps=grid, evaluator=evaluator,parallelism=1, numFolds=5)
        cvModel = cv.fit(self.data)
        index_best = np.argmin(cvModel.avgMetrics)
        map_hyper = cvModel.getEstimatorParamMaps()                       
        print("The best rmse is:{0}".format(cvModel.avgMetrics[index_best]))
        print("The best hyperparameters are:{0}".format(map_hyper[index_best]))
        return cvModel.avgMetrics[index_best]
        
    def avgCrossvalidation(self):
        avgMetrics = []
        for i in range(10):
            result = self.crossValidation()
            avgMetrics.append(result)
        
        return avgMetrics