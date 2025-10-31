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
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
class ALSModel():
    def __init__(self, data):
        self.indexing_name(data)
        # self.train()
        
    def indexing_name(self,data):
        self.drug_indexer = StringIndexer(inputCol = "drugId", outputCol = "ID_Drug_Index").fit(data)
        self.prontein_indexer = StringIndexer(inputCol = "proteinId", outputCol = "ID_Protein_Index").fit(data)
        pipeline = Pipeline(stages = [self.drug_indexer, self.prontein_indexer])
        self.data = pipeline.fit(data).transform(data)
        # self.data.show()
    
    def train(self, seed):
        (training, test) = self.data.randomSplit([0.8, 0.2], seed=seed)
        print("Amount of test:"+ str(test.count()))

        regParams = [0.01, 0.1]
        ranks = [25,30,35]
        alphas = [10.0, 20.0, 40.0, 60.0, 80.0, 100.0]
        #Best hyperparameters
        # regParams = [0.1]
        # ranks = [30,35]
        # alphas = [10.0]
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
                        
                    predictions.show()

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
            # proteins_recommended.show() 
            # self.data.show()
            self.from_index_to_name(proteins_recommended)
            # self.drug_proteins_recommended.show()
    
    def crossValidation(self):
        print("Inizio")
        aus_als = ALS(maxIter = 10, regParam = 0.1, rank = 35, alpha = 10, userCol = "ID_Drug_Index",
                                  itemCol = "ID_Protein_Index", ratingCol = "amount_interactions",coldStartStrategy = "drop")       
        grid = ParamGridBuilder().build()
        
        evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "amount_interactions", predictionCol = "prediction")
        
        cv = CrossValidator(estimator=aus_als, estimatorParamMaps=grid, evaluator=evaluator,parallelism=2, numFolds=5)
        print("Inizio fit")
        cvModel = cv.fit(self.data)
        print("fine fit")
        print(cvModel.avgMetrics)