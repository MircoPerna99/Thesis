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

class FMModel():
    def __init__(self, data):
        self.indexing_name(data)
        
    def indexing_name(self,data):
        self.drug_indexer = StringIndexer(inputCol = "drugId", outputCol = "ID_Drug_Index").fit(data)
        self.prontein_indexer = StringIndexer(inputCol = "proteinId", outputCol = "ID_Protein_Index").fit(data)
        pipeline = Pipeline(stages = [self.drug_indexer, self.prontein_indexer])
        self.data = pipeline.fit(data).transform(data)
        self.data.show()
    
    def train(self):
        (training, test) = self.data.randomSplit([0.8, 0.1], seed=42)