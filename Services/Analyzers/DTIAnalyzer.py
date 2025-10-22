import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
from DataAccess.Model.DTI_Model import DTIModel
from Services.Graphs.Histogram import Histogram
class DTIAnalyzer():
    def  __init__(self):
        pass
    
    def _takeDTI(self):
        repositoryMongo = RepositoryMongo()
        DTIs = repositoryMongo.readDTIs()
        repositoryMongo.close_connection()
        return DTIs
    
    def evaluateProteinDistribution(self):
        DTIs = self._takeDTI()
        
        proteinDistribution = {}
        
        for dti in DTIs:
            if(dti._proteinId in proteinDistribution):
                proteinDistribution[dti._proteinId] = proteinDistribution[dti._proteinId]+ 1
            else:
                proteinDistribution[dti._proteinId] = 1
                
        return proteinDistribution

analyzer = DTIAnalyzer()
data = analyzer.evaluateProteinDistribution()
Histogram.show("prova", "Protein", "links", data.keys(), data.values())   
     
        
    