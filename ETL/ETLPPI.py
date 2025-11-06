import sys
import os

sys.path.append(os.path.abspath("../"))

from DataAccess.Model.PPI_Model import PPIModel
from DataAccess.Repository.RepositoryFile import RepositoryFile
from DataAccess.Repository.RepositoryMongo import RepositoryMongo
import matplotlib.pyplot as plt
import pandas as pd
import networkx as nx
import re

class ETLPPI():
        def __init__(self):
                self._dfPPI = RepositoryFile('/Users/mircoperna/Documents/Universita/Tesi/Code/Thesis/Dates/PPI/human.txt').readFile()
                self._prefix = ["uniprotkb:",]
                self._pattern = r"^(" + "|".join(self._prefix) + ")"

        def countProtein(self):
                print(self._dfPPI['#ID(s) interactor A'].nunique())
        
        def filterPPI(self):
                self._dfPPI= self._dfPPI[(self._dfPPI['#ID(s) interactor A'].str.contains(self._pattern) == True)]
                self._dfPPI= self._dfPPI[(self._dfPPI['ID(s) interactor B'].str.contains(self._pattern) == True)]
                self._dfPPI= self._dfPPI[((self._dfPPI['ID(s) interactor B'] != self._dfPPI['#ID(s) interactor A']) == True)]
                self._dfPPI = self._dfPPI.drop_duplicates(subset=['#ID(s) interactor A','ID(s) interactor B','Confidence value(s)'])

        def mappingPPI(self):
                raise Exception("Define if it shoulf be used")
                unique_vals = pd.unique(self._dfPPI[['#ID(s) interactor A', 'ID(s) interactor B']].values.ravel())
                mapping = {val: i+1 for i, val in enumerate(unique_vals)}
                self._dfPPI = self._dfPPI.assign(
                                id_A_num = self._dfPPI['#ID(s) interactor A'].map(mapping),
                                id_B_num=self._dfPPI['ID(s) interactor B'].map(mapping)
                                )
                print(self._dfPPI)
                
        def fromDataFrameToModel(self):
                PPIs = []
                for index, row in self._dfPPI.iterrows():
                        newPPI = PPIModel()
                        newPPI.setValuesFromText(row['#ID(s) interactor A'], row['ID(s) interactor B'] , row['Confidence value(s)'])
                        PPIs.append(newPPI)
                
                return PPIs
        
        def syncFromTextToMongo(self):
                self.filterPPI()
                self.countProtein()
                PPIs = self.fromDataFrameToModel()

                repositoryMongo = RepositoryMongo()

                for ppi in PPIs:
                        repositoryMongo.insertPPI(ppi)

                repositoryMongo.close_connection()
        

etl = ETLPPI()
etl.syncFromTextToMongo()
