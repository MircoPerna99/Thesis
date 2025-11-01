import pandas as pd
import json
import os
import xml.etree.ElementTree as ET
from configuration import Configuration
from DataAccess.Model.DTI_Model import DTIModel
from DataAccess.Model.PPI_Model import PPIModel

class RepositoryFile():
    def __init__(self, fileName : str):
        config = Configuration()
        if(not RepositoryFile._areParametersCorrected(fileName)):
            exit()
        
        self._fileName = fileName
        
    def _areParametersCorrected(fileName):
        if(fileName is None or fileName == ""):
            print("The fileName parameter is empty")
            return False
        
        return True
    
    def readFile(self, sep = '\t'):
        df = pd.read_csv(self._fileName,sep=sep,on_bad_lines='warn', nrows=6000)
        return df

    def readDTIs(self):
        fileName = os.path.join(os.path.dirname(__file__), "Thesis.DTI.json")
        with open(fileName, 'r') as file:
            DTIsJson =  json.load(file)
            DTIs = []
            for DTI in DTIsJson:
                DTIToAdd =  DTIModel()
                DTIToAdd.setValuesFromMongo(drugId=DTI['drugId'], proteinId=DTI['proteinId'])
                DTIs.append(DTIToAdd)
            
            return DTIs
        
    def readPPIs(self):
        fileName = os.path.join(os.path.dirname(__file__), "Thesis.PPI.json")
        with open(fileName, 'r') as file:
            PPIsJson =  json.load(file)
            PPIs = []
            for PPI in PPIsJson:
                PPIsToAdd =  PPIModel()
                PPIsToAdd.setValuesFromMongo(proteinAId=PPI['proteinAId'], proteinBId=PPI['proteinBId'], confidenceScore=float(PPI['score']))
                PPIs.append(PPIsToAdd)
            
            return PPIs

    