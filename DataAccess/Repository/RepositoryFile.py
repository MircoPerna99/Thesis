import pandas as pd
from lxml import etree
import xml.etree.ElementTree as ET


class RepositoryFile():
    def __init__(self, fileName : str):
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
    
    # def readFileXMLDTI(self):
    #     fileName = "/Users/mircoperna/Documents/Universita/Tesi/Code/Thesis/Dates/full database.xml"
    #     main_tag = "drug"
        
    #     DTIs = []
    #     print('sono qui')
    #     count = 0
    #     context = etree.iterparse(fileName, tag=main_tag)
    #     for event, elem in context:
    #             print('sono qui1')
    #             dti = {
    #                 "id": elem.findtext(".//drugbank-id[@primary='true']"),
    #             }
    #             print('sono qui 2')
    #             DTIs.append(dti) 
    #             count = count + 1
    #             # Libera la memoria usata dall’elemento
    #             elem.clear()
    #             while elem.getprevious() is not None:
    #                 del elem.getparent()[0]
    #             print('sono qui 3')
    #             if(count == 5):
    #                 break
        
    #     for DTI in DTIs:
    #         print(DTI)


    