import pandas as pd

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
    
    def readFile(self):
        df = pd.read_csv(self._fileName,sep='\t',on_bad_lines='warn', nrows=20)
        return df
    