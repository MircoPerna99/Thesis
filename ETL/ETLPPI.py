import sys
import os

# Aggiungi il percorso relativo della cartella dove si trova DataLayer
sys.path.append(os.path.abspath("../"))

# print(os.path.abspath("../")
from DataAccess.Model.PPI_Model import PPIModel
from DataAccess.Repository.RepositoryFile import RepositoryFile
import pandas as pd
import re

dfPPI = RepositoryFile('/Users/mircoperna/Documents/Universita/Tesi/Code/Thesis/Dates/PPI/human.txt').readFile()
PPIs = []

# prefix = ["uniprotkb:", "intact:"]
prefix = ["uniprotkb:",]
pattern = r"^(" + "|".join(prefix) + ")"
regex = re.compile(pattern)
        # if(regex.match(proteinId)):
        #     return re.sub(pattern, "", proteinId)
        # else:
        #     print("New prefix find ", proteinId)
        #     return None
        
result_df= dfPPI[ dfPPI['#ID(s) interactor A'].str.contains(pattern) == True ]
print(result_df['#ID(s) interactor A'].nunique())

# for i,row in dfPPI.iterrows():
#     PPIs.append(PPIModel(row['#ID(s) interactor A'], row['ID(s) interactor B'], row['Confidence value(s)']))
    
# for ppi in PPIs:
#     ppi.toString()

