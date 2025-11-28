import sys
import os

sys.path.append(os.path.abspath("../"))
import xml.etree.ElementTree as ET
from DataAccess.Model.DrugStructureModel import DrugStuctureModel
from DataAccess.Model.ProteinStructureModel import ProteinStuctureModel
from Services.configuration import Configuration

class RepositoryXML():
    def __init__(self):
        self.config = Configuration()
        self.structureDrugs = []
        self.structureProteins = []
    
    def _takeRoot(self):
        tree = ET.parse(self.config["xmlPath"])
        return tree.getroot()
    
    def _addStructure(self, id, structure, isDrug):
        if(isDrug):
            structureDrug = DrugStuctureModel()
            structureDrug.setValuesFromXML(id, structure)
            self.structureDrugs.append(structureDrug)
        else:
            structureProtein = ProteinStuctureModel()
            structureProtein.setValuesFromXML(id, structure)
            self.structureProteins.append(structureProtein)
            
    def _cleanFromDuplicate(self):
        self.structureDrugs = list(set(self.structureDrugs))
        self.structureProteins = list(set(self.structureProteins))
    
    def takeStructes(self):
        root = self._takeRoot()

        NS = {'db': 'http://www.drugbank.ca'} 

        drugs = root.findall('.//db:drug', NS)
        drugAmount = 1
        proteinAmount = 1
        for drug in drugs:
            primary_id = drug.find('./db:drugbank-id[@primary="true"]', NS)
            if(primary_id != None):
                drugId = primary_id.text
                # print(primary_id.text)
                
                smiles_prop = drug.find(".//db:property[db:kind='SMILES']/db:value", NS)
                if(smiles_prop != None):
                    # print(smiles_prop.text)
                    smile = smiles_prop.text
                    self._addStructure(drugId, smile,True)
                    print("Drug "+str(drugAmount))
                    drugAmount  = drugAmount +1 
                                      
                targets = drug.findall(".//db:target", NS)
                for target in targets:
                    polypetide = target.find(".//db:polypeptide", NS)
                    if(polypetide != None):
                        # print(polypetide.get("id"))
                        proteinId = polypetide.get("id")
                        sequence = polypetide.find(".//db:amino-acid-sequence", NS) 
                        # print(sequence.text)
                        fastaStructue = (sequence.text.split("\n",1)[1]).replace('\n','')
                        self._addStructure(proteinId, fastaStructue,False)
                        print("Protein "+str(proteinAmount))
                        proteinAmount  = proteinAmount +1 
            
            self._cleanFromDuplicate()

