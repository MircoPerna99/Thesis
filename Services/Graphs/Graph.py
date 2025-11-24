import matplotlib.pyplot as plt
import networkx as nx

class Graph():
    
        def printDataFrame(self, dataframe, source, target):
            G = nx.from_pandas_edgelist(dataframe, source, target)
            options = {
                "font_size": 8,
                "node_size": 100,
                "node_color": "green",
                "edgecolors": "black",
                "with_labels": True
            }
            nx.draw(G, **options)
            plt.show()
            
        def printHeterogenousGraph(self, dataframe, source, target):
            G = nx.from_pandas_edgelist(dataframe, source, target)
            options = {
                "font_size": 8,
                "node_size": 100,
                "edgecolors": "black",
                "with_labels": True
            }
            
            target_nodes = dataframe[target].unique()
            
            color_map = []
            for node in G.nodes():
                if node in  target_nodes:
                    color_map.append('green')
                else:
                    color_map.append('blue')
                    
            nx.draw(G, node_color=color_map ,**options)
            plt.show()
            
        def printBiologicalNetwork(self, dfDTI, dfPPI):
            G = nx.from_pandas_edgelist(dfDTI, "drugId", "proteinId")
            G.add_edges_from(dfPPI[['proteinAId', 'proteinBId']].values)
            options = {
                "font_size": 8,
                "node_size": 100,
                "edgecolors": "black",
                "with_labels": False
            }
            
            target_nodes = list(dfDTI["proteinId"].unique())
            target_nodes.extend(list(dfPPI["proteinAId"].unique()))
            target_nodes.extend(list(dfPPI["proteinBId"].unique()))
            target_nodes = set(target_nodes)
            color_map = []
            for node in G.nodes():
                if node in  target_nodes:
                    color_map.append('green')
                else:
                    color_map.append('blue')
            
            nx.draw(G, node_color=color_map ,**options)
            plt.show()
            