# Thesis
python3 -m venv venv     
source venv/bin/activate
pip3 install matplotlib numpy networkx pyspark scipy


# import sys
# import os

# # Aggiungi il percorso relativo della cartella dove si trova DataLayer
# sys.path.append(os.path.abspath("../"))


# print(os.path.abspath("../"))

Proteina da analizzare con la prof, perché rispecchia P03129  di uniprot
K02718 protein

uniprot all'incirca 2602 proteine su 1000 righe


# df = pd.DataFrame().from_records(ppi.toDict() for ppi in PPIs)
# G = nx.from_pandas_edgelist(df, "proteinAId", "proteinBId", "score")
# options = {
#     "font_size": 8,
#     "node_size": 100,
#     "node_color": "green",
#     "edgecolors": "black",
#     "with_labels": True
# }
# nx.draw(G, **options)
# plt.show()
# df = pd.crosstab(df.proteinAId, df.proteinBId)
# idx = df.columns.union(df.index)
# df = df.reindex(index = idx, columns=idx, fill_value=0)

# edgeList =  []
# for ppi in PPIs:
#         edgeList.append((ppi._proteinAId, ppi._proteinBId))


# g = ig.Graph(edges=edgeList)