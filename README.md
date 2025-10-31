# Thesis
python3 -m venv venv     
source venv/bin/activate
pip3 install matplotlib numpy networkx pyspark scipy pymongo


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

Chosen parameters: regParam: 0.01, maxIter:5500, initStd:0.1,factorSize:3, RMSE:1.1691024908715422
Chosen parameters: regParam: 0.01, maxIter:1000, initStd:0.1,factorSize:11, RMSE:1.1748271059079674
Chosen parameters: regParam: 0.1, maxIter:900, initStd:0.1,factorSize:2, RMSE:1.0948655387229844


Chosen parameters: regParam: 0.01, maxIter:5000, initStd:0.1,factorSize:2, RMSE:0.7995355277211346

Chosen parameters for ALS: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.7379567205862622
Chosen parameters for FM: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:2, RMSE:1.529792468203947
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:2, RMSE:0.6768022922853879
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:2, RMSE:0.7097834369526423 senza i 0.5 sotto


Chosen parameters for ALS: regParam: 0.1, rank:30, alpha:10.0, RMSE:0.7925182766951163
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:4, RMSE:0.7573606755777234

Chosen parameters for ALS: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.7779192118500439
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:12, RMSE:0.7384839341962539

Chosen parameters for ALS: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.7499679300272661
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:2, RMSE:0.7627806393957951

Chosen parameters for ALS: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.7993442962714252
Chosen parameters for FM WEIGHT: regParam: 0.1, maxIter:1000, initStd:0.1,factorSize:8, RMSE:0.7285177197364054

Chosen parameters: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.7712180177574456
Chosen parameters: regParam: 0.1, rank:25, alpha:10.0, RMSE:0.8697872492728245
 Chosen parameters: regParam: 0.01, maxIter:1000, initStd:0.1,factorSize:2, RMSE:0.9743901860101165

 Chosen parameters: regParam: 0.01, maxIter:1000, initStd:0.1,factorSize:2, RMSE:0.9430104642619399
 Chosen parameters: regParam: 0.01, maxIter:5000, initStd:0.1,factorSize:26, RMSE:0.9414912812565441
 Chosen parameters: regParam: 0.1, maxIter:200, initStd:0.01,factorSize:2, RMSE:0.9555595217250432

0.7928889862823133
0.8890113045186723

 Chosen parameters: regParam: 0.1, rank:35, alpha:10.0, RMSE:0.8679239741689826