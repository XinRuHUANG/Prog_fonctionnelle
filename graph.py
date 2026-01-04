import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import community  # python-louvain
import matplotlib.patches as mpatches

# Charger le CSV
df = pd.read_csv("H:/Desktop/CY TECH/ING2/PROG FONCT/Projet/graph_export/graph.csv", encoding="utf-16")

# Construire le graphe
G = nx.from_pandas_edgelist(df, "source", "target", edge_attr="line")

# Détection des communautés (méthode Louvain)
partition = community.best_partition(G)  # dict: node -> cluster_id

# Liste des clusters
clusters = list(set(partition.values()))

# Générer une couleur par cluster
cmap = plt.cm.get_cmap("tab10", len(clusters))
cluster_colors = {cluster: cmap(i) for i, cluster in enumerate(clusters)}

# Couleur de chaque nœud selon son cluster
node_colors = [cluster_colors[partition[node]] for node in G.nodes()]

# Affichage
plt.figure(figsize=(12, 12))
pos = nx.spring_layout(G, seed=42)

nx.draw(
    G, pos,
    with_labels=True,
    node_size=500,
    font_size=8,
    node_color=node_colors,
    edge_color="gray"
)

# Légende des clusters
patches = [
    mpatches.Patch(color=cluster_colors[c], label=f"Cluster {c}")
    for c in clusters
]
plt.legend(handles=patches, title="Communautés détectées", loc="upper right")

plt.title("Graphe des stations (clusters détectés automatiquement)")
plt.suptitle("Chaque cluster correspond à une ligne", fontsize=12, y=0.92)
plt.show()
