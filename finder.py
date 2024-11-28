import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import geodesic

# Configurações de hiperparâmetros
DBSCAN_EPS = 0.05  # Distância máxima em graus (~5km dependendo da geolocalização)
DBSCAN_MIN_SAMPLES = 2  # Mínimo de transações por cluster

# Função para carregar os dados
def load_data():
    # Simulando a carga de dados das tabelas
    transacoes = pd.DataFrame([
        {"TransacaoID": 1, "Valor": 200.0, "Latitude": -23.55052, "Longitude": -46.633308, "ChaveID": "A123"},
        {"TransacaoID": 2, "Valor": 210.0, "Latitude": -23.55053, "Longitude": -46.633309, "ChaveID": "A123"},
        {"TransacaoID": 3, "Valor": 50.0, "Latitude": -23.56000, "Longitude": -46.640000, "ChaveID": "B456"},
        {"TransacaoID": 4, "Valor": 55.0, "Latitude": -23.56001, "Longitude": -46.640001, "ChaveID": "B456"},
        {"TransacaoID": 5, "Valor": 400.0, "Latitude": -23.57000, "Longitude": -46.650000, "ChaveID": "C789"}
    ])
    
    notas_fiscais = pd.DataFrame([
        {"NotaFiscalID": 1, "Valor": 410.0, "Latitude": -23.57000, "Longitude": -46.650000, "ChaveNF": "NF123"},
        {"NotaFiscalID": 2, "Valor": 200.0, "Latitude": -23.55052, "Longitude": -46.633308, "ChaveNF": "NF456"},
        {"NotaFiscalID": 3, "Valor": 55.0, "Latitude": -23.56000, "Longitude": -46.640000, "ChaveNF": "NF789"}
    ])
    
    return transacoes, notas_fiscais

# Função para calcular distância geográfica
def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

# Clusterização das transações
def cluster_transactions(transacoes):
    coords = transacoes[["Latitude", "Longitude"]].values
    clustering = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES, metric="euclidean").fit(coords)
    
    transacoes["ClusterID"] = clustering.labels_
    return transacoes

# Associação de notas fiscais às transações
def associate_invoices(transacoes, notas_fiscais):
    transacoes["NotaFiscalID"] = None
    transacoes["DistanciaNF"] = None

    for idx, transacao in transacoes.iterrows():
        cluster_id = transacao["ClusterID"]
        if cluster_id == -1:  # Ignorar transações fora de clusters
            continue
        
        # Filtrar notas fiscais próximas geograficamente
        notas_candidatas = notas_fiscais.copy()
        notas_candidatas["Distancia"] = notas_candidatas.apply(
            lambda nf: calculate_distance(
                transacao["Latitude"], transacao["Longitude"],
                nf["Latitude"], nf["Longitude"]
            ),
            axis=1
        )
        notas_candidatas = notas_candidatas[notas_candidatas["Distancia"] < 5]  # Limite de 5 km
        
        # Procurar nota fiscal com o valor mais próximo
        if not notas_candidatas.empty:
            notas_candidatas["ErroValor"] = abs(notas_candidatas["Valor"] - transacao["Valor"])
            melhor_nota = notas_candidatas.sort_values(["ErroValor", "Distancia"]).iloc[0]
            transacoes.loc[idx, "NotaFiscalID"] = melhor_nota["NotaFiscalID"]
            transacoes.loc[idx, "DistanciaNF"] = melhor_nota["Distancia"]

    return transacoes

# Programa principal
def main():
    # Carregar os dados
    transacoes, notas_fiscais = load_data()
    print("Transações carregadas:")
    print(transacoes)
    print("\nNotas fiscais carregadas:")
    print(notas_fiscais)

    # Clusterizar transações
    transacoes = cluster_transactions(transacoes)
    print("\nTransações após clusterização:")
    print(transacoes)

    # Associar notas fiscais às transações
    transacoes = associate_invoices(transacoes, notas_fiscais)
    print("\nTransações após associação com notas fiscais:")
    print(transacoes)

# Executar o programa
if __name__ == "__main__":
    main()
