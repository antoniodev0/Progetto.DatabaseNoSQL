import pandas as pd
from py2neo import Graph, Node, Relationship

graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="riciclo100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="riciclo75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="riciclo50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="riciclo25")

# Dizionario per mappare le percentuali ai grafi
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# Tipi di dati
data_types = ['mittenti', 'destinatari', 'transazioni']

for data_type in data_types:
    for percentage in graphs_by_percentage:
        csv_filename = f'{data_type}_{percentage}%.csv'
        
        # Leggi il file CSV
        data = pd.read_csv(csv_filename)
    
        # Converti i dati in una lista di dizionari
        data_dict_list = data.to_dict(orient='records')
         
        # Ottiene il grafo corrispondente alla percentuale
        graph = graphs_by_percentage[percentage]
        
        # Inserisce i dati nel grafo
        for index, row in data.iterrows():
            node = Node(data_type, **row.to_dict())
            graph.create(node)

            if data_type == 'transazioni':
                
                id_mittente = row['ID_mittente']
                nodo_mittente = graph.nodes.match('mittenti', ID_mittente=id_mittente).first()
                if nodo_mittente:
                    transazione_del_mittente = Relationship(nodo_mittente, 'EFFETTUATO_DA', node)
                    graph.create(transazione_del_mittente)

                id_destinatario = row['ID_destinatario']
                nodo_destinatario = graph.nodes.match('destinatari', ID_destinatario=id_destinatario).first()
                if nodo_destinatario:
                    transazione_al_destinatario = Relationship(node, 'RICEVUTO_DA', nodo_destinatario)
                    graph.create(transazione_al_destinatario)

        print(f"Dati del dataset {data_type}_{percentage}% inseriti in Neo4j con successo.")

print("Inserimento completato per tutti i dataset.")
