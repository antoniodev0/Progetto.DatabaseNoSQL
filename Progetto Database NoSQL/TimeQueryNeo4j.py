from py2neo import Graph
import time
import csv
import statistics
from scipy.stats import t
import numpy as np
percentages = [100, 75, 50, 25]

# Definizione delle query
queries = [
    """
    MATCH (t:transazioni) 
    WHERE t.Motivo_sospetto = 'Origine del denaro sospetta' 
    RETURN COUNT(t) AS total_suspicious_transactions
    """,
    """
    MATCH (t:transazioni) 
    RETURN t.Tipo_transazione AS transaction_type, SUM(t.Importo) AS total_amount 
    ORDER BY transaction_type
    """,
    """
    MATCH (m:mittenti)-[:EFFETTUATO_DA]->(t:transazioni) 
    WHERE m.Paese IN ['Italy', 'France'] AND t.Tipo_transazione = 'trasferimento' AND t.Importo > 0 
    RETURN COUNT(DISTINCT m) AS mittenti_italiani_francesi_con_transazioni
    """,
    """
    MATCH (d:destinatari)<-[:RICEVUTO_DA]-(t:transazioni) 
    WHERE d.Cognome = 'Jones' AND t.Importo > 0 
    RETURN COUNT(DISTINCT d) AS destinatari_con_cognome_Jones
    """   
]

def calculate_confidence_interval(data):
    data = np.array(data[1:])  # Ignora il primo tempo (prima esecuzione)
    avg_execution_time = np.mean(data)
    std_dev = np.std(data, ddof=1)
    n = len(data)

    # Calcola l'intervallo di confidenza al 95%
    t_value = t.ppf(0.975, df=n-1)  # Trova il valore critico t per il 95% di confidenza
    margin_of_error = t_value * (std_dev / np.sqrt(n))

    confidence_interval = (avg_execution_time - margin_of_error, avg_execution_time + margin_of_error)

    return avg_execution_time, confidence_interval

execution_times = []

for percentage in percentages:
    print(f"Dimensioni dataset: {percentage}")

    db_name = f"riciclo{percentage}"
    graph = Graph(f"bolt://localhost:7687/{db_name}", user="neo4j", password="12345678", name=db_name)

    for query_idx, query in enumerate(queries):
        print(f"Query {query_idx + 1}")

        execution_times_query = []

        for _ in range(31):
            start_time = time.time()
            result = graph.run(query).data()
            

            end_time = time.time()
            execution_time = (end_time - start_time) * 1000
            execution_times_query.append(execution_time)

            print(f"Risultati query {query_idx + 1}:\n{result}")

        avg_execution_time, confidence_interval = calculate_confidence_interval(execution_times_query)

        first_execution_time = execution_times_query[0]

        print(f"Tempo di esecuzione medio (ms): {avg_execution_time}")
        print(f"Tempo della prima esecuzione (ms): {first_execution_time}")
        print(f"Intervallo di confidenza al 95%: {confidence_interval}")

        execution_times.append({
            "Query": f"Query {query_idx + 1}",
            "Percentage": f'{percentage}%',
            "First Execution Time (ms)": first_execution_time,
            "Average Execution Time (ms)": avg_execution_time,
            "Confidence Interval (95%)": confidence_interval,
        })

        print("-" * 40)

csv_file = 'query_times_Neo4j.csv'
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Query', 'Percentage', 'First Execution Time (ms)', 'Average Execution Time (ms)', 'Confidence Interval (95%)']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in execution_times:
        writer.writerow(data)