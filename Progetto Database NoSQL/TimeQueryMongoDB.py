import time
import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy.stats import t

# Connessione al database MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['antiriciclaggio']

percentages = ['100%', '75%', '50%', '25%']
num_experiments = 31

# Eseguo le query 
for percentage in percentages:
    transazioni = f'transazioni_{percentage}'
    mittenti = f'mittenti_{percentage}'
    destinatari = f'destinatari_{percentage}'
    
    def query_1(percentage):
        pipeline_query_1 = [
        {
            '$match': {
                'Motivo_sospetto': 'Origine del denaro sospetta'
            }
        },
        {
            '$count': 'total'
        }
        ]
        suspicious_transactions_count = db[transazioni].aggregate(pipeline_query_1)

        
    def query_2(percentage):
        pipeline_query_2 = [
        {
            '$group': {
                '_id': '$Tipo_transazione',
                'total_amount': {'$sum': '$Importo'}
            }
        }
        ]
        total_amount_by_transaction_type = db[transazioni].aggregate(pipeline_query_2)
        
    def query_3(percentage):
        pipeline_query_3 = [
        {
        '$lookup': {
            'from': transazioni,
            'localField': "ID_mittente",
            'foreignField': "ID_mittente",
            'as': "transazioni_info"
        }
        },
        {
        '$match': {
            'Paese': {'$in': ['Italy', 'France']},
            'transazioni_info.Tipo_transazione': 'trasferimento',
            'transazioni_info.Importo': {'$gt': 0}
        }
        },
        {
        '$count': 'total'
        }
        ]
        mittenti_italiani_francesi_con_transazioni = db[mittenti].aggregate(pipeline_query_3)
    
    def query_4(percentage):
        pipeline_query_4 = [
        {
        '$lookup': {
            'from': transazioni,
            'localField': "ID_destinatario",
            'foreignField': "ID_destinatario",
            'as': "transazioni_info"
        }
        },
        {
        '$match': {
            'Cognome': 'Jones',
            'transazioni_info.Importo': {'$gt': 0}
        }
        },
        {
        '$count': 'total'
        }
        ]
        destinatari_jones_con_transazioni = db[destinatari].aggregate(pipeline_query_4)

# Funzione per eseguire un singolo esperimento
def run_experiment(query_func, *args):
    start_time = time.time()
    result = query_func(*args)
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000  # Converti in millisecondi
    return execution_time, result

# Esegui gli esperimenti
results = []

new_query_functions = [
    query_1,
    query_2,
    query_3,
    query_4
]

query_names = [
    'Query 1',
    'Query 2',
    'Query 3',
    'Query 4'
]

# Eseguo le query 
for percentage in percentages:
    
    print(f"\nContenuto informativo al {percentage}:")

    for query_func, query_name in zip(new_query_functions, query_names):
        experiment_data = {
            'Query': query_name,
            'Percentage': f"{percentage}",
            'First Execution Time (ms)': None,
            'Average Execution Time (ms)': None,
            'Confidence Interval (95%)': None
        }

        first_execution_time, _ = run_experiment(query_func, f'{percentage}%')

        experiment_data['First Execution Time (ms)'] = first_execution_time

        execution_times = []
        for _ in range(num_experiments - 1):
            execution_time, _ = run_experiment(query_func, f'{percentage}%')
            execution_times.append(execution_time)
        
        mean_execution_time = np.mean(execution_times)
        std_deviation = np.std(execution_times)
        confidence_interval = t.interval(0.95, num_experiments - 1, loc=mean_execution_time, scale=std_deviation / np.sqrt(num_experiments - 1))  # Calcolo dell'intervallo di confidenza
        experiment_data['Average Execution Time (ms)'] = mean_execution_time
        experiment_data['Confidence Interval (95%)'] = confidence_interval

        results.append(experiment_data)


# Crea un DataFrame con i risultati
results_df = pd.DataFrame(results)

# Salva il DataFrame in un file CSV
results_df.to_csv('query_times_MongoDB.csv', index=False)

# Stampa i dati in maniera tabellare
print("Risultati:")
print(results_df)


