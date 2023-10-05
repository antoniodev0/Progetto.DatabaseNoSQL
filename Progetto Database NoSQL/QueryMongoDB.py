from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['antiriciclaggio']

percentages = ['100%', '75%', '50%', '25%']

for percentage in percentages:
    transazioni = f'transazioni_{percentage}'
    mittenti = f'mittenti_{percentage}'
    destinatari = f'destinatari_{percentage}'
    print(f"Analisi per la percentuale: {percentage}")

    # Query 1: Conteggio delle transazioni sospette con motivo sospetto "origine del denaro sospetta"
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

    for entry in suspicious_transactions_count:
        total_suspicious_transactions = entry['total']
        print(f"Numero totale di transazioni con motivo sospetto 'origine del denaro sospetta': {total_suspicious_transactions}")
    print()
    # Query 2: Importo totale delle transazioni per tipo di transazione
    pipeline_query_2 = [
        {
            '$group': {
                '_id': '$Tipo_transazione',
                'total_amount': {'$sum': '$Importo'}
            }
        }
    ]
    total_amount_by_transaction_type = db[transazioni].aggregate(pipeline_query_2)

    print("Importo totale delle transazioni per tipo di transazione:")
    for entry in total_amount_by_transaction_type:
        print(f"Tipo di transazione: {entry['_id']}, Importo totale: {entry['total_amount']}")
    print()

    # Query 3: Numero di mittenti italiani e francesi che hanno effettuato transazioni di tipo trasferimento
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

    for entry in mittenti_italiani_francesi_con_transazioni:
        print(f"Numero totale di mittenti italiani e francesi che hanno effettuato transazioni: {entry['total']}")
    print()

     # Query 4: Numero di destinatari con il cognome Jones che hanno ricevuto una transazione
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

    for entry in destinatari_jones_con_transazioni:
        print(f"Numero totale di destinatari con importo da transazioni: {entry['total']}")
    print()
