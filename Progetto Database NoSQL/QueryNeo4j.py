from neo4j import GraphDatabase

# Connessione al database Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Lista delle percentuali e nomi dei database
percentages = [100, 75, 50, 25]
db_names = ['riciclo100', 'riciclo75', 'riciclo50', 'riciclo25']

def run_query(session, query):
    return session.run(query)

for percentage, db_name in zip(percentages, db_names):
    print(f"Analisi per la percentuale: {percentage}%")
    
    with driver.session(database=db_name) as session:
        # Query 1: Conteggio delle transazioni con motivo sospetto "Origine del denaro sospetta"
        query_1 = (
            f"MATCH (t:transazioni) "
            f"WHERE t.Motivo_sospetto = 'Origine del denaro sospetta' "
            f"RETURN COUNT(t) AS total_suspicious_transactions"
        )
        result_1 = run_query(session, query_1)
        total_suspcious_transactions = result_1.single()["total_suspcious_transactions"]
        print(f"Numero di transazioni sospette per motivo sospetto: {total_suspcious_transactions}")

        # Query 2: Importo totale delle transazioni per tipo di transazione
        query_2 = (
            f"MATCH (t:transazioni) "
            f"RETURN t.Tipo_transazione AS transaction_type, SUM(t.Importo) AS total_amount "
            f"ORDER BY transaction_type"
        )
        result_2 = run_query(session, query_2)
        print("Importo totale delle transazioni per tipo di transazione:")
        for record in result_2:
            print(f"Tipo di transazione: {record['transaction_type']}, Importo totale: {record['total_amount']}")
        print()
        
        # Query 3: Numero di mittenti italiani e francesi che hanno effettuato transazioni di tipo trasferimento
        query_3 = (
            f"MATCH (m:mittenti)-[:EFFETTUATO_DA]->(t:transazioni) "
            f"WHERE m.Paese IN ['Italy', 'France'] AND t.Tipo_transazione = 'trasferimento' AND t.Importo > 0 "
            f"RETURN COUNT(DISTINCT m) AS mittenti_italiani_francesi_con_transazioni"
        )
        result_3 = run_query(session, query_3)
        mittenti_italiani_francesi_con_transazioni = result_3.single()["mittenti_italiani_francesi_con_transazioni"]
        print(f"Numero di mittenti italiani e francesi che hanno effettuato transazioni: {mittenti_italiani_francesi_con_transazioni}")
        print()
       
       # Query 4:  Numero di destinatari con il cognome Jones che hanno ricevuto una transazione
        query_4 = (
            f"MATCH (d:destinatari)<-[:RICEVUTO_DA]-(t:transazioni) "
            f"WHERE d.Cognome = 'Jones' AND t.Importo > 0 "
            f"RETURN COUNT(DISTINCT d) AS destinatari_con_cognome_Jones"
        )
        result_4 = run_query(session, query_4)
        destinatari_con_importo_da_transazioni_jones = result_4.single()["destinatari_con_cognome_Jones"]
        print(f"Numero di destinatari con il cognome 'Jones' che hanno ricevuto un importo da transazioni: {destinatari_con_importo_da_transazioni_jones}")
        print()

# Chiudi la connessione al database Neo4j
driver.close()

