from faker import Faker
import random
import csv

fake = Faker()

# Definisci il numero totale di transazioni
total_transactions =30000

# Creo delle motivazioni sospette in italiano
italian_suspicious_reasons = [
    "Importo insolitamente elevato",
    "Origine del denaro sospetta",
    "Transazione in un paese a rischio",
    "Attivita' finanziaria anomala",
    "Attivita' anomala in orari insoliti",
    "Transazioni con informazioni inconsistenti"
]

# Dizionari per memorizzare mittenti e destinatari
senders = {}
recipients = {}

# Percentuali richieste
percentages = [100, 75, 50, 25]

for percentage in percentages:
    num_records = int(total_transactions * (percentage / 100))

    # Apre un file CSV per scrivere i mittenti
    senders_csv_filename = f'mittenti_{percentage}%.csv'
    with open(senders_csv_filename, 'w', newline='', encoding='utf-8') as senders_csvfile:
        senders_fieldnames = ['ID_mittente', 'Nome', 'Cognome', 'Paese']
        senders_writer = csv.DictWriter(senders_csvfile, fieldnames=senders_fieldnames)
        senders_writer.writeheader()

        # Genera mittenti e registra gli ID
        for _ in range(num_records):
            sender_id = fake.uuid4()
            sender_first_name = fake.first_name()
            sender_last_name = fake.last_name()
            sender_country = fake.country()
            senders[sender_id] = {
                'Nome': sender_first_name,
                'Cognome': sender_last_name,
                'Paese': sender_country
            }

            senders_writer.writerow({
                'ID_mittente': sender_id,
                'Nome': sender_first_name,
                'Cognome': sender_last_name,
                'Paese': sender_country
            })

        print(f"File CSV mittenti {percentage}% generato con successo.")

    # Apre un file CSV per scrivere i destinatari
    recipients_csv_filename = f'destinatari_{percentage}%.csv'
    with open(recipients_csv_filename, 'w', newline='', encoding='utf-8') as recipients_csvfile:
        recipients_fieldnames = ['ID_destinatario', 'Nome', 'Cognome', 'Paese']
        recipients_writer = csv.DictWriter(recipients_csvfile, fieldnames=recipients_fieldnames)
        recipients_writer.writeheader()

        # Genera destinatari e registra gli ID
        for _ in range(num_records):
            recipient_id = fake.uuid4()
            recipient_first_name = fake.first_name()
            recipient_last_name = fake.last_name()
            recipient_country = fake.country()
            recipients[recipient_id] = {
                'Nome': recipient_first_name,
                'Cognome': recipient_last_name,
                'Paese': recipient_country
            }

            recipients_writer.writerow({
                'ID_destinatario': recipient_id,
                'Nome': recipient_first_name,
                'Cognome': recipient_last_name,
                'Paese': recipient_country
            })
    
        print(f"File CSV destinatari {percentage}% generato con successo.")

    # Apre un file CSV per scrivere le transazioni generate
    transactions_csv_filename = f'transazioni_{percentage}%.csv'
    with open(transactions_csv_filename, 'w', newline='', encoding='utf-8') as transactions_csvfile:
        transactions_fieldnames = ['ID_transazione', 'Data_ora', 'Importo', 'Tipo_transazione', 'ID_mittente', 'ID_destinatario', 'Motivo_sospetto', 'Stato']
        transactions_writer = csv.DictWriter(transactions_csvfile, fieldnames=transactions_fieldnames)
        transactions_writer.writeheader()

        # Genera transazioni utilizzando gli ID registrati per mittenti e destinatari
        for _ in range(num_records):
            transaction_id = fake.uuid4()
            transaction_datetime = fake.date_time_this_year()
            amount = round(random.uniform(100, 10000), 2)
            transaction_type = random.choice(['deposito', 'prelievo', 'trasferimento'])
            sender_id = random.choice(list(senders.keys()))
            recipient_id = random.choice(list(recipients.keys()))
            suspicious_reason = random.choice(italian_suspicious_reasons)
            status = random.choice(['in sospeso', 'completata', 'annullata'])

            transactions_writer.writerow({
                'ID_transazione': transaction_id,
                'Data_ora': transaction_datetime,
                'Importo': amount,
                'Tipo_transazione': transaction_type,
                'ID_mittente': sender_id,
                'ID_destinatario': recipient_id,
                'Motivo_sospetto': suspicious_reason,
                'Stato': status
            })

        print(f"File CSV transazioni {percentage}% generato con successo.")

