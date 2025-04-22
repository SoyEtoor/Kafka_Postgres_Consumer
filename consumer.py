from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask
import threading
import psycopg2
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

KAFKA_CONFIG = {
    'bootstrap.servers': 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'IngEnigma',
    'sasl.password': 'BrARBOxX98VI4f2LIuIT1911NYGrXu',
    'group.id': 'crimes-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

DB_PARAMS = {
    'dbname': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_bWF4oOStGl9s',
    'host': 'ep-royal-boat-a4jfdktg-pooler.us-east-1.aws.neon.tech',
    'port': '5432'
}

TOPIC = "crimes_pg"

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def insert_crime(data: dict):
    required_fields = ['dr_no', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']
    
    if not all(field in data for field in required_fields):
        logging.warning(f"Campos faltantes en el registro: {data}")
        return False

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (dr_no) DO NOTHING;
                """
                cur.execute(query, (
                    data['dr_no'],
                    data['report_date'],
                    data['victim_age'],
                    data['victim_sex'],
                    data['crm_cd_desc']
                ))
        logging.info(f"Registro insertado: DR No {data['dr_no']}")
        return True
    except psycopg2.errors.UniqueViolation:
        logging.warning(f"DR No duplicado, ya existe: {data['dr_no']}")
        return 
    except Exception as e:
        logging.error(f"Error al insertar el registro: {e}", exc_info=True)
        return False

def kafka_consumer_loop():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    logging.info(f"Consumer suscrito al tópico: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"Error en mensaje: {msg.error()}")
                break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Raw Kafka mensaje: {msg.value()}")
                was_inserted = insert_crime(data)
                if was_inserted:
                    consumer.commit(asynchronous=False)
                    
            except json.JSONDecodeError as e:
                logging.warning(f"Error en JSON: {e} | Mensaje: {msg.value()}")
            except Exception as e:
                logging.error(f"Error general al procesar mensaje: {e}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Consumer detenido por el usuario.")
    finally:
        consumer.close()
        logging.info("Consumer cerrado.")

@app.route("/health")
def health_check():
    return "ok", 200

def main():
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()
