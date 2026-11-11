"""Nodo IoT (Producer). Uso: python producer.py <carne> [--3bytes]"""
import random
import sys
import time

from kafka import KafkaProducer

from codec import AUTH, SERVER, encode, generar_data, to_json_bytes

topic = sys.argv[1]  # el carne se usa como topic
compacto = "--3bytes" in sys.argv  # con --3bytes se envia el payload comprimido
serializar = encode if compacto else to_json_bytes  # funcion que convierte los datos a bytes

# Conexion al broker Kafka con autenticacion
producer = KafkaProducer(bootstrap_servers=SERVER, **AUTH)
try:
    while True:  # corre hasta que se presione Ctrl+C
        data = generar_data()  # lectura simulada de los sensores
        payload = serializar(data)
        producer.send(topic, key=b"sensor1", value=payload)  # publica el mensaje en el topic
        producer.flush()  # espera a que el mensaje se envie de verdad
        print(f"Enviado {data} -> {payload!r} ({len(payload)} bytes)", flush=True)
        time.sleep(random.uniform(15, 30))  # espera entre 15 y 30 segundos
except KeyboardInterrupt:
    print("Detenido")
finally:
    producer.close()  # cierra la conexion
