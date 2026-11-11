"""Configuracion comun, generacion de datos y Encode/Decode de 3 bytes."""
import json
import random

# Direccion del servidor Kafka (Edge Server) y credenciales de autenticacion
SERVER = "161.35.11.141:9092"
AUTH = dict(
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="estudiante",
    sasl_plain_password="TryluRandomPass-2!@!",
)

# Las 8 direcciones posibles del viento
DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]


def generar_data():
    """Simula una lectura de los tres sensores."""
    # Normal con media 55 y desviacion 15, recortada al rango del sensor
    temp = min(max(random.gauss(55, 15), 0.0), 110.0)
    hum = min(max(round(random.gauss(55, 15)), 0), 100)
    return {"temperatura": round(temp, 2), "humedad": hum,
            "direccion_viento": random.choice(DIRECCIONES)}


def to_json_bytes(d):
    """Diccionario -> texto JSON en bytes (para enviarlo a Kafka)."""
    return json.dumps(d).encode()


def from_json_bytes(b):
    """Bytes recibidos de Kafka -> diccionario."""
    return json.loads(b.decode())


def encode(d):
    """Convierte la lectura en 3 bytes.
    Temperatura usa 14 bits, humedad 7 bits y viento 3 bits."""
    t = round(d["temperatura"] * 100)  # 56.32 -> 5632, maximo 11000 que cabe en 14 bits
    # Junta los tres valores en un solo numero de 24 bits
    n = (t << 10) | (d["humedad"] << 3) | DIRECCIONES.index(d["direccion_viento"])
    return n.to_bytes(3, "big")  # numero de 24 bits -> 3 bytes


def decode(b):
    """Convierte 3 bytes de vuelta en la lectura original."""
    if len(b) != 3:
        raise ValueError(f"payload de {len(b)} bytes, se esperaban 3")
    n = int.from_bytes(b, "big")  # 3 bytes -> numero de 24 bits
    # Separa cada valor tomando sus bits
    return {"temperatura": (n >> 10) / 100,        # primeros 14 bits, entre 100 para los decimales
            "humedad": (n >> 3) & 0x7F,            # siguientes 7 bits
            "direccion_viento": DIRECCIONES[n & 0x7]}  # ultimos 3 bits


if __name__ == "__main__":  # auto-prueba: python codec.py
    # Probamos los extremos y 10000 lecturas al azar: decode(encode(x)) debe dar x
    for d in [{"temperatura": 0.0, "humedad": 0, "direccion_viento": "N"},
              {"temperatura": 110.0, "humedad": 100, "direccion_viento": "NE"}] + \
             [generar_data() for _ in range(10000)]:
        b = encode(d)
        assert len(b) == 3 and decode(b) == d, (d, decode(b))
    print("OK: encode/decode de 3 bytes correcto")
