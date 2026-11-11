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

# Las 8 direcciones posibles del viento; su posicion (0-7) cabe en 3 bits
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
    """JSON -> 3 bytes: [temp*100 : 14 bits][humedad : 7 bits][viento : 3 bits].
    temp*100 va de 0 a 11000 < 2^14 = 16384, asi la temperatura entra como entero."""
    t = round(d["temperatura"] * 100)  # 56.32 -> 5632 (quitamos el punto decimal)
    # Juntamos los tres valores en un solo numero de 24 bits corriendo cada uno a su lugar
    n = (t << 10) | (d["humedad"] << 3) | DIRECCIONES.index(d["direccion_viento"])
    return n.to_bytes(3, "big")  # numero de 24 bits -> 3 bytes


def decode(b):
    """3 bytes -> JSON (dict)."""
    if len(b) != 3:
        raise ValueError(f"payload de {len(b)} bytes, se esperaban 3")
    n = int.from_bytes(b, "big")  # 3 bytes -> numero de 24 bits
    # Separamos cada campo con desplazamientos (>>) y mascaras (&)
    return {"temperatura": (n >> 10) / 100,        # 14 bits de arriba, /100 recupera los decimales
            "humedad": (n >> 3) & 0x7F,            # 7 bits del medio (0x7F = 1111111)
            "direccion_viento": DIRECCIONES[n & 0x7]}  # 3 bits de abajo (0x7 = 111)


if __name__ == "__main__":  # auto-prueba: python codec.py
    # Probamos los extremos y 10000 lecturas al azar: decode(encode(x)) debe dar x
    for d in [{"temperatura": 0.0, "humedad": 0, "direccion_viento": "N"},
              {"temperatura": 110.0, "humedad": 100, "direccion_viento": "NE"}] + \
             [generar_data() for _ in range(10000)]:
        b = encode(d)
        assert len(b) == 3 and decode(b) == d, (d, decode(b))
    print("OK: encode/decode de 3 bytes correcto")
