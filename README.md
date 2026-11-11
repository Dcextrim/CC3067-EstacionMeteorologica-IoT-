# IoT: Estación Meteorológica (CC3067 Redes)

Simulación de un nodo de sensores (temperatura, humedad y dirección del viento) que publica su telemetría en un broker Apache Kafka (Edge Server `161.35.11.141:9092`), y un consumidor que grafica los datos en tiempo real.

## Archivos

| Archivo | Qué hace |
|---|---|
| `codec.py` | Configuración del servidor, generación de datos (normal con media 55) y `encode`/`decode` del payload de 3 bytes |
| `producer.py` | Dispositivo IoT: envía una lectura cada 15–30 s hasta que se interrumpe (Ctrl+C) |
| `consumer.py` | Se suscribe al topic y actualiza las gráficas con cada mensaje nuevo |
| `capturas/` | Capturas de las gráficas generadas con la data consumida |

## Uso

```
pip install kafka-python matplotlib
python producer.py 231177            # JSON
python consumer.py 231177

python producer.py 231177-3b --3bytes   # payload restringido a 3 bytes
python consumer.py 231177-3b --3bytes
```

Prueba del codificador: `python codec.py`

## Payload de 3 bytes (24 bits)

| Campo | Bits | Codificación |
|---|---|---|
| Temperatura | 14 | `temperatura * 100` (0–11000 < 2^14 = 16384) |
| Humedad | 7 | entero 0–100 |
| Dirección del viento | 3 | índice en `[N, NO, O, SO, S, SE, E, NE]` |
