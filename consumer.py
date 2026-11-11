"""Consumer con grafica en vivo. Uso: python consumer.py <carne> [--3bytes]"""
import sys

import matplotlib.pyplot as plt
from kafka import KafkaConsumer

from codec import AUTH, DIRECCIONES, SERVER, decode, from_json_bytes

topic = sys.argv[1]  # el carne se usa como topic
compacto = "--3bytes" in sys.argv  # con --3bytes se decodifica el payload comprimido
deserializar = decode if compacto else from_json_bytes  # funcion que convierte bytes a datos

# Se suscribe al topic y lee todos los mensajes desde el primero
consumer = KafkaConsumer(topic, bootstrap_servers=SERVER, group_id=f"{topic}-graficas",
                         auto_offset_reset="earliest", enable_auto_commit=False, **AUTH)

# Historial de lecturas recibidas
all_temp, all_hume, all_wind = [], [], []

plt.ion()  # modo interactivo: la ventana se actualiza sin bloquear el programa
fig, (ax_t, ax_h, ax_w) = plt.subplots(3, 1, figsize=(11, 8), sharex=True)  # 3 graficas
titulo = "Estacion meteorologica - topic " + topic + (" (payload 3 bytes)" if compacto else " (JSON)")
fig.suptitle(titulo)


def graficar():
    """Vuelve a dibujar las tres graficas con todos los datos recibidos."""
    x = range(1, len(all_temp) + 1)  # numero de lectura en el eje X
    for ax in (ax_t, ax_h, ax_w):
        ax.clear()
        ax.grid(True, alpha=0.3)
    # Temperatura
    ax_t.plot(x, all_temp, "o-", color="tab:red")
    ax_t.set_ylabel("Temperatura (C)")
    ax_t.set_ylim(0, 110)
    # Humedad
    ax_h.plot(x, all_hume, "o-", color="tab:blue")
    ax_h.set_ylabel("Humedad (%)")
    ax_h.set_ylim(0, 100)
    # Viento: cada direccion se muestra con su nombre en el eje Y
    ax_w.plot(x, [DIRECCIONES.index(w) for w in all_wind], "o", color="tab:green")
    ax_w.set_yticks(range(len(DIRECCIONES)), DIRECCIONES)
    ax_w.set_ylabel("Viento")
    ax_w.set_xlabel("Lectura #")
    fig.canvas.draw_idle()
    plt.pause(0.1)  # le da tiempo a la ventana para refrescarse


graficar()
while plt.fignum_exists(fig.number):  # sigue mientras la ventana este abierta
    # Revisa si llegaron mensajes nuevos
    for msgs in consumer.poll(timeout_ms=500).values():
        for m in msgs:
            try:
                payload = deserializar(m.value)  # bytes -> diccionario
            except ValueError:  # si el mensaje no tiene el formato esperado, no se toma en cuenta
                print(f"offset={m.offset} ignorado: {m.value!r}", flush=True)
                continue
            print(f"offset={m.offset} key={m.key} raw={m.value!r} -> {payload}", flush=True)
            # Guarda la nueva lectura en el historial
            all_temp.append(payload["temperatura"])
            all_hume.append(payload["humedad"])
            all_wind.append(payload["direccion_viento"])
        graficar()  # actualiza la grafica con el dato nuevo
    plt.pause(0.1)
consumer.close()  # cierra la conexion al cerrar la ventana
