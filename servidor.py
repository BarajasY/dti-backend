import asyncio
import json
import os
import random
from collections import Counter
from datetime import datetime, timezone
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from psycopg.rows import dict_row

# --- Configuración ---
POSTGRES_DSN = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)

# --- Lógica del Servidor FastAPI ---
app = FastAPI()

# --- CORS totalmente abierto ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # <--- cualquier dominio
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Clases ---
class MetricasAgente(BaseModel):
    pc_id: str
    usuario: str
    cpu_uso: float
    memoria_uso: float
    disco_uso: float
    procesos_count: int
    error_mensaje: Optional[str] = None


class PcMonitorRow(BaseModel):
    id: int
    cpu: int
    procesos: int
    ram: int
    discoduro: int
    activo: bool


def run_analisis_estadistico() -> dict:
    """
    Ejecuta los análisis estadísticos sobre historial_errores
    e historial_metricas y devuelve un diccionario listo para
    exponer por API.
    """
    if pool is None:
        raise RuntimeError("El pool de PostgreSQL no está inicializado")

    resultado: dict = {
        "frecuencia": {
            "promedio_errores_por_pc": None,
            "pcs_con_alerta": [],  # [{pc_id, fallos}]
        },
        "promedio_carga": {
            "carga_promedio_laboratorio": None,
            "pcs_sobrecargadas": [],  # [{pc_id, media}]
            "pcs_infrautilizadas": [],  # [{pc_id, media}]
        },
        "fiabilidad": {
            "mtbf_segundos": None,
        },
        "series": {
            "patron_detectado": False,
            "origen": None,
            "destino": None,
            "veces": None,
        },
        "poker_infraestructura": {
            "laboratorios_problema": [],  # [{laboratorio, fallos}]
            "fallos_distribuidos_normalmente": True,
        },
    }

    with pool.connection() as conn:
        # Usamos row_factory=dict_row para obtener dicts
        with conn.cursor(row_factory=dict_row) as cur:
            # 1) PRUEBA DE FRECUENCIA (Limones)
            cur.execute(
                "SELECT pc_id, COUNT(*) AS c FROM historial_errores GROUP BY pc_id"
            )
            datos = cur.fetchall()
            if datos:
                avg = sum(d["c"] for d in datos) / len(datos)
                resultado["frecuencia"]["promedio_errores_por_pc"] = avg
                pcs_alerta = []
                for d in datos:
                    if d["c"] > avg * 1.5:
                        pcs_alerta.append(
                            {
                                "pc_id": d["pc_id"],
                                "fallos": d["c"],
                            }
                        )
                resultado["frecuencia"]["pcs_con_alerta"] = pcs_alerta

            # 2) PRUEBA DE PROMEDIO (Balanceo de Carga)
            cur.execute(
                "SELECT pc_id, AVG(cpu_uso) AS media "
                "FROM historial_metricas "
                "GROUP BY pc_id"
            )
            datos = cur.fetchall()
            if datos:
                global_avg = sum(d["media"] for d in datos) / len(datos)
                resultado["promedio_carga"]["carga_promedio_laboratorio"] = global_avg

                pcs_sobre = []
                pcs_infra = []
                for d in datos:
                    media = d["media"]
                    if media is None:
                        continue
                    if media > 80:
                        pcs_sobre.append({"pc_id": d["pc_id"], "media_cpu": media})
                    elif media < 5:
                        pcs_infra.append({"pc_id": d["pc_id"], "media_cpu": media})

                resultado["promedio_carga"]["pcs_sobrecargadas"] = pcs_sobre
                resultado["promedio_carga"]["pcs_infrautilizadas"] = pcs_infra

            # 3) PRUEBA K-S (Fiabilidad) → MTBF
            cur.execute(
                "SELECT EXTRACT(EPOCH FROM timestamp) AS ts "
                "FROM historial_errores "
                "ORDER BY timestamp ASC"
            )
            tiempos_rows = cur.fetchall()
            tiempos = [r["ts"] for r in tiempos_rows]
            if len(tiempos) > 4:
                gaps = [t2 - t1 for t1, t2 in zip(tiempos, tiempos[1:])]
                if gaps:
                    mtbf = sum(gaps) / len(gaps)
                    resultado["fiabilidad"]["mtbf_segundos"] = mtbf

            # 4) PRUEBA DE SERIES (Causa-Efecto)
            cur.execute(
                "SELECT error_mensaje FROM historial_errores ORDER BY timestamp ASC"
            )
            errores_rows = cur.fetchall()
            errores = [r["error_mensaje"] for r in errores_rows]
            if len(errores) > 4:
                pares = list(zip(errores, errores[1:]))
                comunes = Counter(pares).most_common(1)
                if comunes and comunes[0][1] > 2:
                    (origen, destino), veces = comunes[0]
                    resultado["series"] = {
                        "patron_detectado": True,
                        "origen": origen,
                        "destino": destino,
                        "veces": veces,
                    }

            # 5) PRUEBA DE PÓQUER (Infraestructura)
            cur.execute("SELECT pc_id FROM historial_errores")
            fallos_rows = cur.fetchall()
            fallos = [
                (r["pc_id"].split("-")[1] if "-" in r["pc_id"] else "OTRO")
                for r in fallos_rows
            ]
            conteo = Counter(fallos)
            labs_problema = []
            for lab, cant in conteo.items():
                if cant > 5:
                    labs_problema.append({"laboratorio": lab, "fallos": cant})

            if labs_problema:
                resultado["poker_infraestructura"]["laboratorios_problema"] = (
                    labs_problema
                )
                resultado["poker_infraestructura"][
                    "fallos_distribuidos_normalmente"
                ] = False

    return resultado


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket:
    - Ejecuta fn_actualizar_monitoreo_pc() en PostgreSQL
    - Lee pcmonitor
    - Guarda historial
    - Envía estado al cliente
    """
    await websocket.accept()
    print(f"Panel conectado: {websocket.client}")

    try:
        while True:
            def ejecutar_actualizacion():
                if pool is None:
                    raise RuntimeError("Pool no inicializado")

                with pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT fn_actualizar_monitoreo_pc();")
                    conn.commit()

            await asyncio.to_thread(ejecutar_actualizacion)

            rows = await asyncio.to_thread(fetch_pcmonitor_from_pg)

            await asyncio.to_thread(guardar_historial_pcmonitor, rows)

            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).timestamp()

            estado_actual: dict[str, dict] = {}

            for row in rows:
                pc_id = f"PC-{row.id}"

                estado_actual[pc_id] = {
                    "estado": "Online" if row.activo else "Offline",
                    "usuario": "N/A",
                    "cpu_uso": float(row.cpu),
                    "memoria_uso": float(row.ram),
                    "disco_uso": float(row.discoduro),
                    "procesos_count": int(row.procesos),
                    "error_mensaje": None,
                    "timestamp": ts
                }

            await websocket.send_text(json.dumps(estado_actual))

            await asyncio.sleep(10)

    except WebSocketDisconnect:
        print(f"Panel desconectado: {websocket.client}")


def guardar_historial_pcmonitor(rows: list[PcMonitorRow]) -> None:
    """
    Inserta un snapshot de métricas en historial_metricas y,
    de vez en cuando, genera errores sintéticos en historial_errores.
    """
    if pool is None:
        raise RuntimeError("El pool de PostgreSQL no está inicializado")

    ahora = datetime.now(timezone.utc)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                pc_id = f"PC-{row.id}"  # ajusta si tu pc_id real es distinto

                # 1) Insertar snapshot de métricas
                cur.execute(
                    """
                    INSERT INTO historial_metricas
                        (pc_id, cpu_uso, memoria_uso, disco_uso, procesos_count, timestamp)
                    VALUES
                        (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        pc_id,
                        float(row.cpu),
                        float(row.ram),
                        float(row.discoduro),
                        int(row.procesos),
                        ahora,
                    ),
                )

                # 2) Generar errores sintéticos según algún criterio
                #    Puedes tunear este algoritmo a tu gusto

                # a) Error por sobrecarga
                if row.cpu > 95 or row.ram > 95:
                    cur.execute(
                        """
                        INSERT INTO historial_errores (pc_id, error_mensaje, timestamp)
                        VALUES (%s, %s, %s)
                        """,
                        (
                            pc_id,
                            "Sobrecalentamiento / sobrecarga de recursos",
                            ahora,
                        ),
                    )

                # b) Error aleatorio con probabilidad baja (~2%)
                elif random.random() < 0.02:
                    mensaje = random.choice(
                        [
                            "Fallo en disco",
                            "Pérdida de conexión a la red",
                            "Aplicación crítica se cerró inesperadamente",
                            "Timeout de respuesta del sistema",
                        ]
                    )
                    cur.execute(
                        """
                        INSERT INTO historial_errores (pc_id, error_mensaje, timestamp)
                        VALUES (%s, %s, %s)
                        """,
                        (pc_id, mensaje, ahora),
                    )

        conn.commit()


@app.get("/api/analisis/", tags=["analitica"])
async def obtener_analisis():
    """
    Ejecuta los análisis estadísticos sobre el historial
    y devuelve un reporte en formato JSON.
    """
    data = await asyncio.to_thread(run_analisis_estadistico)
    return data


@app.on_event("startup")
async def startup_event():
    """Al iniciar el servidor, configurar y cargar la DB."""
    global pool

    pool = ConnectionPool(
        conninfo=POSTGRES_DSN,
        min_size=1,
        max_size=5,
        max_idle=30,
        num_workers=1,
    )


def fetch_pcmonitor_from_pg() -> list[PcMonitorRow]:
    """
    Lee todas las filas de la tabla pcmonitor en PostgreSQL usando el pool.
    Convierte 'activo' (bit) a booleano.
    """

    if pool is None:
        raise RuntimeError("El pool de PostgreSQL no está inicializado")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    cpu,
                    procesos,
                    ram,
                    discoduro,
                    (activo = B'1') AS activo  -- bit(1) -> bool
                FROM pcmonitor
                ORDER BY id;
            """)
            rows = cur.fetchall()

    resultado: list[PcMonitorRow] = []
    for row in rows:
        resultado.append(
            PcMonitorRow(
                id=row[0],
                cpu=row[1],
                procesos=row[2],
                ram=row[3],
                discoduro=row[4],
                activo=row[5],
            )
        )
    return resultado


@app.get("/api/pcmonitor", response_model=List[PcMonitorRow])
def get_pcmonitor():
    """
    Devuelve el estado actual de la tabla pcmonitor en PostgreSQL.
    """
    rows = fetch_pcmonitor_from_pg()
    return rows


@app.on_event("shutdown")
async def shutdown_event():
    global pool
    if pool is not None:
        pool.close()
        pool = None


# if __name__ == "__main__":
#     print("Iniciando servidor en http://127.0.0.1:8000", flush=True)
#     uvicorn.run(app, host="127.0.0.1", port=8000)
