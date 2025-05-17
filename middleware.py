from fastapi import FastAPI, Query
from typing import Optional, Literal
import mysql.connector
from datetime import datetime
from datetime import timedelta

app = FastAPI()

DB_CONFIG = {
    "mochis_guadalajara": {
        "host": "192.168.117.191",
        "user": "matriz",
        "password": "123456789",
        "database": "asteriskcdrdb",
    },
    "monterrey": {
        "host": "192.168.117.195",
        "user": "sucursal",
        "password": "123456789",
        "database": "asteriskcdrdb",
    }
}

def identificar_sucursal(src, dst, did):
    if src.startswith("v") or dst.startswith("v"):
        return "desconocido"
    elif src.startswith("1") or src.startswith("6875938676"):
        return "mochis"
    elif src.startswith("2") or src.startswith("6682278237"):
        return "guadalajara"
    elif src.startswith("3")  or src.startswith("6682496565"):
        return "monterrey"
    return "desconocido"

def get_calls_from_db(config, filtros, origen_sucursal):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT calldate, src, dst, disposition, duration, did 
            FROM cdr
            WHERE 1=1 
        """
        params = []

        if filtros.get("origen"):
            query += " AND src = %s"
            params.append(filtros["origen"])

        if filtros.get("destino"):
            query += " AND dst = %s"
            params.append(filtros["destino"])

        if filtros.get("estatus"):
            query += " AND disposition = %s"
            params.append(filtros["estatus"])

        if filtros.get("fecha_desde"):
            query += " AND calldate >= %s"
            params.append(filtros["fecha_desde"])

        if filtros.get("fecha_hasta"):
            fecha_hasta_exclusiva = filtros["fecha_hasta"] + timedelta(days=1)
            query += " AND calldate < %s"
            params.append(fecha_hasta_exclusiva)

        query += " ORDER BY calldate DESC LIMIT 100"
        cursor.execute(query, params)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            sucursal_detectada = identificar_sucursal(row["src"], row["dst"], row.get("did", ""))
            if origen_sucursal == "todas" or sucursal_detectada == origen_sucursal:
                row["sucursal"] = sucursal_detectada

                #row["calldate"] = row["calldate"].strftime("%Y-%m-%d")

                results.append(row)

        cursor.close()
        conn.close()
        return results
    except Exception as e:
        return {"error": str(e)}

@app.get("/calls")
def get_calls(
    sucursal: Optional[Literal["mochis", "guadalajara", "monterrey", "todas"]] = Query("todas"),
    origen: Optional[str] = None,
    destino: Optional[str] = None,
    estatus: Optional[str] = None,
    fecha_desde: Optional[datetime] = None,
    fecha_hasta: Optional[datetime] = None
):
    filtros = {
        "origen": origen,
        "destino": destino,
        "estatus": estatus,
        "fecha_desde": fecha_desde,
        "fecha_hasta": fecha_hasta
    }
    data = {}
    if sucursal in ["mochis", "guadalajara", "todas"]:
        data["mochis_guadalajara"] = get_calls_from_db(DB_CONFIG["mochis_guadalajara"], filtros, sucursal)
    if sucursal in ["monterrey", "todas"]:
        data["monterrey"] = get_calls_from_db(DB_CONFIG["monterrey"], filtros, sucursal)
    return data
