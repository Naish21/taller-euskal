"""Recoge la tabla de valor de las acciones de la web de Expansion y sube los datos a un postgres"""

__version__ = "0.03"

import os
import sys
from copy import deepcopy
from datetime import datetime

import pandas as pd
import psycopg


def limpia_tabla_acciones(datf: pd.DataFrame) -> pd.DataFrame:
    """Limpia el dataframe de la tabla de acciones del Ibex35 para que los datos sean coherentes"""
    _datf = deepcopy(datf)
    _datf.columns = [
        "ACCION",
        "VALOR",
        "VARIACION",
        "VAR_VALOR",
        "ACUMULADO_ANUAL",
        "MAX",
        "MIN",
        "VOL",
        "CAPIT",
        "FECHA",
        "_",
    ]
    _datf['FECHA'] = datetime.now().strftime('%Y-%m-%d') + ' ' + _datf['FECHA']
    return _datf[
        [
            "ACCION",
            "FECHA",
            "VALOR",
            "VARIACION",
            "VAR_VALOR",
            "ACUMULADO_ANUAL",
            "MAX",
            "MIN",
            "VOL",
            "CAPIT",
        ]
    ]


def test_postgres_connection(conn_str: str) -> list[tuple]:
    """Comprueba la conexión con postgres"""
    # Connect to an existing database
    with psycopg.connect(conninfo=conn_str) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cursor:
            cursor.execute("SELECT CURRENT_TIME;")
            ans = cursor.fetchall()
    return ans


def create_table(conn_str: str):
    """Creamos la tabla en la base de datos"""

    ddl_table = """
        CREATE TABLE public.ibex35 (
            accion varchar NOT NULL
            ,fecha timestamp NOT NULL
            ,valor numeric NOT NULL
            ,variacion numeric NOT NULL
            ,var_valor numeric NOT NULL
            ,acumulado_anual numeric NOT NULL
            ,max numeric NOT NULL
            ,min numeric NOT NULL
            ,vol numeric NOT NULL
            ,capit numeric NOT NULL
        );"""

    with psycopg.connect(conninfo=conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(ddl_table)
        conn.commit()


def insert_data(conn_str: str, table_name: str, data: list[dict]):
    """Insertamos los datos en la base de datos"""
    with psycopg.connect(conninfo=conn_str) as conn:
        with conn.cursor() as cursor:
            for _data_to_insert in data:
                columns = ", ".join(_data_to_insert.keys())
                values = tuple(_data_to_insert.values())
                cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES {values}")
        conn.commit()


if __name__ == "__main__":
    # Coger los datos de la web:
    DEFAULT_URL = (
        "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"
    )
    url = os.environ.get("URL", DEFAULT_URL)
    web = pd.read_html(url, encoding='ISO-8859-1', decimal=',', thousands='.')
    tabla_cotizaciones = None
    for table in web:
        if "Valor" in table.columns and "Var." in table.columns:
            tabla_cotizaciones = table
            break
    if tabla_cotizaciones is None:
        sys.exit(1)
    ibex35 = limpia_tabla_acciones(tabla_cotizaciones)

    # Insertar los datos en la BBDD:
    connection_string = os.environ.get("CONNECTION_STRING")
    try:
        create_table(conn_str=connection_string)
    except psycopg.errors.DuplicateTable:
        pass
    data_to_insert = ibex35.to_dict("records")
    insert_data(conn_str=connection_string, table_name="IBEX35", data=data_to_insert)
