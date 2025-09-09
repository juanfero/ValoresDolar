import json
import boto3
import pymysql
from datetime import datetime

# Parámetros de conexión a RDS
RDS_HOST = "dolar-db.ctkee2qq2l2z.us-east-1.rds.amazonaws.com"
DB_USER = "admin"
DB_PASS = "Dolar2025$"
DB_NAME = "dolardb"


def handler(event, context):
    """
    Lambda que se activa cuando un archivo llega a S3.
    Procesa el JSON (lista de listas [timestamp, valor]) y guarda en RDS.
    """
    s3 = boto3.client("s3")

    # Obtener info del evento de S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Descargar archivo desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)

    print("DEBUG JSON recibido:", str(data)[:500])  # muestra solo inicio

    # Validar que es lista de listas
    if not isinstance(data, list):
        print("Formato inesperado:", type(data))
        return {"statusCode": 400, "body": "Formato inesperado"}

    # Conexión a RDS
    conn = pymysql.connect(
        host=RDS_HOST,
        user=DB_USER,
        passwd=DB_PASS,
        db=DB_NAME
    )

    inserted = 0
    with conn.cursor() as cursor:
        for fila in data:
            try:
                ts_millis = int(fila[0])  # timestamp en ms
                valor = float(fila[1])

                # convertir a datetime
                fechahora = datetime.utcfromtimestamp(ts_millis / 1000.0)

                sql = """
                INSERT INTO dolar (fechahora, valor)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE valor = VALUES(valor)
                """
                cursor.execute(sql, (fechahora, valor))
                inserted += 1
            except Exception as e:
                print("Error procesando fila:", fila, "Error:", e)

        conn.commit()
    conn.close()

    print(f"Se insertaron/actualizaron {inserted} filas.")

    return {
        "statusCode": 200,
        "body": f"{inserted} registros insertados/actualizados en RDS"
    }
