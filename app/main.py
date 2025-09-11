import json
import boto3
import requests
from datetime import datetime, timezone

s3 = boto3.client("s3")
BUCKET_NAME = "dolarrawjuan2025"  

def handler(event, context):
    """
    Lambda que descarga los valores del dólar desde el Banco de la República
    y los guarda en un archivo JSON en S3.
    """
    url = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # timestamp para el archivo
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        filename = f"dolar-{timestamp}.json"
        
        # subir a S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Archivo {filename} 2 {BUCKET_NAME}"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
if __name__ == "__main__":
    print(handler({}, {}))
