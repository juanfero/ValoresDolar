import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Importamos tus lambdas
import app.main as main
import app.processor as processor


# -------------------------------
# PRUEBAS PARA main.handler
# -------------------------------

@patch("app.main.s3")
@patch("app.main.requests.get")
def test_main_handler_success(mock_get, mock_s3):
    # Simulamos respuesta de requests
    fake_data = {"resultados": [{"fecha": "2025-09-09", "valor": "4200"}]}
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = fake_data

    # Simulamos subida a S3
    mock_s3.put_object.return_value = {}

    result = main.handler({}, {})

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "Archivo" in body["message"]

    # Validar que se llamó a S3 con JSON
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["ContentType"] == "application/json"
    assert json.loads(kwargs["Body"]) == fake_data


@patch("app.main.requests.get")
def test_main_handler_error(mock_get):
    # Simulamos error en requests
    mock_get.side_effect = Exception("falló conexión")

    result = main.handler({}, {})
    assert result["statusCode"] == 500
    body = json.loads(result["body"])
    assert "error" in body


# -------------------------------
# PRUEBAS PARA processor.handler
# -------------------------------

@patch("app.processor.s3")
@patch("app.processor.pymysql.connect")
def test_processor_handler_inserts(mock_connect, mock_s3):
    # Simulamos archivo en S3 con lista de listas [timestamp, valor]
    fake_data = [
        [str(int(datetime(2025, 9, 9, 12, 0).timestamp() * 1000)), "4200.55"]
    ]
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(fake_data).encode("utf-8"))
    }

    # Simulamos conexión MySQL
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    event = {
        "Records": [
            {"s3": {"bucket": {"name": "fake"}, "object": {"key": "fake.json"}}}
        ]
    }
    
    print("hola")

    result = processor.handler(event, {})

    assert result["statusCode"] == 200
    assert "insertados" in result["body"]

    # Validar que ejecutó un insert
    mock_cursor.execute.assert_called()
    sql, params = mock_cursor.execute.call_args[0]
    assert "INSERT INTO dolar" in sql
    assert isinstance(params[0], datetime)  # fechahora
    assert isinstance(params[1], float)     # valor
