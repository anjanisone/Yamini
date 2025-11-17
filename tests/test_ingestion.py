import json
import pytest
from pyspark.sql import SparkSession, Row
from unittest.mock import patch

import ingestion
import cipher


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_ingestion")
        .getOrCreate()
    )


@pytest.fixture
def schema():
    return {
        "type": "record",
        "name": "Securities",
        "am_kafka_key": ["SecurityId"],
        "fields": [
            {"name": "SourceSystem", "type": ["null", "string"]},
            {"name": "Cusip", "type": ["null", "string"], "am_platform_id_key": "Cusip"},
            {"name": "Ticker", "type": ["null", "string"]},
            {"name": "Name", "type": ["null", "string"]},
            {"name": "AMAssetCode", "type": ["null", "string"]},
            {"name": "AssetClass", "type": ["null", "string"]},
            {"name": "AssetType", "type": ["null", "string"]},
            {"name": "SubAssetType", "type": ["null", "string"]},
            {"name": "UpdateTimestamp", "type": "long", "logicalType": "timestamp-millis"},
        ],
    }


def test_row_to_avro_bytes(schema):
    row = {
        "SourceSystem": "APL",
        "Cusip": "037833100",
        "Ticker": "AAPL",
        "Name": "Apple",
        "AMAssetCode": "5543",
        "AssetClass": "11000",
        "AssetType": "28",
        "SubAssetType": "11010",
    }

    with patch.object(cipher, "encrypt", return_value=b"X"):
        out = ingestion._row_to_avro_bytes(schema, [f["name"] for f in schema["fields"]], row)

    assert isinstance(out, bytes)
    assert len(out) > 0


def test_payload_df(spark, schema):
    df = spark.createDataFrame([
        Row(
            SourceSystem="APL",
            Cusip="037833100",
            Ticker="AAPL",
            Name="Apple",
            AMAssetCode="5543",
            AssetClass="11000",
            AssetType="28",
            SubAssetType="11010",
        )
    ])

    with patch.object(cipher, "encrypt", return_value=b"Y"):
        out = ingestion._payload_df_from_df(df, schema, [f["name"] for f in schema["fields"]])

    r = out.collect()[0]["value"]
    assert isinstance(r, bytes)
    assert len(r) > 0


def test_file_write(spark, tmp_path, schema):
    df = spark.createDataFrame([
        Row(
            SourceSystem="APL",
            Cusip="037833100",
            Ticker="AAPL",
            Name="Apple",
            AMAssetCode="5543",
            AssetClass="11000",
            AssetType="28",
            SubAssetType="11010",
        )
    ])

    op = str(tmp_path / "x")

    with patch.object(ingestion, "_df_from_source", return_value=df), \
         patch.object(cipher, "encrypt", return_value=b"Z"):
        ingestion.write_data_as_avro(
            spark=spark,
            data_source="d",
            avro_schema=json.dumps(schema),
            target="file",
            output_path=op,
            file_format="base64_text"
        )

    files = list(tmp_path.joinpath("x").glob("*"))
    assert len(files) > 0
