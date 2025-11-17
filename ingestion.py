import json
import logging
import datetime
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Union
import requests
import random
import string

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, udf, base64 as b64
from pyspark.sql.types import BinaryType

import cipher


def make_spark(app_name: str = "AvroToKafkaBatch") -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def _read_text(path: str) -> str:
    if path.startswith(("dbfs:/", "abfss://", "wasbs://", "adl:/", "s3://")):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(SparkSession.getActiveSession())
            return dbutils.fs.head(path, 10_000_000)
        except Exception:
            pass
    with open(path, "r") as f:
        return f.read()


def _load_schema(
    schema_ref: str,
    schema_registry_base: Optional[str] = "http://registryservice/messagecontracts/registry/api/v1/schema/",
) -> Dict[str, Any]:
    s = (schema_ref or "").strip()
    if s.startswith("{"):
        return json.loads(s)
    if s.startswith(("http://", "https://")):
        r = requests.get(s, timeout=30)
        r.raise_for_status()
        return json.loads(r.text)
    if s.endswith(".avsc") or "/" in s or "\\" in s:
        return json.loads(_read_text(s))
    if schema_registry_base:
        url = schema_registry_base.rstrip("/") + "/" + s
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return json.loads(r.text)
    raise ValueError("Unable to resolve schema_ref")


def _df_from_source(spark: SparkSession, data_source: Union[str, DataFrame]) -> DataFrame:
    return data_source if isinstance(data_source, DataFrame) else spark.read.parquet(data_source)


def _gen():
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))


def _row_to_avro_bytes(
    schema: Dict[str, Any],
    field_names: List[str],
    row_dict: Dict[str, Any],
) -> bytes:
    record = {name: row_dict.get(name) for name in field_names}
    buf = BytesIO()
    key = cipher.create_key(_gen())

    kafka_key_fields = schema.get("am_kafka_key", [])
    target_id_field = kafka_key_fields[0] if kafka_key_fields else None

    amk_fields: List[str] = []
    for f in schema.get("fields", []):
        a = f.get("am_platform_id_key")
        if a:
            if isinstance(a, list):
                amk_fields.extend(a)
            else:
                amk_fields.append(a)
    amk_fields = list(dict.fromkeys(amk_fields))

    record["amk_platform_id_fields"] = [record.get(f) for f in amk_fields]

    if target_id_field:
        if amk_fields:
            s = "=".join(str(record.get(f, "")) for f in amk_fields)
            record[target_id_field] = cipher.encrypt(s, key)
        else:
            record[target_id_field] = None

    for f in schema.get("fields", []):
        if f.get("logicalType") == "timestamp-millis":
            record[f["name"]] = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

    from fastavro import schemaless_writer

    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def _payload_df_from_df(
    df: DataFrame,
    schema: Dict[str, Any],
    field_names: List[str],
) -> DataFrame:
    spark = df.sparkSession
    bc_schema = spark.sparkContext.broadcast(schema)
    bc_fields = spark.sparkContext.broadcast(field_names)

    def enc(r):
        return _row_to_avro_bytes(bc_schema.value, bc_fields.value, r.asDict(True))

    return df.select(udf(enc, BinaryType())(struct(*[col(c) for c in df.columns])).alias("value"))


def write_data_as_avro(
    spark: SparkSession,
    data_source: Union[str, DataFrame],
    avro_schema: str,
    target: str,
    output_path: Optional[str] = None,
    coalesce_partitions: int = 1,
    file_mode: str = "overwrite",
    file_format: str = "base64_text",
    schema_registry_base: Optional[str] = "http://registryservice/messagecontracts/registry/api/v1/schema/",
    eventhub_connection_str: Optional[str] = None,
    eventhub_name: Optional[str] = None,
    batch_size: int = 500,
    partition_key_col: Optional[str] = None,
    kafka_bootstrap_servers: Optional[str] = None,
    kafka_topic: Optional[str] = None,
    kafka_security_protocol: str = "PLAINTEXT",
    kafka_sasl_mechanism: Optional[str] = None,
    kafka_sasl_username: Optional[str] = None,
    kafka_sasl_password: Optional[str] = None,
) -> None:
    df = _df_from_source(spark, data_source)
    if coalesce_partitions > 0:
        df = df.coalesce(coalesce_partitions)

    schema = _load_schema(avro_schema, schema_registry_base)
    field_names = [f["name"] for f in schema["fields"]]
    payload_df = _payload_df_from_df(df, schema, field_names)

    if target.lower() == "file":
        if file_format == "base64_text":
            payload_df.select(b64("value").alias("value")).write.mode(file_mode).text(output_path)
        elif file_format == "parquet_bytes":
            payload_df.write.format("parquet").mode(file_mode).save(output_path)
        elif file_format == "avro_ocf_bytes_field":
            bc = json.dumps({"type": "record", "name": "Payload", "fields": [{"name": "value", "type": "bytes"}]})
            payload_df.write.format("avro").option("avroSchema", bc).mode(file_mode).save(output_path)
        return

    if target.lower() == "eventhub":
        from azure.eventhub import EventData, EventHubProducerClient
        bc_schema = spark.sparkContext.broadcast(schema)
        bc_fields = spark.sparkContext.broadcast(field_names)
        bc_conn = spark.sparkContext.broadcast(eventhub_connection_str)
        bc_name = spark.sparkContext.broadcast(eventhub_name)
        bc_pkcol = spark.sparkContext.broadcast(partition_key_col)
        bc_bsize = spark.sparkContext.broadcast(batch_size)

        def send_partition(it):
            rows = list(it)
            if not rows:
                return
            producer = EventHubProducerClient.from_connection_string(bc_conn.value, eventhub_name=bc_name.value)
            batch = []
            cur = None

            def flush():
                nonlocal batch, cur
                if batch:
                    if cur is not None:
                        producer.send_batch(batch, partition_key=cur)
                    else:
                        producer.send_batch(batch)
                    batch = []
                    cur = None

            try:
                for r in rows:
                    d = r.asDict(True)
                    p = _row_to_avro_bytes(bc_schema.value, bc_fields.value, d)
                    ed = EventData(p)
                    k = d.get(bc_pkcol.value) if bc_pkcol.value else None
                    k = str(k) if k else None
                    if bc_pkcol.value and cur is not None and k != cur:
                        flush()
                    cur = k
                    batch.append(ed)
                    if len(batch) >= bc_bsize.value:
                        flush()
                flush()
            finally:
                producer.close()

        df.foreachPartition(send_partition)
        return

    if target.lower() == "kafka":
        from confluent_kafka import Producer
        if not kafka_topic:
            raise ValueError("kafka_topic must be passed via CLI")

        bc_schema = spark.sparkContext.broadcast(schema)
        bc_fields = spark.sparkContext.broadcast(field_names)
        bc_boot = spark.sparkContext.broadcast(kafka_bootstrap_servers)
        bc_topic = spark.sparkContext.broadcast(kafka_topic)
        bc_sec = spark.sparkContext.broadcast(kafka_security_protocol)
        bc_mech = spark.sparkContext.broadcast(kafka_sasl_mechanism)
        bc_user = spark.sparkContext.broadcast(kafka_sasl_username)
        bc_pass = spark.sparkContext.broadcast(kafka_sasl_password)
        bc_pk = spark.sparkContext.broadcast(partition_key_col)

        def send_partition(it):
            conf = {"bootstrap.servers": bc_boot.value}
            if bc_sec.value != "PLAINTEXT":
                conf["security.protocol"] = bc_sec.value
            if bc_mech.value:
                conf["sasl.mechanism"] = bc_mech.value
            if bc_user.value:
                conf["sasl.username"] = bc_user.value
            if bc_pass.value:
                conf["sasl.password"] = bc_pass.value

            p = Producer(conf)

            def cb(e, m):
                if e:
                    logging.error(str(e))

            for r in it:
                d = r.asDict(True)
                v = _row_to_avro_bytes(bc_schema.value, bc_fields.value, d)
                k = d.get(bc_pk.value) if bc_pk.value else None
                kb = str(k).encode() if k is not None else None
                p.produce(topic=bc_topic.value, value=v, key=kb, on_delivery=cb)
                p.poll(0)
            p.flush()

        df.foreachPartition(send_partition)
        return

    raise ValueError("target must be one of: file | eventhub | kafka")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-source", required=True)
    p.add_argument("--avro-schema", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--output-path")
    p.add_argument("--coalesce-partitions", type=int, default=1)
    p.add_argument("--file-mode", default="overwrite")
    p.add_argument("--file-format", default="base64_text")
    p.add_argument("--schema-registry-base")
    p.add_argument("--eventhub-connection-str")
    p.add_argument("--eventhub-name")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--partition-key-col")
    p.add_argument("--kafka-bootstrap-servers")
    p.add_argument("--kafka-topic")
    p.add_argument("--kafka-security-protocol", default="PLAINTEXT")
    p.add_argument("--kafka-sasl-mechanism")
    p.add_argument("--kafka-sasl-username")
    p.add_argument("--kafka-sasl-password")

    a = p.parse_args()
    spark = make_spark()

    write_data_as_avro(
        spark=spark,
        data_source=a.data_source,
        avro_schema=a.avro_schema,
        target=a.target,
        output_path=a.output_path,
        coalesce_partitions=a.coalesce_partitions,
        file_mode=a.file_mode,
        file_format=a.file_format,
        schema_registry_base=a.schema_registry_base,
        eventhub_connection_str=a.eventhub_connection_str,
        eventhub_name=a.eventhub_name,
        batch_size=a.batch_size,
        partition_key_col=a.partition_key_col,
        kafka_bootstrap_servers=a.kafka_bootstrap_servers,
        kafka_topic=a.kafka_topic,
        kafka_security_protocol=a.kafka_security_protocol,
        kafka_sasl_mechanism=a.kafka_sasl_mechanism,
        kafka_sasl_username=a.kafka_sasl_username,
        kafka_sasl_password=a.kafka_sasl_password,
    )
