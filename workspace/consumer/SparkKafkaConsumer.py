

from datetime import datetime
import socket


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json 
from urllib.parse import urlparse, urlunparse
from schemas.pyspark_schemas import ENDPOINT_SCHEMAS,schema_yfinance
from pyspark.sql.types import StructType, StructField, StringType


class BaseKafkaConsumer():
    """
    Base class for Kafka streaming consumers using Spark.
    """
    #, schema
    def __init__(self, bucket, kafka_bootstrap_servers, topic, formats, storage_access: dict):
        """
        Set Kafka and output path configuration.
        """
        self.topic = topic  
        #self.schema = schema
        self.bucket = bucket      
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.formats = formats
        self.storage_access = storage_access

        # Automatically resolve the correct endpoint (MinIO or S3)
        self.endpoint = self._get_storage_endpoint()

        self.spark = self._create_spark_session()


    def _create_spark_session(self):
        """
        Create Spark session configured for S3 or MinIO.
        """
        is_minio = self.storage_access.get("use_minio", False) 
        # Use path-style access for MinIO (true) and disable SSL; use virtual-hosted style and enable SSL for AWS S3
        spark = SparkSession.builder \
            .appName("KafkaToDatalake") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.hadoop.fs.s3a.access.key", self.storage_access['access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", self.storage_access['secret_key']) \
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", str(is_minio).lower()) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(not is_minio).lower()) \
            .getOrCreate()


        #hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration() 
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    def stream_kafka_to_storage(self):
        """
        Execute batch read from Kafka and write to datalake.
        """
        df_or_list = self._parse_kafka_batch()

# Check if the result is a list (endpoint-based) or a single DataFrame
        if isinstance(df_or_list, list):
            parsed_dfs = df_or_list
            if not parsed_dfs or all(df.head(1) == [] for _, df in parsed_dfs):
                print("⚠️ No data received from Kafka — exiting.")
                self.spark.stop()
                return
        else:
            parsed_dfs = [("ohlcv", df_or_list)]
            if df_or_list.head(1) == []:
                print("⚠️ No data received from Kafka — exiting.")
                self.spark.stop()
                return

        batch_id = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        self._write_batch_to_datalake(parsed_dfs, batch_id)

        print("✅ Finished writing batch. Stopping Spark session.")
        self.spark.stop()

    def _parse_by_endpoint(self, raw_df):
        parsed_dfs = []

        # parse value as JSON string
        raw_json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

        # detect all endpoint types
        sample_schema = StructType().add("endpoint", StringType())
        endpoints_df = raw_json_df.select(from_json(col("json_str"), sample_schema).alias("data"))
        endpoints = endpoints_df.select("data.endpoint").distinct().rdd.map(lambda r: r.endpoint).collect()

        for endpoint in endpoints:
            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if not schema:
                print(f"⚠️ Skipping unknown endpoint '{endpoint}' (no schema defined)")
                continue

            df = raw_json_df \
                .select(from_json(col("json_str"), schema).alias("data")) \
                .filter(col("data.endpoint") == endpoint) \
                .select("data.*")

            parsed_dfs.append((endpoint, df))

        return parsed_dfs


    def _parse_kafka_batch(self):
        """
        Entry point: route to simple or endpoint-specific parsing logic.
        Determines if the message has an 'endpoint' field and dispatches accordingly.
        """
        raw_df = self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        # Peek into the messages with a lightweight schema just to check for 'endpoint'
        preview_schema = StructType().add("endpoint", StringType())

        base_df = raw_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), preview_schema).alias("data")) \
            .select("data.endpoint")

        has_endpoint = base_df.filter(col("endpoint").isNotNull()).limit(1).count() > 0

        if has_endpoint:
            return self._parse_by_endpoint(raw_df)
        else:
            # fallback for simple schema (e.g. yfinance)
            return raw_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema_yfinance).alias("data")) \
                .select("data.*")




 

    def _write_batch_to_datalake(self, parsed_dfs, batch_id):
        """
        Write a batch of (endpoint, DataFrame) to the datalake in multiple formats.
        """
        if not parsed_dfs:
            print(f"⚠️ Batch {batch_id} is empty — skipping.")
            return

        now = datetime.utcnow()
        timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        date_path = now.strftime("%Y/%m/%d/%H")
        for endpoint, df in parsed_dfs:
            if df.head(1) == []:
                print(f"⚠️ No data to write for endpoint '{endpoint}' in batch {batch_id}")
                continue
            
            for fmt in self.formats:
                
                path = f"s3a://{self.bucket}/raw/{self.topic}/{endpoint}/{fmt}/{date_path}/batch_{timestamp}"
                print(path)
                
                try:
                    df.write \
                        .option("fs.s3a.endpoint", self.endpoint) \
                        .mode("append") \
                        .format(fmt) \
                        .save(path)

                    #print(f"✅ Wrote {df.count()} rows to {path}")
                except Exception as e:
                    print(f"❌ Failed to write {fmt} for batch {batch_id} (endpoint: {endpoint}): {e}")


    def _resolve_hostname(self, hostname: str) -> str:
        """
        Resolve a Docker hostname (like 'minio_sp') to its IP address.
        """
        try:
            return socket.gethostbyname(hostname)
        except socket.gaierror:
            return None      

    def _get_storage_endpoint(self) -> str:
        raw_endpoint = self.storage_access.get("endpoint", "").strip()
        if not raw_endpoint:
            raise RuntimeError("Storage endpoint is not set")


        # for exemple http://minio_spg:9000 
        # parsed --> ParseResult(scheme='http', netloc='minio_spg:9000', path='', params='', query='', fragment='')

        # hostname  --> minio_spg
        parsed = urlparse(raw_endpoint)
        hostname = parsed.hostname

        

        if hostname:
            ip = self._resolve_hostname(hostname)
            if ip:
                # Replace hostname with IP, keep port and other URL parts
                new_netloc = ip
                if parsed.port:
                    new_netloc += f":{parsed.port}"

                new_url = urlunparse((
                    parsed.scheme,
                    new_netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment
                ))
                return new_url
            else:
                print(f"⚠️ Warning: Could not resolve hostname {hostname}, using raw endpoint")
                return raw_endpoint
        else:
            return raw_endpoint
        