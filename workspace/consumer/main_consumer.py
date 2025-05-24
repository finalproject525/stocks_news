""" try:
    # When running inside the consumer container
    from SparkKafkaConsumer import BaseKafkaConsumer
except ImportError:
    # When running from dev container """
    
from consumer.SparkKafkaConsumer import BaseKafkaConsumer

    
import os
from config import USE_MINIO, BROKER, TOPIC, BUCKET, OUTPUT_FORMATS, MINIO_ACCESS, S3_ACCESS,SCHEMA_DEFAULT
#from schemas.pyspark_schemas import schema_yfinance
import os
from pathlib import Path
def main():
 

    use_minio_env = os.getenv("USE_MINIO")

    if use_minio_env is not None:
        use_minio = use_minio_env.lower() in ["true", "1", "yes"]
    else:
        use_minio = USE_MINIO
  
    storage_access = MINIO_ACCESS if use_minio else S3_ACCESS
  
    """ 
    schema_name = os.getenv("SCHEMA", SCHEMA_DEFAULT).lower()
       schema = schema_yfinance if schema_name == 'yfinance' else schema_finnhub
    """

    topic_name = os.getenv("TOPIC", TOPIC).lower()
    topic = TOPIC if topic_name == 'yfinance-data' else 'finnhub-data'

    output_formats_env = os.getenv("OUTPUT_FORMATS")

    if output_formats_env:
        parse_formats = lambda s: [fmt.strip() for fmt in s.split(",") if fmt.strip()]
        output_formats = parse_formats(output_formats_env)
    else:
        output_formats = OUTPUT_FORMATS
    ## if need add os.getenv(BUCKET) for replace the existante one 



 #           schema=schema,

    """  print(use_minio)
    print(storage_access)
    print(topic_name)
    print(topic)
    print(output_formats_env)
    print(output_formats)
    print(BUCKET)
    print(BROKER[0]) """
    
    consumer = BaseKafkaConsumer(
        
            bucket=BUCKET,
            kafka_bootstrap_servers=BROKER[0],
            topic=topic,
            formats=output_formats,
            storage_access = storage_access
    )
 



    consumer.stream_kafka_to_storage()

if __name__ == "__main__":
    main()