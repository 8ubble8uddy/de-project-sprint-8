# Импорты.
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.streaming import StreamingQuery

# Переменные окружения.
KAFKA_TOPIC_IN = os.environ.get('KAFKA_TOPIC_IN')
KAFKA_TOPIC_OUT = os.environ.get('KAFKA_TOPIC_OUT')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
KAFKA_USER = os.environ.get('KAFKA_USER')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD')

PG_SOURCE_URL = os.environ.get('PG_SOURCE_URL')
PG_SOURCE_USER = os.environ.get('PG_SOURCE_USER')
PG_SOURCE_PASSWORD = os.environ.get('PG_SOURCE_PASSWORD')
PG_SOURCE_TABLE = os.environ.get('PG_SOURCE_TABLE')

PG_TARGET_URL = os.environ.get('PG_TARGET_URL')
PG_TARGET_USER = os.environ.get('PG_TARGET_USER')
PG_TARGET_PASSWORD = os.environ.get('PG_TARGET_PASSWORD')
PG_TARGET_TABLE = os.environ.get('PG_TARGET_TABLE')


# Объявление main-функции.
def main():

    # Создаём spark-сессию.
    spark = spark_init('join stream')

    # Читаем из топика Kafka сообщения с акциями от ресторанов.
    advertising_stream = read_advertising_stream(spark)

    # Вычитываем всех пользователей с подпиской на рестораны.
    subscribers_df = read_subscribers_df(spark)

    # Джойним данные из сообщения Kafka с пользователями подписки по restaurant_id.
    joined = join(advertising_stream, subscribers_df)

    # Запускаем стриминг.
    streaming = run_query(joined)
    streaming.awaitTermination()


def spark_init(test_name: str) -> SparkSession:
    spark = (
        SparkSession.builder \
        .master('local') \
        .appName(test_name) \
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
    return spark


def read_advertising_stream(spark: SparkSession) -> DataFrame:
    options = {
        'kafka.bootstrap.servers': KAFKA_BROKER,
        'kafka.security.protocol': 'SASL_SSL',
        'kafka.sasl.mechanism': 'SCRAM-SHA-512',
        'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PASSWORD}\";',
        'subscribe': KAFKA_TOPIC_IN
    }
    advertising_stream = spark.readStream.format('kafka').options(**options).load().transform(process_adv_campaign)
    return advertising_stream


def process_adv_campaign(df: DataFrame) -> DataFrame:
    adv_schema = StructType([
        StructField('restaurant_id', StringType(), nullable=False),
        StructField('adv_campaign_id', StringType(), nullable=False),
        StructField('adv_campaign_content', StringType(), nullable=False),
        StructField('adv_campaign_owner', StringType(), nullable=False),
        StructField('adv_campaign_owner_contact', StringType(), nullable=False),
        StructField('adv_campaign_datetime_start', IntegerType(), nullable=False),
        StructField('adv_campaign_datetime_end', IntegerType(), nullable=False),
        StructField('datetime_created', IntegerType(), nullable=False)
    ])
    adv_df = (df
        .withColumn('value', f.col('value').cast('string'))
        .withColumn('adv_campaign', f.from_json(f.col('value'), adv_schema))
        .select('adv_campaign.*')
        .withColumn('current_unix_ts', f.unix_timestamp(f.current_timestamp()))
        .where('adv_campaign_datetime_start <= current_unix_ts AND current_unix_ts < adv_campaign_datetime_end')
        .withColumn('timestamp', f.from_unixtime(f.col('datetime_created'), 'yyyy-MM-dd HH:mm:ss.SSS').cast(TimestampType()))
        .dropDuplicates(['adv_campaign_id', 'timestamp'])
        .withWatermark('timestamp', '10 minutes')
    )
    return adv_df


def read_subscribers_df(spark: SparkSession) -> DataFrame:
    options = {
        'url': f'jdbc:postgresql://{PG_SOURCE_URL}',
        'driver': 'org.postgresql.Driver',
        'dbtable': PG_SOURCE_TABLE,
        'user': PG_SOURCE_USER,
        'password': PG_SOURCE_PASSWORD
    }
    subscribers_df = spark.read.format('jdbc').options(**options).load().distinct()
    return subscribers_df


def join(advertising_stream: DataFrame, subscribers_df: DataFrame) -> DataFrame:
    joined_df = advertising_stream.join(subscribers_df, 'restaurant_id', 'inner').select(
        f.col('restaurant_id'),
        f.col('adv_campaign_id'),
        f.col('adv_campaign_content'),
        f.col('adv_campaign_owner'),
        f.col('adv_campaign_owner_contact'),
        f.col('adv_campaign_datetime_start'),
        f.col('adv_campaign_datetime_end'),
        f.col('datetime_created'),
        f.col('client_id'),
        f.col('current_unix_ts')
    )
    return joined_df


def run_query(df: DataFrame) -> StreamingQuery:

    def foreach_batch_function(df: DataFrame, *args):
        df.persist()
        write_push_notifications(df)
        write_subscribers_feedback(df)
        df.unpersist()

    query =  df.writeStream.foreachBatch(foreach_batch_function).trigger(processingTime='15 seconds').start()
    return query


def write_push_notifications(df: DataFrame):
    options = {
        'kafka.bootstrap.servers': KAFKA_BROKER,
        'kafka.security.protocol': 'SASL_SSL',
        'kafka.sasl.mechanism': 'SCRAM-SHA-512',
        'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{KAFKA_USER}\" password=\"{KAFKA_PASSWORD}\";',
        'topic': KAFKA_TOPIC_OUT
    }
    notifications_df = df.select(
        f.to_json(f.struct(
            f.col('restaurant_id'),
            f.col('adv_campaign_id'),
            f.col('adv_campaign_content'),
            f.col('adv_campaign_owner'),
            f.col('adv_campaign_owner_contact'),
            f.col('adv_campaign_datetime_start'),
            f.col('adv_campaign_datetime_end'),
            f.col('datetime_created'),
            f.col('client_id'),
            f.col('current_unix_ts').alias('trigger_datetime_created')
        )).alias('value')
    )
    notifications_df.write.format('kafka').options(**options).mode('append').save()


def write_subscribers_feedback(df: DataFrame):
    options = {
        'url': f'jdbc:postgresql://{PG_TARGET_URL}',
        'driver': 'org.postgresql.Driver',
        'dbtable': PG_TARGET_TABLE,
        'user': PG_TARGET_USER,
        'password': PG_TARGET_PASSWORD
    }
    feedback_df = df.select(
        f.col('restaurant_id'),
        f.col('adv_campaign_id'),
        f.col('adv_campaign_content'),
        f.col('adv_campaign_owner'),
        f.col('adv_campaign_owner_contact'),
        f.col('adv_campaign_datetime_start'),
        f.col('adv_campaign_datetime_end'),
        f.col('datetime_created'),
        f.col('client_id'),
        f.col('current_unix_ts').alias('trigger_datetime_created'),
        f.lit(None).cast(StringType()).alias('feedback')
    )
    feedback_df.write.format('jdbc').options(**options).mode('append').save()


# Вызов main-функции.
if __name__ == '__main__':
    main()
