from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder \
    .appName("Conteo de palabras Kafka Productor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # Establecer el nivel de registro a ERROR

# Configurar la salida de registros a un archivo
log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.ERROR)

# Configuración de Kafka
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9091",
    "subscribe": "kafkaSpark"
}

# Cargar los datos desde Kafka
lines = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Convertir los datos binarios a strings
lines = lines.selectExpr("CAST(value AS STRING)")

# Dividir las palabras por espacios en blanco
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

# Contar las palabras
wordCounts = words.groupBy("word").count()

# Iniciar el procesamiento en tiempo real
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
