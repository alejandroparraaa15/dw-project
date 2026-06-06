import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, mean, count
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, Normalizer
from pyspark.ml.regression import LinearRegression
from pymongo import MongoClient

# Configurar el conector oficial de MongoDB para la sesión distribuida de Spark
# (Descarga automáticamente el paquete java nativo necesario al inicializar)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'

def get_spark_session():
    return SparkSession.builder \
        .appName("Acme_Ltd_Advanced_Analytics_Engine") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/acme_financial_dwh.time_series") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/acme_financial_dwh") \
        .getOrCreate()

# =================================================================
# CASO DE USO A: AGREGACIÓN SIMPLE (DESCRIPTIVE ANALYTICS)
# =================================================================
def run_spark_aggregation_job():
    """
    [USE CASE A] Lee las series temporales, extrae el componente anual,
    calcula métricas de resumen por lote y persiste el resultado derivado.
    """
    spark = get_spark_session()
    print("🚀 Apache Spark: Iniciando Job de Agregación de Datos...")
    
    # 1. Cargar datos directamente desde la colección NoSQL de series temporales
    raw_df = spark.read.format("mongo").load()
    if raw_df.rdd.isEmpty():
        print("⚠️ No hay datos en la serie temporal para procesar.")
        return

    # UDF para extraer el año de negocio del string YYYY-MM-DD de forma segura
    extract_year_udf = udf(lambda date_str: int(date_str.split("-")[0]) if date_str else None, IntegerType())
    # UDF para extraer el precio de cierre del mapa flexible heterogéneo
    extract_close_udf = udf(lambda val_map: float(val_map["close"]) if val_map and "close" in val_map else None, DoubleType())

    processed_df = raw_df.withColumn("business_year", extract_year_udf(col("business_date"))) \
                         .withColumn("close_price", extract_close_udf(col("values")))
    
    # 2. Agrupar por clave de partición analítica (Asset + Año) y computar resúmenes
    aggregated_df = processed_df.groupBy("assetId", "business_year") \
        .agg(
            count("close_price").alias("cnt"), # Equivalente al cómputo exigido en el PDF
            mean("close_price").alias("average_close")
        ).filter(col("business_year").isNotNull())

    # 3. Persistir la salida analítica de vuelta en una nueva colección dedicada
    aggregated_df.write \
        .format("mongo") \
        .option("collection", "analytics_totals") \
        .mode("append") \
        .save()
        
    print("✅ Apache Spark: Job de Agregación completado y guardado en 'analytics_totals'.")

# =================================================================
# CASO DE USO B: REGRESIÓN DE MACHINE LEARNING (PREDICTIVE ANALYTICS)
# =================================================================
def run_spark_machine_learning_job(asset_id: str):
    """
    [USE CASE B] Extrae un conjunto financiero de entrenamiento, procesa vectores de
    características, ajusta una Regresión Lineal y guarda las predicciones futuras.
    """
    spark = get_spark_session()
    print(f"🚀 Apache Spark: Iniciando Pipeline de ML para el activo {asset_id}...")
    
    raw_df = spark.read.format("mongo").load()
    if raw_df.rdd.isEmpty():
        return
        
    # Funciones de extracción de indicadores para construir el vector de variables continuas
    extract_field_udf = lambda field: udf(lambda m: float(m[field]) if m and field in m else None, DoubleType())
    timestamp_udf = udf(lambda d: int(datetime.strptime(d, "%Y-%m-%d").timestamp()) if d else None, IntegerType())

    df = raw_df.filter(col("assetId") == asset_id) \
               .withColumn("open", extract_field_udf("open")(col("values"))) \
               .withColumn("close", extract_field_udf("close")(col("values"))) \
               .withColumn("low", extract_field_udf("low")(col("values"))) \
               .withColumn("high", extract_field_udf("high")(col("values"))) \
               .withColumn("seconds", timestamp_udf(col("business_date"))) \
               .filter(col("open").isNotNull() & col("close").isNotNull())

    if df.count() < 2:
        print("⚠️ Datos insuficientes para entrenar el modelo de Regresión.")
        return

    # 1. Feature Engineering: Preparar y ensamblar vectores continuos
    assembler = VectorAssembler(inputCols=["seconds", "close", "low", "high"], outputCol="features")
    assembled_df = assembler.transform(df)
    
    # 2. Normalización de las características analíticas
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
    normalized_df = normalizer.transform(assembled_df)
    
    # 3. Dividir aleatoriamente en subconjuntos de Entrenamiento (70%) y Test (30%)
    training_data, test_data = normalized_df.randomSplit([0.7, 0.3], seed=42)
    
    # 4. Configurar y ajustar el modelo de Regresión Lineal exigido
    lr = LinearRegression(featuresCol="normFeatures", labelCol="open", maxIter=10, regParam=1.0, elasticNetParam=1.0)
    lr_model = lr.fit(training_data)
    
    # 5. Generar predicciones sobre el conjunto de test para evaluar el rendimiento
    predictions = lr_model.transform(test_data).select("seconds", "open", "prediction")
    
    # 6. Escribir las salidas predictivas directamente en la base de datos NoSQL
    predictions.write \
        .format("mongo") \
        .option("collection", "analytics_predictions") \
        .mode("append") \
        .save()
        
    print("✅ Apache Spark: Pipeline de ML finalizado con éxito en 'analytics_predictions'.")

if __name__ == "__main__":
    # Prueba de ejecución directa por lotes desde la consola
    run_spark_aggregation_job()
    run_spark_machine_learning_job("QDL/BITFINEX/BTCUSD")