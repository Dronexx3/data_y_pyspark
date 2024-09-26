from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("MovieLensBasicAnalysis") \
    .master("spark://54.226.84.147:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "2") \
    .getOrCreate()

# Cargar el dataset MovieLens
file_path = "u_data.csv"
columns = ["user_id", "item_id", "rating", "timestamp"]

# Leer el archivo u.data como un DataFrame
df = spark.read.csv(file_path, sep='\t', inferSchema=True, header=False)
df = df.toDF(*columns)

# Mostrar esquema del DataFrame
df.printSchema()

# Mostrar los primeros 5 registros
df.show(5)

# Contar el número total de registros
total_records = df.count()
print(f"Total de registros: {total_records}")

# Mostrar estadísticas básicas del dataset
df.describe().show()

# Contar el número de usuarios únicos
unique_users = df.select("user_id").distinct().count()
print(f"Total de usuarios únicos: {unique_users}")

# Contar el número de items (películas) únicos
unique_items = df.select("item_id").distinct().count()
print(f"Total de items (películas) únicos: {unique_items}")

# Mostrar la distribución de ratings
df.groupBy("rating").count().orderBy("rating").show()

# Detener la sesión de Spark
spark.stop()
