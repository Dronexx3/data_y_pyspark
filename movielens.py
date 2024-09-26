from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("MovieLensBasicAnalysis") \
    .master("spark://54.226.84.147:7077") \
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
first = df.show(5)
print(f"Primeras 5 filas del DataFrame: {first}")

print("\nEstadísticas descriptivas del DataFrame:")
print(df.describe())

# Detener la sesión de Spark
spark.stop()
