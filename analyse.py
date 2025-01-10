from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("SNCF_Objets_Trouves").getOrCreate()


file_path = "objets-trouves-restitution.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")

# Renommer les colonnes problématiques
data = data.withColumnRenamed("Gare", "gare") \
           .withColumnRenamed("Nature d'objets", "nature")

# Trouver les 5 gares ayant trouvé le plus d'objets
top_gares = (
    data.groupBy("gare")
    .agg(count("*").alias("nombre_objets"))
    .orderBy(col("nombre_objets").desc())
    .limit(5)
)
top_gares.show()

# Trouver la nature de l'objet le plus retrouvé
nature_top = (
    data.groupBy("nature")
    .agg(count("*").alias("nombre_objets"))
    .orderBy(col("nombre_objets").desc())
    .limit(1)
)
nature_top.show()

# Arrêter la session Spark
spark.stop()
