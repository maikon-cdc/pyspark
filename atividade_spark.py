from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder.appName("AnaliseHousing").getOrCreate()

df = spark.read.csv("housing.csv", header=True, inferSchema=True)

df.limit(5).toPandas().to_csv("amostra_5_linhas.csv", index=False)

resultados = df.groupBy("ocean_proximity").agg(
    count("*").alias("total_casas"),
    avg("median_house_value").alias("media_valor_casas")
)

resultados.toPandas().to_csv("resultados_analise.csv", index=False)

media_geral = df.agg(avg("median_house_value")).first()[0]
total_registros = df.count()

with open("resumo.txt", "w") as f:
    f.write(f"Total de registros: {total_registros}\n")
    f.write(f"Média geral de valores das casas: {media_geral:.2f}\n")

spark.stop()