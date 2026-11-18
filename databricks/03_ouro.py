"""
Alerta Córrego — Camada Gold (Ouro)

Modelo dimensional:
- ouro_dim_ponto: dimensão dos pontos monitorados
- ouro_dim_tempo: dimensão temporal
- ouro_fato_monitoramento: medições de nível associadas à chuva horária

Origem: tabelas Silver no Databricks.
"""

from pyspark.sql import functions as F

CAT = "workspace"
SCH = "gs"

# 1. Dimensão de pontos monitorados
pontos = (
    spark.table(f"{CAT}.{SCH}.prata_nivel_corrego")
    .select("ponto_id")
    .distinct()
)

(
    pontos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CAT}.{SCH}.ouro_dim_ponto")
)

# 2. Dimensão de tempo
tempo = (
    spark.table(f"{CAT}.{SCH}.prata_nivel_corrego")
    .select("data_hora")
    .distinct()
    .withColumn("data", F.to_date("data_hora"))
    .withColumn("ano", F.year("data_hora"))
    .withColumn("mes", F.month("data_hora"))
    .withColumn("dia", F.dayofmonth("data_hora"))
    .withColumn("hora", F.hour("data_hora"))
    .withColumn("minuto", F.minute("data_hora"))
)

(
    tempo.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CAT}.{SCH}.ouro_dim_tempo")
)

# 3. Tabela fato: nível do córrego e precipitação horária
sensor = spark.table(f"{CAT}.{SCH}.prata_nivel_corrego")
chuva = spark.table(f"{CAT}.{SCH}.prata_chuva_hora")

chuva_hora = chuva.select(
    F.col("ponto_id"),
    F.col("data_hora").alias("hora_chuva"),
    F.col("precip_mm")
)

fato = (
    sensor.alias("s")
    .join(
        chuva_hora.alias("c"),
        (F.col("s.ponto_id") == F.col("c.ponto_id"))
        & (
            F.date_trunc("hour", F.col("s.data_hora"))
            == F.col("c.hora_chuva")
        ),
        "left"
    )
    .select(
        F.col("s.ponto_id"),
        F.col("s.data_hora"),
        F.col("s.nivel_cm"),
        F.col("c.precip_mm"),
        F.col("s.lote_ts")
    )
)

(
    fato.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CAT}.{SCH}.ouro_fato_monitoramento")
)

# 4. Verificação das tabelas Gold
for tabela in [
    "ouro_dim_ponto",
    "ouro_dim_tempo",
    "ouro_fato_monitoramento",
]:
    total = spark.table(f"{CAT}.{SCH}.{tabela}").count()
    print(f"{tabela}: {total} registros")
