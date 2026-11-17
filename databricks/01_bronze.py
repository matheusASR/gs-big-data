# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Alerta Córrego (GS 2026.2, Big Data)
# MAGIC
# MAGIC Lê os arquivos que o Airflow gravou em `chegada/` e que foram enviados ao Volume pela interface,
# MAGIC **só o que é novo**, sem duplicar (requisitos 04 e 05).
# MAGIC
# MAGIC **Por que Auto Loader?** Ele guarda no *checkpoint* quais arquivos já leu. No 2º lote entram só os
# MAGIC arquivos novos, e rodar de novo sem arquivo novo não muda nada. A Bronze guarda o dado **como veio**
# MAGIC (1 linha por arquivo, com os arrays aninhados), mais `arquivo_origem` e `carregado_em` para rastreio.
# MAGIC
# MAGIC **Como usar (gera as evidências do relatório):**
# MAGIC 1. Rode as células de preparação.
# MAGIC 2. Suba o **lote 1** em `chuva/`, `cemaden/`, `sensor/` e `cadastro/` (Catalog > Volumes > chegada > Upload).
# MAGIC 3. Defina o widget `etapa` como `1 - depois do lote 1` e rode a célula **Ingerir**.
# MAGIC 4. Em **outro dia**, suba o **lote 2**, `etapa = 3 - depois do lote 2` e rode **Ingerir** de novo.
# MAGIC 5. Rode **Ingerir** mais uma vez sem arquivo novo, `etapa = 4 - reexecução sem arquivo novo`. A contagem não pode mudar.
# MAGIC 6. Tire print da tabela `log_contagens` (célula final).

# COMMAND ----------

from pyspark.sql import functions as F

CAT, SCH, VOL = "workspace", "gs", "chegada"
BASE = f"/Volumes/{CAT}/{SCH}/{VOL}"
FONTES = ["chuva", "cemaden", "sensor", "cadastro"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{SCH}.{VOL}")
for f in FONTES:
    dbutils.fs.mkdirs(f"{BASE}/{f}")

dbutils.widgets.text("etapa", "0 - antes do lote 1", "Etapa (rótulo da evidência)")
print("Volume pronto em", BASE)

# COMMAND ----------

def ingerir(fonte: str):
    """Auto Loader incremental e idempotente: um checkpoint e uma tabela Bronze por fonte."""
    leitura = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .option("multiLine", "true")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
    )
    return (
        leitura.writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CAT}.{SCH}.bronze_{fonte}")
    )


def contar(etapa: str):
    """Registra linhas e arquivos distintos de cada Bronze. É a evidência de 'antes e depois'."""
    linhas = []
    for f in FONTES:
        t = f"{CAT}.{SCH}.bronze_{f}"
        if spark.catalog.tableExists(t):
            r = spark.sql(f"SELECT count(*) AS n, count(DISTINCT arquivo_origem) AS a FROM {t}").first()
            linhas.append((etapa, t, int(r.n), int(r.a)))
        else:
            linhas.append((etapa, t, 0, 0))
    df = (spark.createDataFrame(linhas, "etapa string, tabela string, linhas long, arquivos long")
          .withColumn("registrado_em", F.current_timestamp()))
    df.write.mode("append").saveAsTable(f"{CAT}.{SCH}.log_contagens")
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingerir (rode depois de cada lote)
# MAGIC `chuva` e `sensor` têm **1 linha por arquivo** (o array de pontos ou de leituras fica aninhado até a Prata).

# COMMAND ----------

etapa = dbutils.widgets.get("etapa")

for fonte in FONTES:
    pasta = dbutils.fs.ls(f"{BASE}/{fonte}")
    if not pasta:
        print(f"[aviso] {fonte}/ está vazia: suba os arquivos do lote antes de ingerir.")
        continue
    ingerir(fonte).awaitTermination()
    print(f"[ok] bronze_{fonte}")

display(contar(etapa))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evidência para o relatório (item 07 do slide 38)
# MAGIC Print desta tabela: contagens da Bronze antes e depois do 2º lote e da 2ª execução.

# COMMAND ----------

display(spark.table(f"{CAT}.{SCH}.log_contagens").orderBy("registrado_em", "tabela"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Conferência rápida: o CEMADEN veio como esperado?
# MAGIC O schema do JSON do CEMADEN **não foi confirmado**. Rode `printSchema()` e ajuste o mapeamento `CAMPOS`
# MAGIC no notebook da Prata antes de seguir.

# COMMAND ----------

if spark.catalog.tableExists(f"{CAT}.{SCH}.bronze_cemaden"):
    spark.table(f"{CAT}.{SCH}.bronze_cemaden").printSchema()
