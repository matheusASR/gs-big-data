# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — Alerta Córrego (GS 2026.2, Big Data)
# MAGIC
# MAGIC Tipos corretos, nulos e inválidos tratados, **MERGE pela chave** e regras de qualidade **com o total barrado
# MAGIC por cada uma** (requisito 06).
# MAGIC
# MAGIC **Decisões e porquês**
# MAGIC - **MERGE, não INSERT:** a janela do Open-Meteo de uma execução se sobrepõe à da seguinte, e a previsão de um
# MAGIC   horário é substituída pelo valor posterior. O MERGE pela chave mantém a linha mais recente e torna a Prata idempotente.
# MAGIC - **Desempate por `lote_ts`** (data no nome do arquivo), e não por `carregado_em`: no Auto Loader todos os arquivos
# MAGIC   de um microlote recebem o mesmo `carregado_em`.
# MAGIC - **Quarentena só com a chave:** o que a regra barra vai para `prata_rejeitados` com a chave e o motivo, nunca com telefone.
# MAGIC - **LGPD (minimização):** o telefone em claro só existe no schema restrito `gs_restrito`. Opt-out apaga nome e telefone.
# MAGIC - **Fuso:** o CEMADEN entrega em UTC. A sessão roda em America/Sao_Paulo para os horários baterem com o Open-Meteo.

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window, functions as F, types as T

CAT, SCH, SCH_RESTRITO = "workspace", "gs", "gs_restrito"
PICO_CM = 80          # salto isolado de nível entre leituras de 10 min tratado como pico (ajuste se necessário)
PRECIP_MAX_MM_H = 150  # acima disto é fisicamente suspeito

# Mapeamento do JSON do CEMADEN. NÃO foi confirmado: rode printSchema() na Bronze e ajuste os nomes à direita.
CAMPOS_CEMADEN = {
    "estacao_id": "codestacao", "estacao_nome": "nome", "municipio": "cidade",
    "latitude": "latitude", "longitude": "longitude",
    "acc_1h": "acc1hr", "acc_3h": "acc3hr", "acc_6h": "acc6hr", "acc_12h": "acc12hr",
    "acc_24h": "acc24hr", "acc_48h": "acc48hr", "acc_72h": "acc72hr", "acc_96h": "acc96hr",
}
ORDEM_ACC = ["acc_1h", "acc_3h", "acc_6h", "acc_12h", "acc_24h", "acc_48h", "acc_72h", "acc_96h"]

LOTE_TS = F.regexp_extract("arquivo_origem", r"_(\d{8}T\d{4})\.json", 1).alias("lote_ts")

REGRAS_LOG = []   # (fonte, regra, barrados)
QUARENTENA = []   # DataFrames (fonte, regra, chave)

# COMMAND ----------

def barrar(df, fonte, regra, invalido, chaves):
    """Separa as linhas inválidas, registra o total barrado e guarda a chave na quarentena."""
    ruins = df.filter(invalido)
    n = ruins.count()
    REGRAS_LOG.append((fonte, regra, n))
    if n:
        QUARENTENA.append(ruins.select(
            F.lit(fonte).alias("fonte"), F.lit(regra).alias("regra"),
            F.concat_ws("|", *[F.col(c).cast("string") for c in chaves]).alias("chave")))
    return df.filter(~invalido)


def manter_mais_recente(df, fonte, chaves, ordem):
    """Uma linha por chave, a mais recente. Registra quantas foram descartadas por duplicidade."""
    w = Window.partitionBy(*chaves).orderBy(*[F.col(c).desc() for c in ordem])
    d = df.withColumn("_rn", F.row_number().over(w))
    n = d.filter("_rn > 1").count()
    REGRAS_LOG.append((fonte, "duplicado_mantida_a_mais_recente", n))
    return d.filter("_rn = 1").drop("_rn")


def prata_chuva(b):
    c = (b.select("arquivo_origem", F.explode("pontos").alias("p"))
         .select(LOTE_TS, F.col("p.ponto_id").alias("ponto_id"),
                 F.explode(F.arrays_zip(F.col("p.hourly.time").alias("t"),
                                        F.col("p.hourly.precipitation").alias("mm"))).alias("z"))
         .select("ponto_id", "lote_ts", F.to_timestamp("z.t").alias("data_hora"),
                 F.col("z.mm").cast("double").alias("precip_mm")))
    p = F.col("precip_mm")
    c = barrar(c, "chuva", f"precip_nula_negativa_ou_acima_de_{PRECIP_MAX_MM_H}mm_h",
               p.isNull() | (p < 0) | (p > PRECIP_MAX_MM_H), ["ponto_id", "data_hora"])
    return manter_mais_recente(c, "chuva", ["ponto_id", "data_hora"], ["lote_ts"])


def prata_sensor(b):
    s = (b.select("arquivo_origem", F.explode("leituras").alias("l"))
         .select(LOTE_TS, F.col("l.ponto_id").alias("ponto_id"),
                 F.to_timestamp("l.data_hora").alias("data_hora"),
                 F.col("l.nivel_cm").cast("double").alias("nivel_cm")))
    s = manter_mais_recente(s, "sensor", ["ponto_id", "data_hora"], ["lote_ts"])
    n = F.col("nivel_cm")
    s = barrar(s, "sensor", "nivel_nulo_ou_negativo", n.isNull() | (n < 0), ["ponto_id", "data_hora"])
    w = Window.partitionBy("ponto_id").orderBy("data_hora")
    s = s.withColumn("_ant", F.lag("nivel_cm").over(w)).withColumn("_prox", F.lead("nivel_cm").over(w))
    pico = F.coalesce((F.abs(n - F.col("_ant")) > PICO_CM) & (F.abs(n - F.col("_prox")) > PICO_CM), F.lit(False))
    s = barrar(s, "sensor", f"pico_isolado_maior_que_{PICO_CM}cm", pico, ["ponto_id", "data_hora"])
    return s.drop("_ant", "_prox")


def prata_cemaden(b):
    tipo = b.schema["dados"].dataType
    if not isinstance(tipo, T.ArrayType):
        raise ValueError("'dados' não é um array: ajuste prata_cemaden ao formato real do CEMADEN. "
                         f"Tipo atual: {tipo.simpleString()[:300]}")
    e = (b.select("arquivo_origem", F.to_timestamp("coletado_em_utc").alias("coletado_em"),
                  F.explode("dados").alias("e"))
         .select("arquivo_origem", "coletado_em", "e.*"))
    faltando = [v for v in CAMPOS_CEMADEN.values() if v not in e.columns]
    if faltando:
        raise ValueError(f"Campos ausentes no JSON do CEMADEN: {faltando}. Colunas disponíveis: {e.columns}")
    c = e.select(LOTE_TS, "coletado_em", *[F.col(v).alias(k) for k, v in CAMPOS_CEMADEN.items()])
    for k in ORDEM_ACC + ["latitude", "longitude"]:
        c = c.withColumn(k, F.col(k).cast("double"))
    # só o município de São Paulo (filtro informativo, não é regra de qualidade)
    mun = F.upper(F.translate("municipio", "ÃÁÂÀÉÊÍÓÔÕÚÇ", "AAAAEEIOOOUC"))
    fora = c.filter(mun != "SAO PAULO").count()
    REGRAS_LOG.append(("cemaden", "filtro_fora_do_municipio_de_sao_paulo", fora))
    c = c.filter(mun == "SAO PAULO")
    # acumulados precisam crescer com a janela: 1h <= 3h <= ... <= 96h
    viola = reduce(lambda a, b_: a | b_, [
        F.coalesce(F.col(x) > F.col(y), F.lit(False)) for x, y in zip(ORDEM_ACC, ORDEM_ACC[1:])])
    c = barrar(c, "cemaden", "acumulados_nao_crescem_com_a_janela", viola, ["estacao_id", "coletado_em"])
    return manter_mais_recente(c, "cemaden", ["estacao_id", "coletado_em"], ["lote_ts"])


def prata_cadastro(b):
    m = (b.select("arquivo_origem", F.explode("moradores").alias("m"))
         .select(LOTE_TS, "m.*")
         .withColumn("atualizado_em", F.to_timestamp("atualizado_em"))
         .withColumn("fone", F.regexp_replace(F.coalesce("telefone", F.lit("")), r"\D", "")))
    fone_ok = ((F.length("fone") == 10) | ((F.length("fone") == 11) & (F.substring("fone", 3, 1) == "9")))
    # regra: quem tem consentimento precisa de telefone válido (o rejeitado não vira estado do morador)
    m = barrar(m, "cadastro", "telefone_invalido_com_consentimento",
               F.col("consentimento") & ~fone_ok, ["morador_id"])
    m = manter_mais_recente(m, "cadastro", ["morador_id"], ["atualizado_em", "lote_ts"])
    ativo = F.col("consentimento")
    return m.select(
        "morador_id", "ponto_id", "canal", "consentimento", "atualizado_em",
        F.when(ativo, F.col("nome")).alias("nome"),          # opt-out elimina dado pessoal
        F.when(ativo, F.col("fone")).alias("telefone"))


def mesclar(df, destino, chaves, view):
    """MERGE pela chave: atualiza o que existe, insere o que é novo. Reexecutar não duplica."""
    df.createOrReplaceTempView(view)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {destino} AS SELECT * FROM {view} WHERE 1 = 0")
    cond = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"MERGE INTO {destino} AS alvo USING {view} AS novo ON {cond} "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

# COMMAND ----------
# >>> EXECUÇÃO
# MAGIC %md
# MAGIC ## Executar a Prata
# MAGIC Rode depois de cada ingestão da Bronze. É seguro rodar várias vezes.

# COMMAND ----------
# >>> EXECUÇÃO

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH_RESTRITO}")
REGRAS_LOG.clear(); QUARENTENA.clear()

B = lambda f: spark.table(f"{CAT}.{SCH}.bronze_{f}")

mesclar(prata_chuva(B("chuva")), f"{CAT}.{SCH}.prata_chuva_hora", ["ponto_id", "data_hora"], "lote_chuva")
mesclar(prata_sensor(B("sensor")), f"{CAT}.{SCH}.prata_nivel_corrego", ["ponto_id", "data_hora"], "lote_sensor")
mesclar(prata_cadastro(B("cadastro")), f"{CAT}.{SCH_RESTRITO}.prata_morador", ["morador_id"], "lote_cadastro")
if spark.catalog.tableExists(f"{CAT}.{SCH}.bronze_cemaden"):
    mesclar(prata_cemaden(B("cemaden")), f"{CAT}.{SCH}.prata_cemaden_estacao",
            ["estacao_id", "coletado_em"], "lote_cemaden")

# COMMAND ----------
# >>> EXECUÇÃO

# Evidência: total barrado por cada regra (print para o relatório) e quarentena só com chaves
log = (spark.createDataFrame(REGRAS_LOG, "fonte string, regra string, barrados long")
       .withColumn("executado_em", F.current_timestamp()))
log.write.mode("append").saveAsTable(f"{CAT}.{SCH}.log_qualidade")
if QUARENTENA:
    reduce(lambda a, b_: a.unionByName(b_), QUARENTENA) \
        .withColumn("registrado_em", F.current_timestamp()) \
        .write.mode("append").saveAsTable(f"{CAT}.{SCH}.prata_rejeitados")
display(log)

# COMMAND ----------
# >>> EXECUÇÃO
# MAGIC %md
# MAGIC ## Conferência (prints do relatório)
# MAGIC Linhas por tabela da Prata. Reexecutar este notebook não pode aumentar estes números sem dado novo.

# COMMAND ----------
# >>> EXECUÇÃO

for t in [f"{SCH}.prata_chuva_hora", f"{SCH}.prata_nivel_corrego", f"{SCH}.prata_cemaden_estacao",
          f"{SCH_RESTRITO}.prata_morador"]:
    if spark.catalog.tableExists(f"{CAT}.{t}"):
        print(t, spark.table(f"{CAT}.{t}").count())
display(spark.table(f"{CAT}.{SCH}.log_qualidade").orderBy(F.col("executado_em").desc()).limit(20))
