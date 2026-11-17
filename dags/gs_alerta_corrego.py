"""DAG de coleta do Alerta Córrego (GS 2026.2, Big Data).

Fluxo:  inicio >> [coletar_chuva, coletar_cemaden, gerar_cadastro]
        coletar_chuva >> gerar_sensores
        [coletar_chuva, coletar_cemaden, gerar_cadastro, gerar_sensores] >> conferir_chegada

Cada tarefa grava UM arquivo por coleta em  <CHEGADA_DIR>/<fonte>/<fonte>_<AAAAMMDDTHHMM>.json
(a data da coleta vai no nome, req. 02). Esses arquivos sobem ao Volume do Databricks em lotes.

Variables do Airflow (nenhuma chave no código, req. 03):
  CHEGADA_DIR         pasta de pouso            (padrão /opt/airflow/chegada)
  CEMADEN_URL         endpoint do Mapa Interativo (confirme no DevTools; ver README)
  ALERTA_WEBHOOK_URL  webhook (Discord/Slack) para notificar falhas (opcional)
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

import geradores

log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
# O endpoint do CEMADEN mostra só o presente. Uma execução retroativa (catchup) rotularia a foto de hoje
# com data antiga, então a tarefa só coleta se a execução for recente.
JANELA_RECENTE = timedelta(hours=12)


# --------------------------------------------------------------------------- utilidades
def _chegada() -> Path:
    return Path(Variable.get("CHEGADA_DIR", "/opt/airflow/chegada"))


def _arquivo(fonte: str, data_interval_end) -> Path:
    pasta = _chegada() / fonte
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta / f"{fonte}_{data_interval_end.strftime('%Y%m%dT%H%M')}.json"


def _gravar(caminho: Path, conteudo: dict) -> None:
    caminho.write_text(json.dumps(conteudo, ensure_ascii=False), encoding="utf-8")
    log.info("Arquivo gravado: %s (%d bytes)", caminho, caminho.stat().st_size)


def notificar_falha(context) -> None:
    """on_failure_callback: avisa por webhook (se configurado) e sempre registra no log."""
    ti = context["task_instance"]
    msg = f"[Alerta Córrego] falha em {ti.dag_id}.{ti.task_id} (execução {context.get('logical_date')})"
    log.error(msg)
    try:
        url = Variable.get("ALERTA_WEBHOOK_URL", None)
        if url:
            requests.post(url, json={"content": msg, "text": msg}, timeout=10)
    except Exception as exc:  # a notificação nunca pode derrubar a tarefa
        log.warning("Webhook indisponível: %s", exc)


# --------------------------------------------------------------------------- tarefas
def coletar_chuva(data_interval_start, data_interval_end, **_):
    """Chuva horária (mm) do Open-Meteo para os 4 pontos. Fonte externa real, sem chave."""
    # Janela com 1 dia de folga antes (aquecimento do modelo de nível) e 1 dia depois (previsão).
    inicio = (data_interval_start - timedelta(days=1)).in_timezone("America/Sao_Paulo").date()
    fim = (data_interval_end + timedelta(days=1)).in_timezone("America/Sao_Paulo").date()

    pontos = []
    for p in geradores.PONTOS:
        resp = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": p["lat"], "longitude": p["lon"],
                "hourly": "precipitation", "timezone": "America/Sao_Paulo",
                "start_date": inicio.isoformat(), "end_date": fim.isoformat(),
            },
            timeout=30,
        )
        resp.raise_for_status()  # falha explícita, sem falha silenciosa (retries cuidam do resto)
        corpo = resp.json()
        pontos.append({
            "ponto_id": p["ponto_id"], "nome": p["nome"],
            "latitude": corpo.get("latitude"), "longitude": corpo.get("longitude"),
            "hourly": corpo["hourly"],
        })

    _gravar(_arquivo("chuva", data_interval_end), {
        "coletado_em": pendulum.now("UTC").isoformat(),
        "fonte": "open-meteo/forecast", "janela": [inicio.isoformat(), fim.isoformat()],
        "pontos": pontos,
    })


def coletar_cemaden(data_interval_end, **_):
    """Foto atual dos pluviômetros automáticos do CEMADEN (acumulados 1 a 96 h). Fonte externa real."""
    if pendulum.now("UTC") - data_interval_end > JANELA_RECENTE:
        raise AirflowSkipException("Execução retroativa: o endpoint do CEMADEN só mostra o presente.")

    url = Variable.get("CEMADEN_URL", "http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php?uf=SP")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    _gravar(_arquivo("cemaden", data_interval_end), {
        "coletado_em_utc": pendulum.now("UTC").isoformat(),
        "fonte": url,
        "dados": resp.json(),  # bruto, sem tratamento: a Bronze guarda como veio (o CEMADEN entrega em UTC)
    })


def gerar_cadastro(data_interval_end, **_):
    """Cadastro de moradores SINTÉTICO (Faker). Nenhum dado real."""
    destino = _arquivo("cadastro", data_interval_end)
    existentes = geradores.ids_ja_cadastrados(destino.parent, ignorar=destino)
    run_id = data_interval_end.strftime("%Y%m%dT%H%M")
    registros = geradores.gerar_cadastro(run_id, data_interval_end.isoformat(), existentes)
    _gravar(destino, {"gerado_em": data_interval_end.isoformat(), "sintetico": True, "moradores": registros})


def gerar_sensores(data_interval_start, data_interval_end, **_):
    """Nível do córrego SIMULADO a partir da chuva real que acabou de ser coletada."""
    arq_chuva = _arquivo("chuva", data_interval_end)
    chuva = json.loads(arq_chuva.read_text(encoding="utf-8"))
    chuva_por_ponto = {
        p["ponto_id"]: dict(zip(p["hourly"]["time"], p["hourly"]["precipitation"]))
        for p in chuva["pontos"]
    }
    leituras = geradores.gerar_leituras_nivel(chuva_por_ponto, data_interval_start, data_interval_end)
    _gravar(_arquivo("sensor", data_interval_end), {
        "gerado_em": data_interval_end.isoformat(), "simulado": True,
        "modelo": "reservatorio_linear(base, ganho_cm_mm, tau_h) + ruido + falhas injetadas",
        "leituras": leituras,
    })


def conferir_chegada(data_interval_end, **_):
    """Falha se faltar o arquivo de alguma fonte desta execução."""
    esperadas = ["chuva", "sensor", "cadastro"]
    if pendulum.now("UTC") - data_interval_end <= JANELA_RECENTE:
        esperadas.append("cemaden")
    faltando = [f for f in esperadas if not _arquivo(f, data_interval_end).exists()]
    if faltando:
        raise AirflowFailException(f"Arquivos ausentes em chegada/: {faltando}")
    log.info("Chegada conferida: %s", esperadas)


# --------------------------------------------------------------------------- DAG
with DAG(
    dag_id="gs_alerta_corrego",
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 11, 10, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "on_failure_callback": notificar_falha},
    tags=["gs-2026-2", "big-data", "alerta-corrego"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    t_chuva = PythonOperator(task_id="coletar_chuva", python_callable=coletar_chuva)
    t_cemaden = PythonOperator(task_id="coletar_cemaden", python_callable=coletar_cemaden)
    t_cadastro = PythonOperator(task_id="gerar_cadastro", python_callable=gerar_cadastro)
    t_sensor = PythonOperator(task_id="gerar_sensores", python_callable=gerar_sensores)
    # none_failed: o CEMADEN pode ser "skipped" em execução retroativa, e a conferência segue.
    t_conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada,
                                trigger_rule="none_failed")

    inicio >> [t_chuva, t_cemaden, t_cadastro]
    t_chuva >> t_sensor
    [t_chuva, t_cemaden, t_cadastro, t_sensor] >> t_conferir
