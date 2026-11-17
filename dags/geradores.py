"""Geradores das duas fontes simuladas do Alerta Córrego.

Tudo aqui é DETERMINÍSTICO: a mesma execução do Airflow sempre gera os mesmos bytes.
Por isso reexecutar uma coleta não cria dado novo (idempotência) e o resultado é auditável.

Fontes simuladas (documentar no relatório como "dado simulado com geração justificada"):
  1. Nível do córrego (cm), a cada 10 min por ponto. Substitui o sensor ESP32 até haver sensor real.
  2. Cadastro de moradores (sintético). Dado pessoal 100% fictício, só para exercitar a LGPD.
"""
from __future__ import annotations

import json
import math
import random
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Sao_Paulo")

# ATENÇÃO: coordenadas APROXIMADAS dos bairros. Troque pelas coordenadas exatas dos pontos monitorados
# (as mesmas do MVP). O Open-Meteo tem grade de vários km, então a diferença é pequena para a chuva.
# base_cm, ganho_cm_mm e tau_h são parâmetros ILUSTRATIVOS do modelo de resposta do córrego:
# ajuste para que as faixas de nível fiquem coerentes com os cortes de corregos.py (MVP).
PONTOS = [
    {"ponto_id": "JD_PANTANAL", "nome": "Jardim Pantanal", "lat": -23.494, "lon": -46.411,
     "base_cm": 45.0, "ganho_cm_mm": 2.4, "tau_h": 4.0},
    {"ponto_id": "JD_ROMANO", "nome": "Jardim Romano", "lat": -23.489, "lon": -46.401,
     "base_cm": 50.0, "ganho_cm_mm": 2.2, "tau_h": 3.5},
    {"ponto_id": "JD_HELENA", "nome": "Jardim Helena", "lat": -23.487, "lon": -46.435,
     "base_cm": 40.0, "ganho_cm_mm": 2.0, "tau_h": 3.0},
    {"ponto_id": "GUAIANASES", "nome": "Guaianases", "lat": -23.541, "lon": -46.414,
     "base_cm": 35.0, "ganho_cm_mm": 1.8, "tau_h": 2.5},
]

# Probabilidades de falha injetadas de propósito (a Prata precisa ter o que barrar).
P_NULO, P_PICO, P_NEGATIVO, P_DUPLICADO = 0.015, 0.005, 0.003, 0.010


def _local(dt: datetime) -> datetime:
    """Converte qualquer datetime (inclusive pendulum, que o Airflow entrega) para datetime padrão em São Paulo."""
    return datetime.fromtimestamp(dt.timestamp(), TZ)


def _rng(*partes: str) -> random.Random:
    return random.Random("|".join(partes))


def gerar_leituras_nivel(
    chuva_por_ponto: dict[str, dict[str, float]],
    inicio: datetime,
    fim: datetime,
    passo_min: int = 10,
    aquecimento_h: int = 24,
) -> list[dict]:
    """Gera leituras de nível entre `inicio` (inclusive) e `fim` (exclusive).

    Modelo: reservatório linear. A cada passo, o excesso de nível decai com constante `tau_h`
    e ganha `ganho_cm_mm` por mm de chuva. A chuva vem do Open-Meteo (dado real), então o nível
    sobe e desce junto com a chuva real, com atraso. Ruído e falhas são injetados depois.

    chuva_por_ponto: {ponto_id: {"2026-11-10T14:00": mm, ...}} no fuso America/Sao_Paulo.
    """
    inicio = _local(inicio)
    fim = _local(fim)
    dt_h = passo_min / 60.0
    leituras: list[dict] = []

    for p in PONTOS:
        serie = chuva_por_ponto.get(p["ponto_id"], {})
        excesso = 0.0
        t = inicio - timedelta(hours=aquecimento_h)
        while t < fim:
            chave_hora = t.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M")
            mm_h = serie.get(chave_hora) or 0.0
            excesso = excesso * math.exp(-dt_h / p["tau_h"]) + p["ganho_cm_mm"] * mm_h * dt_h

            if t >= inicio:
                r = _rng("nivel", p["ponto_id"], t.isoformat())
                nivel = p["base_cm"] + excesso + r.gauss(0, 0.8)
                sorteio = r.random()
                nivel_out: float | None = round(nivel, 1)
                duplicar = False
                if sorteio < P_NULO:
                    nivel_out = None
                elif sorteio < P_NULO + P_PICO:
                    nivel_out = round(nivel + 300, 1)
                elif sorteio < P_NULO + P_PICO + P_NEGATIVO:
                    nivel_out = -abs(round(nivel, 1))
                elif sorteio < P_NULO + P_PICO + P_NEGATIVO + P_DUPLICADO:
                    duplicar = True
                reg = {
                    "ponto_id": p["ponto_id"],
                    "data_hora": t.isoformat(timespec="seconds"),
                    "nivel_cm": nivel_out,
                    "origem": "simulado",
                }
                leituras.append(reg)
                if duplicar:
                    leituras.append(dict(reg))
            t += timedelta(minutes=passo_min)
    return leituras


def _nome_de(morador_id: str) -> str:
    from faker import Faker

    fk = Faker("pt_BR")
    fk.seed_instance(zlib.crc32(morador_id.encode()))
    return fk.name()


def _telefone(rng: random.Random) -> str:
    """Celular brasileiro fictício. ~2% saem inválidos para exercitar a regra de qualidade."""
    if rng.random() < 0.02:
        return rng.choice(["123", "00000", "(11) 9999", "sem telefone"])
    ddd = rng.choice(["11", "11", "11", "12", "13"])
    return f"({ddd}) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"


def ids_ja_cadastrados(pasta: Path, ignorar: Path | None = None) -> list[str]:
    """Lê os arquivos de cadastro anteriores e devolve os moradores que ainda têm consentimento."""
    estado: dict[str, bool] = {}
    for arq in sorted(pasta.glob("cadastro_*.json")):
        if ignorar and arq.resolve() == ignorar.resolve():
            continue
        for m in json.loads(arq.read_text(encoding="utf-8")).get("moradores", []):
            estado[m["morador_id"]] = bool(m["consentimento"])
    return [mid for mid, ativo in estado.items() if ativo]


def gerar_cadastro(run_id: str, agora_iso: str, existentes: list[str]) -> list[dict]:
    """Gera novos cadastros, atualizações de telefone e opt-outs. 100% sintético."""
    rng = _rng("cadastro", run_id)
    inicial = not existentes
    registros: list[dict] = []

    for p in PONTOS:
        for i in range(25 if inicial else rng.randint(2, 6)):
            mid = f"M-{p['ponto_id']}-{run_id}-{i:02d}"
            registros.append({
                "morador_id": mid, "nome": _nome_de(mid), "telefone": _telefone(rng),
                "ponto_id": p["ponto_id"], "canal": rng.choice(["SMS", "WHATSAPP"]),
                "consentimento": True, "evento": "cadastro", "atualizado_em": agora_iso,
            })

    if existentes:
        for mid in rng.sample(existentes, k=min(2, len(existentes))):
            registros.append({
                "morador_id": mid, "nome": _nome_de(mid), "telefone": _telefone(rng),
                "ponto_id": mid.split("-")[1], "canal": rng.choice(["SMS", "WHATSAPP"]),
                "consentimento": True, "evento": "atualizacao", "atualizado_em": agora_iso,
            })
        if len(existentes) > 10:
            mid = rng.choice(existentes)
            registros.append({
                "morador_id": mid, "nome": None, "telefone": None,
                "ponto_id": mid.split("-")[1], "canal": None,
                "consentimento": False, "evento": "optout", "atualizado_em": agora_iso,
            })
    return registros
