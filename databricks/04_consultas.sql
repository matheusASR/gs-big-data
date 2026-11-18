-- Alerta Córrego — Consultas analíticas da camada Gold
-- Sensores e cadastros sintéticos.
-- Limiares de risco ilustrativos, não oficiais.

-- 1. Monitoramento por região
SELECT
    ponto_id,
    COUNT(*) AS total_medicoes,
    ROUND(AVG(nivel_cm), 2) AS nivel_medio_cm,
    ROUND(MAX(nivel_cm), 2) AS nivel_maximo_cm,
    ROUND(AVG(precip_mm), 2) AS precipitacao_horaria_media_mm,
    ROUND(MAX(precip_mm), 2) AS precipitacao_horaria_maxima_mm
FROM workspace.gs.ouro_fato_monitoramento
GROUP BY ponto_id
ORDER BY nivel_maximo_cm DESC;


-- 2. Evolução diária do nível da água
SELECT
    DATE(data_hora) AS data,
    ponto_id,
    COUNT(*) AS total_medicoes,
    ROUND(AVG(nivel_cm), 2) AS nivel_medio_cm,
    ROUND(MAX(nivel_cm), 2) AS nivel_maximo_cm
FROM workspace.gs.ouro_fato_monitoramento
GROUP BY DATE(data_hora), ponto_id
ORDER BY data, ponto_id;


-- 3. Relação entre precipitação e nível da água
SELECT
    ponto_id,
    DATE_TRUNC('HOUR', data_hora) AS hora,
    ROUND(AVG(nivel_cm), 2) AS nivel_medio_cm,
    ROUND(MAX(nivel_cm), 2) AS nivel_maximo_cm,
    ROUND(MAX(precip_mm), 2) AS chuva_horaria_mm
FROM workspace.gs.ouro_fato_monitoramento
GROUP BY ponto_id, DATE_TRUNC('HOUR', data_hora)
ORDER BY chuva_horaria_mm DESC, nivel_maximo_cm DESC
LIMIT 20;


-- 4. Classificação ilustrativa de risco
WITH classificacao AS (
    SELECT
        ponto_id,
        nivel_cm,
        CASE
            WHEN nivel_cm >= 70 THEN 'CRITICO'
            WHEN nivel_cm >= 55 THEN 'ALERTA'
            WHEN nivel_cm >= 40 THEN 'ATENCAO'
            ELSE 'BAIXO'
        END AS risco
    FROM workspace.gs.ouro_fato_monitoramento
)
SELECT
    ponto_id,
    risco,
    COUNT(*) AS total_medicoes,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY ponto_id),
        2
    ) AS percentual
FROM classificacao
GROUP BY ponto_id, risco
ORDER BY ponto_id,
    CASE risco
        WHEN 'CRITICO' THEN 1
        WHEN 'ALERTA' THEN 2
        WHEN 'ATENCAO' THEN 3
        ELSE 4
    END;


-- 5. Impacto potencial sobre moradores cadastrados
-- Consulta agregada: não expõe nomes ou telefones.
WITH risco_por_ponto AS (
    SELECT
        ponto_id,
        MAX(nivel_cm) AS nivel_maximo_cm,
        CASE
            WHEN MAX(nivel_cm) >= 70 THEN 'CRITICO'
            WHEN MAX(nivel_cm) >= 55 THEN 'ALERTA'
            WHEN MAX(nivel_cm) >= 40 THEN 'ATENCAO'
            ELSE 'BAIXO'
        END AS risco_maximo
    FROM workspace.gs.ouro_fato_monitoramento
    GROUP BY ponto_id
),
moradores_por_ponto AS (
    SELECT
        ponto_id,
        COUNT(DISTINCT morador_id) AS moradores_cadastrados
    FROM workspace.gs_restrito.prata_morador
    WHERE consentimento = TRUE
    GROUP BY ponto_id
)
SELECT
    r.ponto_id,
    ROUND(r.nivel_maximo_cm, 2) AS nivel_maximo_cm,
    r.risco_maximo,
    COALESCE(m.moradores_cadastrados, 0)
        AS moradores_potencialmente_impactados
FROM risco_por_ponto r
LEFT JOIN moradores_por_ponto m
    ON r.ponto_id = m.ponto_id
ORDER BY
    CASE r.risco_maximo
        WHEN 'CRITICO' THEN 1
        WHEN 'ALERTA' THEN 2
        WHEN 'ATENCAO' THEN 3
        ELSE 4
    END,
    moradores_potencialmente_impactados DESC;
