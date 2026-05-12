-- ============================================================
-- NYC Mobility Analytics: Yellow Taxi vs HVFHV (2019-2025)
-- Queries para datasets do QuickSight (Athena sobre Gold/Iceberg)
-- Cada query = 1 dataset no QuickSight
-- ============================================================

-- ============================================================
-- DATASET PRINCIPAL (base para todos os visuais)
-- Importar como "ds_fato_corrida" no QuickSight
-- ============================================================
-- No QuickSight: New Dataset > Athena > Custom SQL
-- Database: nyc_taxi_gold

SELECT
    f.id_corrida,
    f.datahora_embarque,
    f.ano_mes,
    f.year,
    f.month,
    f.duracao_segundos,
    f.distancia_percorrida,
    f.valor_justo,
    f.valor_gorjeta,
    f.valor_pedagio,
    f.valor_taxa_modernizacao,
    f.valor_taxa_aeroporto,
    f.valor_taxa_congestionamento,
    f.valor_taxa_congestionamento_cbd,
    f.valor_seguro_bfc,
    f.valor_demais_taxas,
    f.quantidade_viagem,
    -- Dimensoes
    t.grupo_tipo_transporte,
    e.empresa,
    pu.distrito   AS distrito_embarque,
    pu.zona       AS zona_embarque,
    pu.zona_servico AS zona_servico_embarque,
    do_.distrito  AS distrito_desembarque,
    do_.zona      AS zona_desembarque,
    d.faixa_distancia,
    d.descricao_distancia,
    h.faixa_horaria,
    g.teve_gorjeta,
    c.incide_taxa_cbd
FROM nyc_taxi_gold.fato_corrida f
LEFT JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
LEFT JOIN nyc_taxi_gold.dim_empresa e ON f.id_empresa = e.id_empresa
LEFT JOIN nyc_taxi_gold.dim_local pu ON f.id_local_embarque = pu.id_local
LEFT JOIN nyc_taxi_gold.dim_local do_ ON f.id_local_desembarque = do_.id_local
LEFT JOIN nyc_taxi_gold.dim_distancia d ON f.id_faixa_distancia = d.id_distancia
LEFT JOIN nyc_taxi_gold.dim_tempo_horario h ON f.id_faixa_horaria = h.id_faixa_horaria
LEFT JOIN nyc_taxi_gold.dim_status_gorjeta g ON f.id_status_gorjeta = g.id_status_gorjeta
LEFT JOIN nyc_taxi_gold.dim_status_congestionamento c ON f.id_status_congestionamento = c.id_status_congestionamento
;


-- ============================================================
-- 2.1 Q1 - DINAMICA COMPETITIVA YELLOW TAXI vs HVFHV
-- ============================================================

-- Q1.1: Evolucao da participacao de mercado (corridas) - Grafico de linha
-- Visual: Line chart, X=ano_mes, Y=pct_mercado, Color=grupo_tipo_transporte
SELECT
    ano_mes,
    grupo_tipo_transporte,
    COUNT(*) AS total_corridas,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano_mes), 2) AS pct_mercado
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE grupo_tipo_transporte IN ('TAXI', 'HVFHV')
GROUP BY ano_mes, grupo_tipo_transporte
ORDER BY ano_mes
;

-- Q1.2: Distribuicao de corridas por zona e tipo (Top 10 zonas) - Bar chart
-- Visual: Horizontal bar, Y=zona_embarque, X=total_corridas, Color=grupo_tipo_transporte
SELECT
    l.zona AS zona_embarque,
    t.grupo_tipo_transporte,
    COUNT(*) AS total_corridas
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
JOIN nyc_taxi_gold.dim_local l ON f.id_local_embarque = l.id_local
WHERE t.grupo_tipo_transporte IN ('TAXI', 'HVFHV')
GROUP BY l.zona, t.grupo_tipo_transporte
ORDER BY total_corridas DESC
LIMIT 20
;


-- ============================================================
-- 2.2 Q2 - FATORES QUE INFLUENCIAM A GORJETA
-- ============================================================

-- Q2.1: Probabilidade de gorjeta por servico - Bar chart
-- Visual: Bar chart, X=grupo_tipo_transporte, Y=pct_com_gorjeta
SELECT
    t.grupo_tipo_transporte,
    COUNT(*) AS total_corridas,
    SUM(CASE WHEN f.valor_gorjeta > 0 THEN 1 ELSE 0 END) AS corridas_com_gorjeta,
    ROUND(100.0 * SUM(CASE WHEN f.valor_gorjeta > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_com_gorjeta
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE t.grupo_tipo_transporte IN ('TAXI', 'HVFHV')
GROUP BY t.grupo_tipo_transporte
;

-- Q2.2: Valor medio da gorjeta (quando paga) - Bar chart
-- Visual: Bar chart, X=grupo_tipo_transporte, Y=gorjeta_media
SELECT
    t.grupo_tipo_transporte,
    ROUND(AVG(f.valor_gorjeta), 2) AS gorjeta_media
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE f.valor_gorjeta > 0
  AND t.grupo_tipo_transporte IN ('TAXI', 'HVFHV')
GROUP BY t.grupo_tipo_transporte
;

-- Q2.3: Correlacao gorjeta vs valor da corrida e distancia - Scatter plot
-- Visual: Scatter, X=valor_justo, Y=valor_gorjeta, Color=grupo_tipo_transporte
-- (usar amostra para performance)
SELECT
    t.grupo_tipo_transporte,
    f.valor_justo AS fare_amount,
    f.valor_gorjeta AS tip_amount,
    f.distancia_percorrida AS trip_distance
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE f.valor_gorjeta > 0
  AND f.valor_justo BETWEEN 1 AND 200
  AND t.grupo_tipo_transporte IN ('TAXI', 'HVFHV')
  AND f.year = 2025
LIMIT 50000
;


-- ============================================================
-- 2.3 Q3 - IMPACTO DA PANDEMIA DE COVID-19
-- ============================================================

-- Q3.1: Volume mensal de corridas (2019-2025) - Line chart
-- Visual: Multi-line, X=ano_mes, Y=total_corridas, Color=grupo_tipo_transporte
-- Marcar: MAR 2020 Lockdown, Pandemic wave, Omicron
SELECT
    ano_mes,
    t.grupo_tipo_transporte,
    COUNT(*) AS total_corridas
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
GROUP BY ano_mes, t.grupo_tipo_transporte
ORDER BY ano_mes
;

-- Q3.2: Mudanca no valor medio da gorjeta (pre, durante, pos-pandemia) - Line chart
-- Visual: Line, X=ano_mes, Y=gorjeta_media, Color=grupo_tipo_transporte
SELECT
    ano_mes,
    t.grupo_tipo_transporte,
    ROUND(AVG(f.valor_gorjeta), 2) AS gorjeta_media,
    CASE
        WHEN ano_mes < '2020-03' THEN 'PRE-PANDEMIA'
        WHEN ano_mes BETWEEN '2020-03' AND '2021-06' THEN 'DURANTE'
        ELSE 'POS-PANDEMIA'
    END AS periodo_pandemia
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE f.valor_gorjeta > 0
GROUP BY ano_mes, t.grupo_tipo_transporte
ORDER BY ano_mes
;


-- ============================================================
-- 2.4 Q4 - TAXA DE CONGESTIONAMENTO DO CBD (Jan 2025)
-- ============================================================

-- Q4.1: Impacto no volume de corridas em Manhattan (Jan 2025) - Bar chart
-- Visual: Grouped bar, X=zona_servico, Y=total_corridas, Color=grupo_tipo_transporte
SELECT
    t.grupo_tipo_transporte,
    CASE
        WHEN l.zona_servico IN ('YELLOW ZONE', 'BORO ZONE') THEN 'CBD ZONE (BELOW 60TH)'
        ELSE 'FORA DO CBD'
    END AS regiao_cbd,
    COUNT(*) AS total_corridas
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
JOIN nyc_taxi_gold.dim_local l ON f.id_local_embarque = l.id_local
WHERE f.year = 2025 AND f.month = 1
  AND l.distrito = 'MANHATTAN'
GROUP BY t.grupo_tipo_transporte,
    CASE WHEN l.zona_servico IN ('YELLOW ZONE', 'BORO ZONE') THEN 'CBD ZONE (BELOW 60TH)' ELSE 'FORA DO CBD' END
;

-- Q4.2: Arrecadacao total da taxa de congestionamento - KPI
-- Visual: KPI cards
SELECT
    t.grupo_tipo_transporte,
    ROUND(SUM(f.valor_taxa_congestionamento_cbd), 2) AS total_fee_collected,
    ROUND(AVG(f.valor_taxa_congestionamento_cbd), 2) AS avg_fee_per_ride
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_tipo_transporte t ON f.id_tipo_transporte = t.id_tipo_transporte
WHERE f.year = 2025 AND f.month = 1
  AND f.valor_taxa_congestionamento_cbd > 0
GROUP BY t.grupo_tipo_transporte
;

-- Q4.3: Mudanca na distribuicao de corridas dentro vs fora CBD - Donut/Pie
-- Visual: Donut chart, Segment=regiao, Value=total_corridas
-- Comparar dez/2024 vs jan/2025
SELECT
    CASE WHEN f.year = 2024 AND f.month = 12 THEN 'DEZ-2024'
         WHEN f.year = 2025 AND f.month = 1  THEN 'JAN-2025'
    END AS periodo,
    CASE
        WHEN l.distrito = 'MANHATTAN' THEN 'DENTRO CBD'
        ELSE 'FORA CBD'
    END AS regiao,
    COUNT(*) AS total_corridas
FROM nyc_taxi_gold.fato_corrida f
JOIN nyc_taxi_gold.dim_local l ON f.id_local_embarque = l.id_local
WHERE (f.year = 2024 AND f.month = 12) OR (f.year = 2025 AND f.month = 1)
GROUP BY
    CASE WHEN f.year = 2024 AND f.month = 12 THEN 'DEZ-2024'
         WHEN f.year = 2025 AND f.month = 1  THEN 'JAN-2025' END,
    CASE WHEN l.distrito = 'MANHATTAN' THEN 'DENTRO CBD' ELSE 'FORA CBD' END
;


-- ============================================================
-- FILTROS DO DASHBOARD (parametros no QuickSight)
-- ============================================================
-- Date Range:      filtro em f.ano_mes ou f.year
-- Service Type:    filtro em t.grupo_tipo_transporte
-- Location:        filtro em l.distrito (Borough)
-- Zone:            filtro em l.zona
