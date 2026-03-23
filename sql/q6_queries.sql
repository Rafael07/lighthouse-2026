-- ============================================================================
-- QUESTÃO 6 - DIMENSÃO DE CALENDÁRIO E ANÁLISE DE VENDAS
-- ============================================================================
-- Objetivo: Identificar o dia da semana com PIOR média de vendas,
--           considerando dias sem venda como R$ 0.
--
-- Problema resolvido: O cálculo anterior ignorava dias sem venda,
--                     inflando artificialmente a média.
-- ============================================================================


-- ============================================================================
-- Q6.1 - CRIAÇÃO DA DIMENSÃO DE CALENDÁRIO
-- ============================================================================

-- Remove tabela se existir (para permitir re-execução)
DROP TABLE IF EXISTS dim_calendario;

-- Cria dimensão de calendário com todas as datas do período de vendas
CREATE TABLE dim_calendario AS
WITH periodo AS (
    -- Identifica o período de análise (menor e maior data de venda)
    SELECT 
        MIN(sale_date) AS data_inicio,
        MAX(sale_date) AS data_fim
    FROM vendas_2023_2024
)
SELECT 
    data::date AS data,
    EXTRACT(DOW FROM data)::integer AS dia_semana_num,  -- 0=Domingo, 6=Sábado
    CASE EXTRACT(DOW FROM data)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda-feira'
        WHEN 2 THEN 'Terça-feira'
        WHEN 3 THEN 'Quarta-feira'
        WHEN 4 THEN 'Quinta-feira'
        WHEN 5 THEN 'Sexta-feira'
        WHEN 6 THEN 'Sábado'
    END AS dia_semana_nome,
    EXTRACT(YEAR FROM data)::integer AS ano,
    EXTRACT(MONTH FROM data)::integer AS mes,
    EXTRACT(DAY FROM data)::integer AS dia
FROM 
    periodo,
    generate_series(data_inicio, data_fim, '1 day'::interval) AS data;

-- Verificação da dimensão criada
SELECT 
    'Dimensão de calendário criada' AS status,
    COUNT(*) AS total_dias,
    MIN(data) AS data_inicio,
    MAX(data) AS data_fim
FROM dim_calendario;


-- ============================================================================
-- Q6.1 - ANÁLISE COM LEFT JOIN (QUERY PRINCIPAL)
-- ============================================================================

-- Passo 1: Agregar vendas por dia
WITH vendas_diarias AS (
    SELECT 
        sale_date,
        SUM(total) AS total_dia
    FROM vendas_2023_2024
    GROUP BY sale_date
),

-- Passo 2: LEFT JOIN com calendário (inclui dias sem venda)
calendario_completo AS (
    SELECT 
        c.data,
        c.dia_semana_nome,
        c.dia_semana_num,
        COALESCE(v.total_dia, 0) AS total_dia  -- Dias sem venda = 0
    FROM dim_calendario c
    LEFT JOIN vendas_diarias v ON c.data = v.sale_date
)

-- Passo 3: Calcular média por dia da semana
SELECT 
    dia_semana_nome AS "Dia da Semana",
    COUNT(*) AS "Qtd de Dias",
    SUM(CASE WHEN total_dia = 0 THEN 1 ELSE 0 END) AS "Dias Sem Venda",
    SUM(total_dia) AS "Total Vendas (R$)",
    ROUND(AVG(total_dia)::numeric, 2) AS "Média Vendas (R$)"
FROM calendario_completo
GROUP BY dia_semana_nome, dia_semana_num
ORDER BY "Média Vendas (R$)" ASC;  -- Pior média primeiro


-- ============================================================================
-- Q6.2 - RESPOSTA: DIA COM PIOR MÉDIA (VALIDAÇÃO)
-- ============================================================================

WITH vendas_diarias AS (
    SELECT 
        sale_date,
        SUM(total) AS total_dia
    FROM vendas_2023_2024
    GROUP BY sale_date
),
calendario_completo AS (
    SELECT 
        c.data,
        c.dia_semana_nome,
        c.dia_semana_num,
        COALESCE(v.total_dia, 0) AS total_dia
    FROM dim_calendario c
    LEFT JOIN vendas_diarias v ON c.data = v.sale_date
),
media_por_dia AS (
    SELECT 
        dia_semana_nome,
        dia_semana_num,
        ROUND(AVG(total_dia)::numeric, 2) AS media_vendas
    FROM calendario_completo
    GROUP BY dia_semana_nome, dia_semana_num
)
SELECT 
    dia_semana_nome AS "Dia com Pior Média",
    media_vendas AS "Média de Vendas (R$)"
FROM media_por_dia
ORDER BY media_vendas ASC
LIMIT 1;


-- ============================================================================
-- ANÁLISE COMPLEMENTAR: COMPARATIVO COM/SEM DIAS ZERADOS
-- ============================================================================
-- Esta query mostra a diferença entre o cálculo correto e o errado

WITH vendas_diarias AS (
    SELECT 
        sale_date,
        SUM(total) AS total_dia
    FROM vendas_2023_2024
    GROUP BY sale_date
),

-- Método CORRETO: considera todos os dias do calendário
metodo_correto AS (
    SELECT 
        c.dia_semana_nome,
        c.dia_semana_num,
        ROUND(AVG(COALESCE(v.total_dia, 0)::numeric), 2) AS media_correta
    FROM dim_calendario c
    LEFT JOIN vendas_diarias v ON c.data = v.sale_date
    GROUP BY c.dia_semana_nome, c.dia_semana_num
),

-- Método ERRADO (do estagiário): considera apenas dias com venda
metodo_errado AS (
    SELECT 
        c.dia_semana_nome,
        c.dia_semana_num,
        ROUND(AVG(v.total_dia)::numeric, 2) AS media_errada
    FROM dim_calendario c
    INNER JOIN vendas_diarias v ON c.data = v.sale_date
    GROUP BY c.dia_semana_nome, c.dia_semana_num
)

SELECT 
    c.dia_semana_nome AS "Dia da Semana",
    e.media_errada AS "Média ERRADA (R$)",
    c.media_correta AS "Média CORRETA (R$)",
    ROUND(e.media_errada - c.media_correta, 2) AS "Diferença (R$)",
    ROUND(((e.media_errada - c.media_correta) / c.media_correta) * 100, 2) AS "Inflação (%)"
FROM metodo_correto c
JOIN metodo_errado e ON c.dia_semana_num = e.dia_semana_num
ORDER BY c.dia_semana_num;