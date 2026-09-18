-- Temporary view é lógica intermediária do pipeline, não dado publicado

-- Aqui só existe fato verificável: se o código está ou não está no cadastro

CREATE TEMPORARY VIEW vra_marcado AS
WITH aerodromo AS (
    SELECT DISTINCT codigo_oaci FROM voebem.bronze.aerodromos
    WHERE codigo_oaci IS NOT NULL AND codigo_oaci <> ''
),
empresa AS(
    SELECT DISTINCT icao FROM voebem.bronze.empresas_aereas_nacionais
    WHERE icao IS NOT NULL AND icao <> ''
    UNION
    SELECT DISTINCT icao FROM voebem.bronze.empresas_aereas_estrangeiras
    WHERE icao IS NOT NULL AND icao <> ''
)

SELECT
    v.*,
    (ao.codigo_oaci IS NOT NULL) AS origem_no_cadastro,
    (ad.codigo_oaci IS NOT NULL) AS destino_no_cadastro,
    (em.icao IS NOT NULL) AS empresa_no_cadastro,
    -- cálculo do atraso em minutos (diferença real e previsto)
    CASE 
        WHEN v.partida_real IS NOT NULL AND v.partida_prevista IS NOT NULL
        THEN CAST ((unix_timestamp(v.partida_real) - unix_timestamp(v.partida_prevista))/60 AS INT)
    END AS atraso_partida_min,
    CASE 
        WHEN v.chegada_real IS NOT NULL AND v.chegada_prevista IS NOT NULL
        THEN CAST ((unix_timestamp(v.chegada_real) - unix_timestamp(v.chegada_prevista))/60 AS INT)
    END AS atraso_chegada_min
    FROM voebem.bronze.vra v
    LEFT JOIN aerodromo ao ON v.icao_aerodromo_origem = ao.codigo_oaci
    LEFT JOIN aerodromo ad ON v.icao_aerodromo_destino = ad.codigo_oaci
    LEFT JOIN empresa em ON v.icao_empresa_aerea = em.icao
