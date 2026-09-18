CREATE OR REFRESH MATERIALIZED VIEW vra_quarentena
COMMENT 'Silver quarentena - espelho diagnóstico dos registros silver.vra que reprovaram em ealgima expectation do contrato de dados, com motivo por registro. Não é filtro: silver.vra permanece com a contagem original. A decisão de excluir ou não cada categoria é de negócio, ou seja, acontece na gold.'
AS
SELECT
    *,
    CASE
        WHEN partida_prevista IS NULL OR chegada_prevista IS NULL THEN 'horarios_previstos_ausentes'
        WHEN situacao_voo NOT IN ('REALIZADO', 'CANCELADO') THEN 'situacao_voo_desconhecida'
        WHEN NOT (partida_prevista IS NULL OR chegada_prevista IS NULL OR chegada_prevista > partida_prevista) THEN 'chegada_prevista_antes_da_partida_prevista'
        WHEN NOT (partida_real IS NULL OR chegada_real IS NULL OR chegada_real > partida_real) THEN 'chegada_real_antes_da_partida_real'
        WHEN atraso_partida_min IS NOT NULL AND atraso_partida_min NOT BETWEEN -120 AND 1440 THEN 'atraso_partida_implausivel'
        WHEN atraso_chegada_min IS NOT NULL AND atraso_chegada_min NOT BETWEEN -120 AND 1440 THEN 'atraso_chegada_implausivel'
        WHEN NOT empresa_no_cadastro THEN 'empresa_fora_do_cadastro_anac'
        WHEN NOT origem_no_cadastro THEN 'aeroporto_origem_fora_do_cadastro_anac'
        WHEN NOT destino_no_cadastro THEN 'aeroporto_destino_fora_do_cadastro_anac'
    END AS motivo_quarentena,
    current_timestamp() AS _quarentenado_em
FROM vra_auditado
WHERE NOT (
    partida_prevista IS NOT NULL AND chegada_prevista IS NOT NULL
    AND situacao_voo IN ('REALIZADO', 'CANCELADO')
    AND (partida_prevista IS NULL OR chegada_prevista IS NULL OR chegada_prevista > partida_prevista)
    AND (partida_real IS NULL OR chegada_real IS NULL OR chegada_real > partida_real)
    AND (atraso_partida_min IS NULL OR atraso_partida_min BETWEEN -120 AND 1440)
    AND (atraso_chegada_min IS NULL OR atraso_chegada_min BETWEEN -120 AND 1440)
    AND empresa_no_cadastro
    AND origem_no_cadastro
    AND destino_no_cadastro
);