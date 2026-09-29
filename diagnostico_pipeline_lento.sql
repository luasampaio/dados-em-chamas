-- ============================================================
-- DIAGNÓSTICO DE PIPELINES LENTOS — Databricks System Tables
-- Catálogo alvo: ajustar conforme ambiente
--   bs_dbricks_catalog_gov_pr (produção) ou
--   bs_dbricks_catalog_gov_tu (governança/tuning)
-- ============================================================

-- ------------------------------------------------------------
-- 1) EVOLUÇÃO DE DURAÇÃO POR JOB — detecta degradação ao longo do tempo
--    Use pra achar jobs que estão ficando cada vez mais lentos,
--    não só o mais lento em termos absolutos.
-- ------------------------------------------------------------
WITH v_runs AS (
  SELECT
    job_id                                        AS c_job_id,
    run_id                                        AS c_run_id,
    period_start_time                             AS d_inicio,
    period_end_time                               AS d_fim,
    result_state                                  AS r_status,
    trigger_type                                  AS r_trigger,
    DATEDIFF(SECOND, period_start_time, period_end_time) AS v_duracao_seg
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
)
SELECT
  c_job_id,
  DATE_TRUNC('DAY', d_inicio)                     AS d_dia,
  COUNT(*)                                        AS v_qtd_execucoes,
  ROUND(AVG(v_duracao_seg) / 60.0, 1)             AS v_duracao_media_min,
  ROUND(MAX(v_duracao_seg) / 60.0, 1)             AS v_duracao_max_min,
  ROUND(PERCENTILE(v_duracao_seg, 0.95) / 60.0, 1) AS v_p95_min
FROM v_runs
GROUP BY c_job_id, DATE_TRUNC('DAY', d_inicio)
ORDER BY c_job_id, d_dia;

-- ------------------------------------------------------------
-- 2) TOP TASKS MAIS LENTAS DENTRO DE UM JOB ESPECÍFICO
--    Preencher c_job_id abaixo. Ajuda a achar qual task
--    do pipeline (bronze/silver/gold) é o gargalo.
-- ------------------------------------------------------------
SELECT
  job_id                                          AS c_job_id,
  run_id                                          AS c_run_id,
  task_key                                        AS r_task,
  period_start_time                               AS d_inicio,
  DATEDIFF(SECOND, period_start_time, period_end_time) AS v_duracao_seg,
  result_state                                    AS r_status
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = :c_job_id  -- <<< preencher
  AND period_start_time >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY v_duracao_seg DESC
LIMIT 20;

-- ------------------------------------------------------------
-- 3) QUERIES SQL MAIS LENTAS (warehouses SQL / DBSQL)
--    Traz spill, bytes lidos, shuffle — direto do
--    system.query.history (equivalente ao Query Profile, agregado)
-- ------------------------------------------------------------
SELECT
  statement_id                                    AS c_query_id,
  executed_by                                     AS r_usuario,
  warehouse_id                                    AS c_warehouse,
  start_time                                      AS d_inicio,
  total_duration_ms / 1000.0                       AS v_duracao_seg,
  read_bytes / POWER(1024, 3)                      AS v_gb_lidos,
  spilled_local_bytes / POWER(1024, 3)             AS v_gb_spill,
  shuffle_read_bytes / POWER(1024, 3)              AS v_gb_shuffle,
  LEFT(statement_text, 200)                        AS r_query_preview
FROM system.query.history
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
  AND total_duration_ms > 60000  -- acima de 1 min
ORDER BY total_duration_ms DESC
LIMIT 20;

-- ------------------------------------------------------------
-- 4) SPILL — sinal direto de subdimensionamento de memória
--    Se v_gb_spill > 0, o executor não tinha memória suficiente
--    pro shuffle/sort e escreveu em disco (muito mais lento).
-- ------------------------------------------------------------
SELECT
  statement_id                                    AS c_query_id,
  executed_by                                     AS r_usuario,
  warehouse_id                                    AS c_warehouse,
  start_time                                      AS d_inicio,
  ROUND(spilled_local_bytes / POWER(1024, 3), 2)   AS v_gb_spill,
  ROUND(total_duration_ms / 1000.0, 1)             AS v_duracao_seg,
  LEFT(statement_text, 200)                        AS r_query_preview
FROM system.query.history
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
  AND spilled_local_bytes > 0
ORDER BY spilled_local_bytes DESC
LIMIT 20;

-- ------------------------------------------------------------
-- 5) CLUSTERS COM AUTOSCALING LENTO / SUBDIMENSIONADOS
--    Cruza tempo de execução do job com o tamanho do cluster
--    no período — ajuda a ver se o cluster escalou tarde.
-- ------------------------------------------------------------
SELECT
  cn.cluster_id                                   AS c_cluster,
  cn.start_time                                   AS d_inicio_no,
  cn.end_time                                     AS d_fim_no,
  cn.node_type                                    AS r_tipo_no,
  cn.driver                                       AS i_eh_driver
FROM system.compute.node_timeline cn
WHERE cn.cluster_id = :c_cluster_id  -- <<< preencher com cluster do job lento
  AND cn.start_time >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY cn.start_time;

-- ------------------------------------------------------------
-- 6) TABELAS COM MUITOS ARQUIVOS PEQUENOS (small files problem)
--    Requer DESCRIBE DETAIL por tabela — não é system table,
--    mas encaixa aqui como checagem complementar.
-- ------------------------------------------------------------
-- DESCRIBE DETAIL bs_dbricks_catalog_gov_tu.silver.sua_tabela;
-- Olhar numFiles vs sizeInBytes: se numFiles alto e arquivos
-- médios < 128MB, rodar OPTIMIZE.
