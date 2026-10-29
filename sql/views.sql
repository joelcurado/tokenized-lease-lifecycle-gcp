-- BigQuery Standard SQL. Replace ${DATASET} with project.dataset (infra/setup.sh does this).
-- Kept to portable constructs so the same file is exercised against DuckDB in tests.

-- Latest known position of every lease token.
CREATE OR REPLACE VIEW `${DATASET}.positions_latest` AS
SELECT contract_id, token_id, holder, currency, status, outstanding_principal, arrears_amount,
       periods_paid, periods_missed, term_months, version, last_event_id, updated_at
FROM (
  SELECT p.*, ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY version DESC) AS rn
  FROM `${DATASET}.position_history` p
)
WHERE rn = 1;

-- Exposure per token holder, live contracts only.
CREATE OR REPLACE VIEW `${DATASET}.portfolio_by_holder` AS
SELECT holder,
       COUNT(*) AS live_contracts,
       SUM(outstanding_principal) AS outstanding_principal,
       SUM(arrears_amount) AS arrears_amount
FROM `${DATASET}.positions_latest`
WHERE status IN ('ACTIVE', 'ARREARS')
GROUP BY holder;

-- Arrears ageing by number of unpaid periods.
CREATE OR REPLACE VIEW `${DATASET}.arrears_ageing` AS
SELECT CASE WHEN periods_missed = 1 THEN '1 period'
            WHEN periods_missed = 2 THEN '2 periods'
            ELSE '3+ periods' END AS bucket,
       COUNT(*) AS contracts,
       SUM(arrears_amount) AS arrears_amount
FROM `${DATASET}.positions_latest`
WHERE status = 'ARREARS'
GROUP BY 1;

-- Reconciliation control: every applied event must have produced exactly one version step.
-- A healthy system returns zero rows.
CREATE OR REPLACE VIEW `${DATASET}.control_version_vs_events` AS
SELECT p.contract_id, p.version, e.applied_events
FROM `${DATASET}.positions_latest` p
JOIN (SELECT contract_id, COUNT(*) AS applied_events
      FROM `${DATASET}.applied_events` GROUP BY contract_id) e
  ON p.contract_id = e.contract_id
WHERE p.version <> e.applied_events;
