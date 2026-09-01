-- Prediction-quality alert for the caught-in-air Inference monitor.
--
-- Returns the most recent window's model accuracy. Wire a Databricks SQL Alert to this
-- query and trigger when accuracy falls below your threshold (e.g. < 0.85). In this demo
-- it fires once caught_v2 takes over: accuracy drops from ~0.94 (caught_v1) to ~0.76.
--
-- Setup:
--   1. SQL editor -> paste this query -> set the `catalog` / `schema` parameters
--      (defaults main / dqm) to where the monitor's metric tables live.
--   2. Save the query, then create an Alert on it:
--        Column:    latest_accuracy
--        Condition: is below
--        Threshold: 0.85
--   3. Schedule it (e.g. daily) and add a destination (email / Slack / webhook).
--
-- Requires the Inference monitor to have been created and refreshed at least once.
-- Metric-table column names can vary slightly by monitor version; adjust if needed.

SELECT min(p.accuracy_score) AS latest_accuracy
FROM {{catalog}}.{{schema}}.caught_in_air_inference_profile_metrics AS p
WHERE p.column_name = ':table'          -- table-level rows carry the model-quality metrics
  AND p.slice_key IS NULL               -- overall, not a sliced subset
  AND p.window.start = (
    SELECT max(p2.window.start)
    FROM {{catalog}}.{{schema}}.caught_in_air_inference_profile_metrics AS p2
    WHERE p2.column_name = ':table'
      AND p2.slice_key IS NULL
  );

-- ---------------------------------------------------------------------------
-- Alternative: alert on prediction drift instead of accuracy.
-- Fires when the predicted_caught distribution shifts window-over-window
-- (Jensen-Shannon distance). Trigger when predicted_caught_js_distance is above ~0.10.
--
-- SELECT max(d.js_distance) AS predicted_caught_js_distance
-- FROM {{catalog}}.{{schema}}.caught_in_air_inference_drift_metrics AS d
-- WHERE d.column_name = 'predicted_caught'
--   AND d.drift_type = 'CONSECUTIVE'
--   AND d.window.start = (
--     SELECT max(d2.window.start)
--     FROM {{catalog}}.{{schema}}.caught_in_air_inference_drift_metrics AS d2
--     WHERE d2.column_name = 'predicted_caught'
--       AND d2.drift_type = 'CONSECUTIVE'
--   );
