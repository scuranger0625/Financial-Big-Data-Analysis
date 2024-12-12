-- 建立一個名為 contingency_table 的虛擬表
WITH contingency_table AS (
  SELECT 
    THRESH, -- 延遲閾值
    -- 計算真陰性（true_negatives）：出發延遲小於閾值且到達延遲小於15分鐘
    COUNTIF(dep_delay < THRESH AND arr_delay < 15) AS true_negatives,
    -- 計算假陰性（false_negatives）：出發延遲小於閾值且到達延遲大於等於15分鐘
    COUNTIF(dep_delay < THRESH AND arr_delay >= 15) AS false_negatives,
    -- 計算假陽性（false_positives）：出發延遲大於等於閾值且到達延遲小於15分鐘
    COUNTIF(dep_delay >= THRESH AND arr_delay < 15) AS false_positives,
    -- 計算真陽性（true_positives）：出發延遲大於等於閾值且到達延遲大於等於15分鐘
    COUNTIF(dep_delay >= THRESH AND arr_delay >= 15) AS true_positives,
    COUNT(*) AS total -- 計算總數
  FROM 
    dsongcp.flights, 
    UNNEST([5, 10, 11, 12, 13, 15, 20]) AS THRESH -- 展開不同的延遲閾值
  WHERE 
    arr_delay IS NOT NULL AND dep_delay IS NOT NULL -- 過濾掉到達延遲或出發延遲為空的記錄
  GROUP BY 
    THRESH -- 根據閾值分組
)
SELECT
  -- 計算準確率（accuracy）
  ROUND((true_positives + true_negatives) / total, 2) AS accuracy,
  -- 計算假陽性率（false positive rate, FPR）
  ROUND(false_positives / (true_positives + false_positives), 2) AS fpr,
  -- 計算假陰性率（false negative rate, FNR）
  ROUND(false_negatives / (false_negatives + true_negatives), 2) AS fnr,
  * -- 選擇所有欄位
FROM 
  contingency_table
