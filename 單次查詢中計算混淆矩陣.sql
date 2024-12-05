SELECT 
  COUNTIF(dep_delay < 15 AND arr_delay < 15) AS true_negatives, -- 真負例：出發延誤小於15且到達延誤小於15
  COUNTIF(dep_delay < 15 AND arr_delay >= 15) AS false_negatives, -- 假負例：出發延誤小於15但到達延誤大於等於15
  COUNTIF(dep_delay >= 15 AND arr_delay < 15) AS false_positives, -- 假正例：出發延誤大於等於15但到達延誤小於15
  COUNTIF(dep_delay >= 15 AND arr_delay >= 15) AS true_positives, -- 真正例：出發延誤大於等於15且到達延誤大於等於15
  COUNT(*) AS total -- 總記錄數：所有航班記錄的總數
FROM 
  dsongcp.flights -- 數據來源表
WHERE 
  arr_delay IS NOT NULL AND dep_delay IS NOT NULL -- 過濾掉出發延誤和到達延誤為 NULL 的記錄
