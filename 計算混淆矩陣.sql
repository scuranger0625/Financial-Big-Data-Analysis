-- 計算混淆矩陣中的 "True Negatives"（真正的負例）
-- 目標：找到出發延誤和到達延誤都少於 15 分鐘的航班數量
SELECT 
  COUNT(*) AS true_negatives  -- 計算符合條件的航班總數，命名為 true_negatives
FROM 
  dsongcp.flights            -- 查詢來源表：dsongcp.flights，包含航班數據
WHERE 
  dep_delay < 15             -- 條件 1：出發延誤少於 15 分鐘
  AND arr_delay < 15         -- 條件 2：到達延誤少於 15 分鐘
