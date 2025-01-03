-- 查詢每個出發機場的航班數量，並顯示航班數量最多的前 5 個機場
SELECT 
  ORIGIN,               -- 提取出發機場代碼
  COUNT(*) AS num_flights  -- 統計每個機場的航班總數，命名為 num_flights
FROM 
  dsongcp.flights       -- 資料來源表：包含所有航班數據的表格
GROUP BY 
  ORIGIN                -- 按出發機場分組，確保每個機場統計一次
ORDER BY 
  num_flights DESC      -- 按航班數量由多到少排序
LIMIT 
  5                     -- 只顯示航班數量最多的前 5 個機場
