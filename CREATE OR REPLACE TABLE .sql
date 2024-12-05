-- 創建或替換一個名為 dsongcp.flights2 的資料表 (Table)
-- 該表基於 dsongcp.flights_raw 表進行轉換，提取所需欄位並進行預處理，生成一個靜態的資料副本
CREATE OR REPLACE TABLE dsongcp.flights2 AS
SELECT
  FlightDate AS FL_DATE,                  -- 將航班日期欄位重命名為 FL_DATE，用於表示航班的日期
  Reporting_Airline AS UNIQUE_CARRIER,   -- 將航空公司代碼欄位重命名為 UNIQUE_CARRIER，用於唯一標識航空公司
  OriginAirportSeqID AS ORIGIN_AIRPORT_SEQ_ID,  -- 提取出發機場的唯一序列 ID，表示出發機場的唯一識別碼
  Origin AS ORIGIN,                      -- 出發機場代碼，用於表示航班的起點
  DestAirportSeqID AS DEST_AIRPORT_SEQ_ID, -- 提取目的地機場的唯一序列 ID，表示目的地機場的唯一識別碼
  Dest AS DEST,                          -- 目的地機場代碼，用於表示航班的終點
  CRSDepTime AS CRS_DEP_TIME,            -- 計劃出發時間，用於表示航班的計劃出發時刻
  DepTime AS DEP_TIME,                   -- 實際出發時間，用於表示航班的實際出發時刻
  CAST(DepDelay AS FLOAT64) AS DEP_DELAY, -- 將出發延遲時間轉換為 FLOAT64 型別，並重命名為 DEP_DELAY，用於記錄延遲的分鐘數
  CAST(TaxiOut AS FLOAT64) AS TAXI_OUT,  -- 將滑行到跑道時間轉換為 FLOAT64 型別，並重命名為 TAXI_OUT，用於表示滑行時間（分鐘）
  WheelsOff AS WHEELS_OFF,               -- 航班實際起飛時間，用於記錄輪胎離地的時刻
  WheelsOn AS WHEELS_ON,                 -- 航班實際著陸時間，用於記錄輪胎著地的時刻
  CAST(TaxiIn AS FLOAT64) AS TAXI_IN,    -- 將滑行到停機位時間轉換為 FLOAT64 型別，並重命名為 TAXI_IN，用於記錄著陸後的滑行時間
  CRSArrTime AS CRS_ARR_TIME,            -- 計劃到達時間，用於表示航班的預計到達時刻
  ArrTime AS ARR_TIME,                   -- 實際到達時間，用於記錄航班的實際到達時刻
  CAST(ArrDelay AS FLOAT64) AS ARR_DELAY, -- 將到達延遲時間轉換為 FLOAT64 型別，並重命名為 ARR_DELAY，用於記錄到達延遲的分鐘數
  IF(Cancelled = '1.00', True, False) AS CANCELLED, -- 如果 Cancelled 的值為 '1.00'，則表示航班取消，轉換為布林值 True，否則為 False
  IF(Diverted = '1.00', True, False) AS DIVERTED,   -- 如果 Diverted 的值為 '1.00'，則表示航班備降，轉換為布林值 True，否則為 False
  DISTANCE                              -- 提取航班距離，用於表示航班的飛行總距離
FROM
  dsongcp.flights_raw;                  -- 資料來源為原始航班數據表 flights_raw
