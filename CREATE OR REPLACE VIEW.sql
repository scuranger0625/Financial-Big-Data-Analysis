-- 創建或替換一個名為 dsongcp.flights 的視圖 (View)
-- 該視圖基於 dsongcp.flights_raw 表進行轉換，選擇需要的欄位並進行處理
CREATE OR REPLACE VIEW dsongcp.flights AS
SELECT
  FlightDate AS FL_DATE,                  -- 將航班日期 (FlightDate) 重命名為 FL_DATE
  Reporting_Airline AS UNIQUE_CARRIER,   -- 將航空公司代碼 (Reporting_Airline) 重命名為 UNIQUE_CARRIER
  OriginAirportSeqID AS ORIGIN_AIRPORT_SEQ_ID,  -- 出發機場的唯一序列 ID
  Origin AS ORIGIN,                      -- 出發機場代碼
  DestAirportSeqID AS DEST_AIRPORT_SEQ_ID, -- 目的地機場的唯一序列 ID
  Dest AS DEST,                          -- 目的地機場代碼
  CRSDepTime AS CRS_DEP_TIME,            -- 計劃出發時間 (CRSDepTime)
  DepTime AS DEP_TIME,                   -- 實際出發時間 (DepTime)
  CAST(DepDelay AS FLOAT64) AS DEP_DELAY, -- 將出發延遲時間 (DepDelay) 轉換為 FLOAT64 型別並重命名為 DEP_DELAY
  CAST(TaxiOut AS FLOAT64) AS TAXI_OUT,  -- 將滑行到跑道時間 (TaxiOut) 轉換為 FLOAT64 型別並重命名為 TAXI_OUT
  WheelsOff AS WHEELS_OFF,               -- 航班起飛時間 (WheelsOff)
  WheelsOn AS WHEELS_ON,                 -- 航班著陸時間 (WheelsOn)
  CAST(TaxiIn AS FLOAT64) AS TAXI_IN,    -- 將滑行到停機位時間 (TaxiIn) 轉換為 FLOAT64 型別並重命名為 TAXI_IN
  CRSArrTime AS CRS_ARR_TIME,            -- 計劃到達時間 (CRSArrTime)
  ArrTime AS ARR_TIME,                   -- 實際到達時間 (ArrTime)
  CAST(ArrDelay AS FLOAT64) AS ARR_DELAY, -- 將到達延遲時間 (ArrDelay) 轉換為 FLOAT64 型別並重命名為 ARR_DELAY
  IF(Cancelled = '1.00', True, False) AS CANCELLED, -- 如果 Cancelled 的值是 '1.00'，將其轉換為布林值 True (取消)，否則為 False
  IF(Diverted = '1.00', True, False) AS DIVERTED,   -- 如果 Diverted 的值是 '1.00'，將其轉換為布林值 True (備降)，否則為 False
  DISTANCE                              -- 航班距離 (DISTANCE)
FROM
  dsongcp.flights_raw;                  -- 資料來源是原始的 flights_raw 表
