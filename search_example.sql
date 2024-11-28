SELECT 
FORMAT("%s-%02d-%02d",
 Year,
 CAST(Month AS INT64),
 CAST(DayofMonth AS INT64)) AS resurrect,
 FlightDate,
 CAST(EXTRACT(YEAR FROM FlightDate) AS INT64) AS ex_year,
 CAST(EXTRACT(MONTH FROM FlightDate) AS INT64) AS ex_month,
 CAST(EXTRACT(DAY FROM FlightDate) AS INT64) AS ex_day,
 FROM dsongcp.flights_raw
 LIMIT 5
