SELECT MIN(c_city), MIN(d_date), MIN(s_city)
FROM lineorder, customer, date, supplier
WHERE lo_orderdate = d_datekey
  AND lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND c_nation IN ('JORDAN', 'SAUDI ARABIA')
  AND c_mktsegment = 'AUTOMOBILE'
  AND d_monthnuminyear BETWEEN 5 AND 12
  AND d_year BETWEEN 1994 AND 1997
  AND s_address NOT LIKE '%nXCDRcfr%';
