SELECT MIN(c_city), MIN(d_date), MIN(s_city)
FROM lineorder, customer, date, supplier
WHERE lo_orderdate = d_datekey
  AND lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND c_nation IN ('JORDAN', 'SAUDI ARABIA')
  AND d_monthnuminyear BETWEEN 3 AND 10
  AND d_year IN (1992, 1993, 1994, 1997, 1998)
  AND s_nation = 'PERU';

