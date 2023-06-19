SELECT MIN(c_city), MIN(d_date)
FROM lineorder, customer, date
WHERE lo_orderdate = d_datekey
  AND lo_custkey = c_custkey
  AND c_nation IN ('JORDAN', 'SAUDI ARABIA')
  AND d_sellingseason IN ('Spring', 'Summer', 'Fall');
