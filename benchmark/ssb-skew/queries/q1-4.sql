SELECT MIN(c_city), MIN(d_date)
FROM lineorder, customer, date
WHERE lo_orderdate = d_datekey
    AND lo_custkey = c_custkey
    AND c_name IN ('Customer#000000002', 'Customer#000000006')
    AND d_month IN ('January', 'May', 'July', 'September', 'November');
