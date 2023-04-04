INSERT INTO lineorder SELECT * FROM 'https://github.com/d-justen/duckdb-polr/releases/download/v1.0/lineorder.tbl';
INSERT INTO customer SELECT * FROM 'https://github.com/d-justen/duckdb-polr/releases/download/v1.0/customer.tbl';
INSERT INTO part SELECT * FROM 'https://github.com/d-justen/duckdb-polr/releases/download/v1.0/part.tbl';
INSERT INTO supplier SELECT * FROM 'https://github.com/d-justen/duckdb-polr/releases/download/v1.0/supplier.tbl';
INSERT INTO date SELECT * FROM 'https://github.com/d-justen/duckdb-polr/releases/download/v1.0/date.tbl';
