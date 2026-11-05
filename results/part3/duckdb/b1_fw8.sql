SET temp_directory = '/home/ubuntu/duck/tmp';
COPY (
    SELECT lower(w) AS word, count(*) AS n
    FROM (SELECT unnest(regexp_extract_all(text, '[A-Za-z]+')) AS w
          FROM read_parquet(['/home/ubuntu/fineweb/000.parquet','/home/ubuntu/fineweb/001.parquet','/home/ubuntu/fineweb/002.parquet','/home/ubuntu/fineweb/003.parquet','/home/ubuntu/fineweb/004.parquet','/home/ubuntu/fineweb/005.parquet','/home/ubuntu/fineweb/006.parquet','/home/ubuntu/fineweb/007.parquet']))
    GROUP BY 1
) TO '/home/ubuntu/duck/out/fw8/b1.tsv' (FORMAT csv, DELIMITER '\t', HEADER false);
