SET temp_directory = '/home/ubuntu/duck/tmp';
COPY (
    SELECT lower(w) AS word, count(*) AS n
    FROM (SELECT unnest(regexp_extract_all(text, '[A-Za-z]+')) AS w
          FROM read_parquet(['/home/ubuntu/fineweb/000.parquet']))
    GROUP BY 1
) TO '/home/ubuntu/duck/out/fw1/b1.tsv' (FORMAT csv, DELIMITER '\t', HEADER false);
