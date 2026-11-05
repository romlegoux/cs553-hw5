SET temp_directory = '@TMP@';
COPY (
    SELECT lower(w) AS word, count(*) AS n
    FROM (SELECT unnest(regexp_extract_all(text, '[A-Za-z]+')) AS w
          FROM read_parquet(@FILES@))
    GROUP BY 1
) TO '@OUT@/b1.tsv' (FORMAT csv, DELIMITER '\t', HEADER false);
