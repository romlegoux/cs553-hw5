SET temp_directory = '@TMP@';

CREATE TEMP TABLE m AS
SELECT id,
       nw BETWEEN 50 AND 100000 AND nl BETWEEN 3 * nw AND 10 * nw AS keep,   -- mean length 3..10 without division
       sha256(trim(regexp_replace(
           translate(text, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),  -- ASCII-only lowercase
           '[ \t\r\n]+', ' ', 'g'), ' ')) AS fp
FROM (SELECT id, text,
             len(regexp_extract_all(text, '[A-Za-z]+')) AS nw,
             length(regexp_replace(text, '[^A-Za-z]', '', 'g')) AS nl
      FROM read_parquet(@FILES@));

SELECT count(*) AS input_docs,
       count(*) FILTER (WHERE NOT keep) AS filtered_out,
       count(*) FILTER (WHERE keep) - count(DISTINCT fp) FILTER (WHERE keep) AS duplicates
FROM m;

COPY (
    SELECT id, dump, url, token_count, text
    FROM read_parquet(@FILES@)
    WHERE id IN (SELECT min(id) FROM m WHERE keep GROUP BY fp)
) TO '@OUT@/b2.parquet' (FORMAT parquet);
SELECT count(*) AS output_docs FROM '@OUT@/b2.parquet';
