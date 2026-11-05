### Q8 - Spark on FW-8, reading TSV vs Parquet

Stage TSV time (s) Parquet time (s) Gain TSV input Parquet input
   B1         87.8             93.7  -7%  27.03 GB      16.62 GB
   B4        152.3            139.1   9%  54.48 GB      44.07 GB

Means of 3 runs. Input = bytes read, from the Spark UI (input_bytes.txt).
