### Speedup and efficiency (for Q5)

Scaling Framework  N  Mean (s) Speedup  Efficiency                 Source
 strong MapReduce  1     284.8     1.0        1.00            strong runs
 strong MapReduce  2     172.6    1.65        0.82            strong runs
 strong MapReduce  4     106.5    2.67        0.67            strong runs
 strong MapReduce  8      93.3    3.05        0.38            strong runs
 strong     Spark  1     178.9     1.0        1.00            strong runs
 strong     Spark  2     106.1    1.69        0.84            strong runs
 strong     Spark  4      85.3     2.1        0.52            strong runs
 strong     Spark  8      81.0    2.21        0.28            strong runs
   weak MapReduce  1     171.2       -        1.00              weak runs
   weak MapReduce  2     172.6       -        0.99      = strong run, N=2
   weak MapReduce  4     240.7       -        0.71              weak runs
   weak MapReduce  8     264.8       -        0.65 = Table 4c, B2 on FW-8
   weak     Spark  1      88.7       -        1.00              weak runs
   weak     Spark  2     106.1       -        0.84      = strong run, N=2
   weak     Spark  4     178.0       -        0.50              weak runs
   weak     Spark  8     206.6       -        0.43 = Table 4c, B2 on FW-8

Strong: speedup S(N) = T(1)/T(N), efficiency = S(N)/N. Weak: efficiency = T(1)/T(N).
