### Table 4e - Scaling of B2 on cluster B

 Workers (N) Strong, FW-2 MapReduce (s) Strong, FW-2 Spark (s) Weak, N files MapReduce (s) Weak, N files Spark (s)
           1                      284.8                  178.9                       171.2                    88.7
           2                      172.6                  106.1                       172.6                   106.1
           4                      106.5                   85.3                       240.7                   178.0
           8                       93.3                   81.0                       264.8                   206.6

Strong scaling uses FW-2 (first 2 files) instead of FW-8: with FW-8 the workers' disks filled up at N=4 (see part4e/clusterB/fw8_diskfull). Weak-scaling points N=2 and N=8 reuse runs made under the same conditions: N=2 is the strong-scaling run (2 files, 2 workers), N=8 is B2 on FW-8 from Table 4c.
