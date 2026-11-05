### Q3 - Word count on FW-1, with and without a combiner

                    Variant Time (s) Map output records Combine output records             Reduce shuffle bytes
   MapReduce, with combiner     69.1        540,092,071             25,812,805                      181,336,643
MapReduce, without combiner    122.0        540,092,071                      0                    8,474,198,518
         Spark, reduceByKey     34.7                  -                      -            see spark_shuffle.txt
  Spark, groupByKey (1 run)     55.5                  -                      - see spark_shuffle_groupbykey.txt

Times are means of 3 runs unless marked; counters are from the first run of each variant.
