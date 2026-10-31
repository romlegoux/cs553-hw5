package edu.iit.cs553.hw5;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class B1WordCountSpark {

    public static void main(String[] args) {
        // args: input, output, nb of partitions
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("B1WordCountSpark"));

        // same role as MR counters
        LongAccumulator tokens = sc.sc().longAccumulator("TOKENS");
        LongAccumulator total = sc.sc().longAccumulator("TOTAL_WORDS");

        JavaRDD<String> words = sc.textFile(args[0]).flatMap(line -> {
            // id, dump, url, token_count, text
            String[] f = line.split("\t", 5);
            tokens.add(Long.parseLong(f[3]));
            List<String> out = new ArrayList<>();
            for (String w : f[4].split("[^A-Za-z]+"))
                if (!w.isEmpty()) out.add(w.toLowerCase());
            total.add(out.size());
            return out.iterator();
        });

        // reduceByKey sums inside each partition before the shuffle = combiner
        JavaPairRDD<String, Long> counts = words.mapToPair(w -> new Tuple2<>(w, 1L)).reduceByKey(Long::sum, Integer.parseInt(args[2]));
        counts.map(t -> t._1 + "\t" + t._2).saveAsTextFile(args[1]);

        System.out.println("TOKENS=" + tokens.value() + " TOTAL_WORDS=" + total.value());
        sc.stop();
    }
}
