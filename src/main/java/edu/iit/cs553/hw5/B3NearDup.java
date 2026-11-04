package edu.iit.cs553.hw5;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class B3NearDup {

    static final long P = (1L << 61) - 1;
    static final int K = 64, BANDS = 8, ROWS = 8;
    static final long[] A = new long[K], B = new long[K];

    // random a_i, b_i from fixed seed -> same values on every executor
    static {
        Random r = new Random(42);
        for (int i = 0; i < K; i++) {
            A[i] = 1 + Long.remainderUnsigned(r.nextLong(), P - 1);
            B[i] = Long.remainderUnsigned(r.nextLong(), P);
        }
    }

    // (a_i * x + b_i) mod P without overflow, uses 2^61 = 1 mod P
    static long h(int i, long x) {
        long hi = Math.multiplyHigh(A[i], x), lo = A[i] * x;
        long r = ((hi << 3) | (lo >>> 61)) + (lo & P);
        r = (r & P) + (r >>> 61);
        if (r >= P) r -= P;
        r += B[i];
        return r >= P ? r - P : r;
    }

    // min over 5-word shingles of h_i(x), x = first 8 bytes of SHA-1
    static long[] signature(String text) {
        List<String> w = new ArrayList<>();
        for (String s : text.split("[^A-Za-z]+"))
            if (!s.isEmpty()) w.add(s.toLowerCase());
        long[] sig = new long[K];
        Arrays.fill(sig, Long.MAX_VALUE);
        for (int j = 0; j + 5 <= w.size(); j++) {
            String sh = String.join(" ", w.subList(j, j + 5));
            long x = Long.remainderUnsigned(ByteBuffer.wrap(DigestUtils.sha1(sh)).getLong(), P);
            for (int i = 0; i < K; i++) sig[i] = Math.min(sig[i], h(i, x));
        }
        return sig;
    }

    // band b (8 values) -> 64-bit bucket id
    static long bucket(long[] sig, int b) {
        long x = b + 1;
        for (int i = b * ROWS; i < (b + 1) * ROWS; i++) x = (x ^ sig[i]) * 0x9E3779B97F4A7C15L;
        return x ^ (x >>> 32);
    }

    // estimated Jaccard >= 0.8
    static boolean similar(long[] a, long[] b) {
        int same = 0;
        for (int i = 0; i < K; i++) if (a[i] == b[i]) same++;
        return (double) same / K >= 0.8;
    }

    public static void main(String[] args) {
        // args: input (B2 TSV), output (parquet), nb of partitions
        SparkSession spark = SparkSession.builder().appName("B3NearDup")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // smaller shuffle
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        int parts = Integer.parseInt(args[2]);

        // id -> TSV line
        JavaPairRDD<String, String> docs = sc.textFile(args[0])
                .mapToPair(l -> new Tuple2<>(l.split("\t", 2)[0], l));

        // id -> signature, computed once
        JavaPairRDD<String, long[]> sigs = docs
                .mapValues(l -> signature(l.split("\t", 5)[4]))
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        // (bucket, id) for the 8 bands: no signature in this shuffle
        JavaPairRDD<Long, String> members = sigs.flatMapToPair(s -> {
            List<Tuple2<Long, String>> out = new ArrayList<>();
            for (int b = 0; b < BANDS; b++) out.add(new Tuple2<>(bucket(s._2, b), s._1));
            return out.iterator();
        });

        // smallest id of each bucket
        JavaPairRDD<Long, String> reps = members.reduceByKey((a, b) -> a.compareTo(b) <= 0 ? a : b, parts);

        // candidate pairs (doc, rep), doc != rep
        JavaPairRDD<String, String> pairs = members.join(reps, parts).values()
                .filter(t -> !t._1.equals(t._2))
                .mapToPair(t -> t)
                .distinct();

                // (doc, rep) pairs that are near-duplicates
        JavaPairRDD<String, String> nearPairs = pairs.join(sigs)
                .mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)))
                .join(sigs)
                .filter(t -> similar(t._2._1._2, t._2._2))
                .mapToPair(t -> new Tuple2<>(t._2._1._1, t._1))
                .cache();
        // Q7: optional 4th arg -> save the pairs as "doc TAB rep"
        if (args.length > 3) nearPairs.map(t -> t._1 + "\t" + t._2).saveAsTextFile(args[3]);
        JavaRDD<String> marked = nearPairs.keys().distinct().cache();
        // remove marked docs, write parquet with the 5 fields
        JavaRDD<Row> rows = docs.subtractByKey(marked.mapToPair(id -> new Tuple2<>(id, true)))
                .values()
                .map(l -> {
                    String[] f = l.split("\t", 5);
                    return RowFactory.create(f[0], f[1], f[2], Long.parseLong(f[3]), f[4]);
                });
        StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("dump", DataTypes.StringType)
                .add("url", DataTypes.StringType)
                .add("token_count", DataTypes.LongType)
                .add("text", DataTypes.StringType);
        spark.createDataFrame(rows, schema).write().parquet(args[1]);

        System.out.println("NEAR_DUPS=" + marked.count());
        spark.stop();
    }
}

