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
        for (int j = 0; j + 5 <= w.size(); j++) { // >= 50 words after B2, so never empty
            String sh = String.join(" ", w.subList(j, j + 5));
            long x = Long.remainderUnsigned(ByteBuffer.wrap(DigestUtils.sha1(sh)).getLong(), P);
            for (int i = 0; i < K; i++) sig[i] = Math.min(sig[i], h(i, x));
        }
        return sig;
    }

    public static void main(String[] args) {
        // args: input (B2 TSV), output (parquet), nb of partitions
        SparkSession spark = SparkSession.builder().appName("B3NearDup").getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        int parts = Integer.parseInt(args[2]);

        // id -> TSV line
        JavaPairRDD<String, String> docs = sc.textFile(args[0])
                .mapToPair(l -> new Tuple2<>(l.split("\t", 2)[0], l));

        // one (bucket, (id, sig)) per band
        JavaPairRDD<String, Tuple2<String, long[]>> members = docs.flatMapToPair(d -> {
            long[] sig = signature(d._2.split("\t", 5)[4]);
            List<Tuple2<String, Tuple2<String, long[]>>> out = new ArrayList<>();
            for (int b = 0; b < BANDS; b++) {
                String bucket = b + ":" + Arrays.toString(Arrays.copyOfRange(sig, b * ROWS, (b + 1) * ROWS));
                out.add(new Tuple2<>(bucket, new Tuple2<>(d._1, sig)));
            }
            return out.iterator();
        });

        // in each bucket, compare every doc with the smallest id
        JavaRDD<String> marked = members.groupByKey(parts).flatMap(g -> {
            Tuple2<String, long[]> rep = null;
            for (Tuple2<String, long[]> d : g._2)
                if (rep == null || d._1.compareTo(rep._1) < 0) rep = d;
            List<String> out = new ArrayList<>();
            for (Tuple2<String, long[]> d : g._2) {
                if (d._1.equals(rep._1)) continue;
                int same = 0;
                for (int i = 0; i < K; i++) if (d._2[i] == rep._2[i]) same++;
                if ((double) same / K >= 0.8) out.add(d._1); // estimated Jaccard >= 0.8
            }
            return out.iterator();
        }).distinct().cache();

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
