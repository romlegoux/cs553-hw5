package edu.iit.cs553.hw5;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

public class B4Report {


    static Dataset<Row> tsv(SparkSession spark, String path) {
        Column f = split(col("value"), "\t", 5);
        return spark.read().text(path).select(
                f.getItem(0).as("id"), f.getItem(1).as("dump"), f.getItem(2).as("url"),
                f.getItem(3).cast("long").as("token_count"), f.getItem(4).as("text"));
    }

    // one funnel line: step, stage, docs, tokens
    static Dataset<Row> stage(Dataset<Row> d, int step, String name) {
        return d.agg(count("*").as("docs"), sum("token_count").as("tokens"))
                .select(lit(step).as("step"), lit(name).as("stage"), col("docs"), col("tokens"));
    }

    static void save(Dataset<Row> d, String path) {
        d.coalesce(1).write().option("header", true).csv(path);
    }

    public static void main(String[] args) {
        // args: B0 tsv, B2 tsv, B3 parquet, output dir
        SparkSession spark = SparkSession.builder().appName("B4Report").getOrCreate();
        String out = args[3];

        // Q8: the B0 input can be the raw Parquet instead of the TSV
        Dataset<Row> b0 = args[0].contains(".parquet")
                ? spark.read().parquet(args[0]).select("id", "dump", "url", "token_count", "text")
                : tsv(spark, args[0]);
        Dataset<Row> b2 = tsv(spark, args[1]);
        Dataset<Row> b3 = spark.read().parquet(args[2]);

        // same filter as B2
        UserDefinedFunction keep = udf((UDF1<String, Boolean>) B2FilterDedupMR::keep, DataTypes.BooleanType);
        Dataset<Row> filtered = b0.filter(keep.apply(col("text")));

        // 1. funnel
        save(stage(b0, 1, "after B0")
                .union(stage(filtered, 2, "after quality filter"))
                .union(stage(b2, 3, "after exact dedup"))
                .union(stage(b3, 4, "after near-dup (B3)"))
                .orderBy("step"), out + "/funnel");

        // 2. top 20 domains by tokens (host without leading www.)
        save(b3.withColumn("domain", regexp_replace(expr("parse_url(url, 'HOST')"), "^www\\.", ""))
                .groupBy("domain").agg(sum("token_count").as("tokens"))
                .orderBy(desc("tokens")).limit(20), out + "/top_domains");

        // 3. tokens per Common Crawl snapshot
        save(b3.groupBy("dump").agg(count("*").as("docs"), sum("token_count").as("tokens"))
                .orderBy("dump"), out + "/tokens_per_dump");

        spark.stop();
    }
}
