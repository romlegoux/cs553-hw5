package edu.iit.cs553.hw5;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class B1WordCountMR extends Configured implements Tool {

    // for the report: total words, sum of token_count
    public enum Stats { TOTAL_WORDS, TOKENS }

    public static class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text word = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text line, Context ctx)
                throws IOException, InterruptedException {
            // id, dump, url, token_count, text
            String[] f = line.toString().split("\t", 5);
            ctx.getCounter(Stats.TOKENS).increment(Long.parseLong(f[3]));

            // words = runs of ASCII letters, lowercased
            long n = 0;
            for (String w : f[4].split("[^A-Za-z]+")) {
                if (w.isEmpty()) continue; 
                word.set(w.toLowerCase());
                ctx.write(word, one);
                n++;
            }
            ctx.getCounter(Stats.TOTAL_WORDS).increment(n);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // args: input, output, nb of reducers
        Job job = Job.getInstance(getConf(), "B1WordCountMR");
        job.setJarByClass(B1WordCountMR.class);
        job.setMapperClass(WordMapper.class);

        if (getConf().getBoolean("wc.combiner", true)) job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new B1WordCountMR(), args));
    }
}
