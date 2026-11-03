package edu.iit.cs553.hw5;
import java.io.IOException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class B2FilterDedupMR extends Configured implements Tool {

    public enum Stats { FILTERED_OUT, DUPLICATES }

    // Gopher rules
    static boolean keep(String text) {
        long n = 0, len = 0;
        for (String w : text.split("[^A-Za-z]+"))
            if (!w.isEmpty()) { n++; len += w.length(); }
        double mean = (double) len / n;
        return n >= 50 && n <= 100000 && mean >= 3 && mean <= 10;
    }

    static String normalize(String text) {
        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray())
            sb.append(c >= 'A' && c <= 'Z' ? (char) (c + 32) : c);
        return sb.toString().replaceAll(" +", " ").replaceAll("^ | $", "");
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text line, Context ctx)
                throws IOException, InterruptedException {
            String text = line.toString().split("\t", 5)[4];
            if (!keep(text)) {
                ctx.getCounter(Stats.FILTERED_OUT).increment(1);
                return;
            }
            // same fingerprint -> same reducer
            ctx.write(new Text(DigestUtils.sha256Hex(normalize(text))), line);
        }
    }

    public static class DedupReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text fp, Iterable<Text> lines, Context ctx)
                throws IOException, InterruptedException {
            String best = null;
            long n = 0;
            for (Text t : lines) {
                String l = t.toString(); // copy, Hadoop reuses the Text object
                if (best == null || l.split("\t", 2)[0].compareTo(best.split("\t", 2)[0]) < 0) best = l;
                n++;
            }
            ctx.getCounter(Stats.DUPLICATES).increment(n - 1);
            ctx.write(NullWritable.get(), new Text(best)); // only the line is written
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // args: input, output, nb of reducers
        Job job = Job.getInstance(getConf(), "B2FilterDedupMR");
        job.setJarByClass(B2FilterDedupMR.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(DedupReducer.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new B2FilterDedupMR(), args));
    }
}
