package edu.iit.cs553.hw5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRSort extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        // args: input, output, nb of reducers
        if (args.length != 3) {
            System.err.println("Usage: MRSort <input> <output> <nb_reducers>");
            return 1;
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int nbReducers = Integer.parseInt(args[2]);

        // job setup
        Job job = Job.getInstance(getConf(), "MRSort");
        job.setJarByClass(MRSort.class);

        // TeraGen format)
        job.setInputFormatClass(TeraInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        job.setOutputFormatClass(TeraOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        // key/value types: Text compares bytes unsigned
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // identity map and reduce, the shuffle does the sort
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        // must be set before sampling
        job.setNumReduceTasks(nbReducers);

        // partition file 
        Path partFile = new Path(output + "_partitions");
        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partFile);

        // sampling on tiny
        InputSampler.Sampler<Text, Text> sampler =
        new InputSampler.RandomSampler<>(0.01, 100000, 20);
        InputSampler.writePartitionFile(job, sampler);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // ToolRunner removes -D options before run()
        System.exit(ToolRunner.run(new MRSort(), args));
    }
}
