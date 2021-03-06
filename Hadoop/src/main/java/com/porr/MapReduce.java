package com.porr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {


    public static class CustsMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("name   " + parts[1]));
            System.out.println(context);
        }
    }

    public static class TxnsMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("rel   " + parts[1]));
            context.write(new Text(parts[1]), new Text("rel   " + parts[0]));
            System.out.println(context);
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String name = "";
            int count = 0;
            for (Text t : values)
            {
                String parts[] = t.toString().split("   ");
                if (parts[0].equals("rel"))
                {
                    count++;
                }
                else if (parts[0].equals("name"))
                {
                    name = parts[1];
                }
            }
            String str = String.format("%d", count);
            context.write(new Text(name), new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(MapReduce.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
