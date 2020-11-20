package com.atguigu.partiton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitonDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PartitonDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitonReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartiton.class);

        FileInputFormat.setInputPaths(job, new Path("d:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
