package com.github.ravikirankalal;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by ravikiran.kalal on 19/07/16.
 */
@Slf4j
public class CustomDriver {

    public static final String OUTPUT_FOLDER = "target/mapreduceoutput";

    public void run(String inputFilePath,Class<? extends InputFormat> clazz) throws Exception {
        Job job = new Job();
        job.setJarByClass(CustomDriver.class);
        job.setJobName("XSSF input format Test");

        job.setMapperClass(CustomMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(CustomReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER));

        job.setInputFormatClass(clazz);

        job.waitForCompletion(true);
    }
}
