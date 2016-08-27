package com.github.ravikirankalal.hadoop.inputFormat;

import com.github.ravikirankalal.hadoop.reader.XSSFRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;

/**
 * Created by ravikiran.kalal on 25/06/16.
 */
public class XSSFInputFormat extends FileInputFormat<LongWritable,Row>{
    @Override
    public RecordReader<LongWritable, Row> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new XSSFRecordReader();
    }
}