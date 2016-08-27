package com.github.ravikirankalal.hadoop.reader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Created by ravikiran.kalal on 25/06/16.
 */
public abstract class AbstractRecordReader extends RecordReader<LongWritable, Row>{
    private LongWritable key;
    private Row value;
    protected Iterator<Row> rowIterator;
    protected InputStream is;

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (rowIterator.hasNext()) {
            value = rowIterator.next();
            key = new LongWritable(value.getRowNum());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {

        return key;
    }

    @Override
    public Row getCurrentValue() throws IOException, InterruptedException {

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return 0;

    }

    @Override
    public void close() throws IOException {
        if (is != null) {
            is.close();
        }

    }
}