package com.github.ravikirankalal.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ravikiran.kalal on 25/06/16.
 */
@Slf4j
public class CustomMapper extends Mapper<LongWritable, Row, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Row value, Context context)
            throws InterruptedException, IOException {
        Iterator<Cell> cellIterator = value.cellIterator();
        while (cellIterator.hasNext()) {
            context.write(new Text(cellIterator.next().getStringCellValue()),new LongWritable(1));
        }
    }
}