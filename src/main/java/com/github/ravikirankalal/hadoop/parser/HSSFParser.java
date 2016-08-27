package com.github.ravikirankalal.hadoop.parser;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Created by ravikiran.kalal on 25/06/16.
 */
@Slf4j
public class HSSFParser {

    public Iterator parseExcelData(InputStream is) {
        Iterator<Row> rowIterator = null;
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(is);

            // Taking first sheet from the workbook
            HSSFSheet sheet = workbook.getSheetAt(0);

            // Iterate through each rows from first sheet
            rowIterator = sheet.iterator();
        } catch (IOException e) {
            log.error("IO Exception : File not found ");
        }
        return rowIterator;
    }
}