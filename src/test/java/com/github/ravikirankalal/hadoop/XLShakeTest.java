package com.github.ravikirankalal.hadoop;

import com.github.ravikirankalal.hadoop.inputFormat.HSSFInputFormat;
import com.github.ravikirankalal.hadoop.inputFormat.XSSFInputFormat;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by ravikiran.kalal on 19/07/16.
 */
public class XLShakeTest {
    @Test
    public void testXSSFInputFormat() throws Exception{
        String expectedHash = "7cb61203f751c94f2a56729da8e15352";
        new CustomDriver().run("src/test/resources/XSSFtest.xlsx", XSSFInputFormat.class);
        String actualHash = DigestUtils.md5Hex(new FileInputStream(CustomDriver.OUTPUT_FOLDER+"/part-r-00000"));
        assertEquals(expectedHash,actualHash);
        FileUtils.deleteDirectory(new File(CustomDriver.OUTPUT_FOLDER));
    }

    @Test
    public void testHSSFInputFormat() throws Exception{
        FileUtils.deleteDirectory(new File(CustomDriver.OUTPUT_FOLDER));
        String expectedHash = "7cb61203f751c94f2a56729da8e15352";
        new CustomDriver().run("src/test/resources/HSSFtest.xls", HSSFInputFormat.class);
        String actualHash = DigestUtils.md5Hex(new FileInputStream(CustomDriver.OUTPUT_FOLDER+"/part-r-00000"));
        assertEquals(expectedHash,actualHash);
        FileUtils.deleteDirectory(new File(CustomDriver.OUTPUT_FOLDER));
    }
}
