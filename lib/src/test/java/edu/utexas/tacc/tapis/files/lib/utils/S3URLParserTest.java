package edu.utexas.tacc.tapis.files.lib.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class S3URLParserTest {

    @Test
    public void testGetRegion() {
        String region;
        region = S3URLParser.getRegion("https://bucket.s3.test-region.amazonaws.com");
        Assert.assertEquals(region, "test-region");

        region = S3URLParser.getRegion("https://s3.test-region.amazonaws.com/bucket");
        Assert.assertEquals(region, "test-region");

        region = S3URLParser.getRegion("https://s3.amazonaws.com/bucket");
        Assert.assertNull(region);

        region = S3URLParser.getRegion("https://localhost:9000/bucket");
        Assert.assertNull(region);

    }
}