package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.shared.s3.S3Utils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test(groups = "integration")
public class S3URLParserTest {

    @Test
    public void testGetRegion() {
        String region;
        region = S3Utils.getS3Region("https://bucket.s3.test-region.amazonaws.com");
        Assert.assertEquals(region, "test-region");

        region = S3Utils.getS3Region("https://s3.test-region.amazonaws.com/bucket");
        Assert.assertEquals(region, "test-region");

        region = S3Utils.getS3Region("https://s3.amazonaws.com/bucket");
        Assert.assertNull(region);

        region = S3Utils.getS3Region("https://localhost:9000/bucket");
        Assert.assertNull(region);

    }
}