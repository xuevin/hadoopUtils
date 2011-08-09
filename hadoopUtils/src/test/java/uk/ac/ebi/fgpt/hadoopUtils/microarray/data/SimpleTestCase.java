package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class SimpleTestCase {
  protected final File getTestTempDir() throws IOException {
    File testTempDir = null;
    
    if (testTempDir == null) {
      String systemTmpDir = System.getProperty("java.io.tmpdir");
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDir = new File(systemTmpDir, "mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong);
      if (!testTempDir.mkdir()) {
        throw new IOException("Could not create " + testTempDir);
      }
      testTempDir.deleteOnExit();
    }
    return testTempDir;
  }
  
  @Test
  public void test() {
    assertEquals(true, true);
  }
}
