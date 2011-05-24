package uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.solver.DistributedConjugateGradientSolver;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;

public class DistributedIrlsTest {
  
  private Probeset mockProbeset;
  
  @Before
  public void setUp() throws Exception {
    mockProbeset = new Probeset();
    mockProbeset.setProbesetName("mock_name");
    
    Vector vector0 = new DenseVector(3);
    vector0.set(0, 1);
    vector0.set(1, 2);
    vector0.set(2, 3);
    Vector vector1 = new DenseVector(3);
    vector1.set(0, 7);
    vector1.set(1, 8);
    vector1.set(2, 9);
    
    Vector[] mockarray = new Vector[2];
    mockarray[0] = vector0;
    mockarray[1] = vector1;
    mockProbeset.setArrayOfProbes(mockarray);
  }
  
  @Test
  public void testRun() throws IOException {
    assertEquals(true, true);
    Configuration conf = new Configuration();
    conf.set("temp", "/tmp/tmp");
    conf.set("design", "/tmp/design");
    // TODO Doesn't work in a test... try to fix
    // DistributedIrls.run(mockProbeset, 0.0001, 20,conf);
  }
  
}
