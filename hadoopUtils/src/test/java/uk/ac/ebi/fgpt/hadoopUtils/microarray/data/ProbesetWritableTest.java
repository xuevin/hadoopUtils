package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MahoutTestCase;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;

public final class ProbesetWritableTest extends MahoutTestCase {
  private ProbesetWritable probeSetWritable;
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
    
    probeSetWritable = new ProbesetWritable(mockProbeset);
  }
  
  @Test
  public void testProbesetWritable() {
    assertNotNull(probeSetWritable);
  }
  
  @Test
  public void testGet() {
    assertEquals("Probeset Object Changed?", mockProbeset, probeSetWritable.get());
  }
  
  @Test
  public void testReadAndWrite() throws IOException {
    ProbesetWritable probesetWritable2 = new ProbesetWritable();
    writeAndRead(probeSetWritable, probesetWritable2);
    Probeset probeset2 = probesetWritable2.get();
    
    assertEquals(mockProbeset.getProbesetName(), probeset2.getProbesetName());
    
  }
  
  
  private static void writeAndRead(Writable toWrite, Writable toRead) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      toWrite.write(dos);
    } finally {
      dos.close();
    }
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    try {
      toRead.readFields(dis);
    } finally {
      dis.close();
    }
  }
}
