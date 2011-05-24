package uk.ac.ebi.fgpt.hadoopUtils.sequential;

import static org.junit.Assert.*;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.math.VectorOperations;

public class VectorOperationsTest {
  Vector vector;
  
  @Before
  public void setUp() throws Exception {
    vector = new DenseVector(12);
    vector.setQuick(0, 1.6131933);
    vector.setQuick(1, 0.88172288);
    vector.setQuick(2, 2.0282054);
    vector.setQuick(3, 3.39008756);
    vector.setQuick(4, 1.35785352);
    vector.setQuick(5, 1.68312769);
    vector.setQuick(6, 0.6219449);
    vector.setQuick(7, 0.96481182);
    vector.setQuick(8, 3.21660037);
    vector.setQuick(9, 2.68736019);
    vector.setQuick(10, 1.63260033);
    vector.setQuick(11, 3.109336);
  }
  
  @Test
  public void testNormalizeVector() {
    
    Vector vec = VectorOperations.normalize(vector);
    assertEquals(-0.3332886698201715,vec.get(0),0);
    assertEquals(0.10025329390898544,vec.get(2),0);
//    for (int i = 0; i < vec.size(); i++) {
//      
//      -0.3332886698201715     -1.0974184922918326     0.10025329390898544     1.5229420926551087      -0.6000291085673093     -0.26023178118294693    -1.3687952921075335     -1.0106197165169237     1.341
//
//    }
  }
  
}
