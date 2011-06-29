package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import static org.junit.Assert.*;

import org.apache.mahout.math.Matrix;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;

public class DesignMatrixFactoryTest {
  
  @Before
  public void setUp() throws Exception {

  }
  
  @Test
  public void testDesignMatrix() {
    int numProbes = 3;
    int numSamples = 5;
    DesignMatrixFactory factory = new DesignMatrixFactory(numProbes, numSamples);
    
    Matrix test = factory.getDesignMatrix();
    // for (int i = 0; i < numProbes * numSamples; i++) {
    // for (int j = 0; j < (numProbes + numSamples - 1); j++) {
    // System.out.print(test.get(i, j) + "\t");
    // }
    // System.out.println();
    // }
    // show that the first "ones" are in the right place.
    for (int i = 0; i < numProbes; i++) {
      assertEquals(1, test.get(numProbes * i, i), 0);
    }
  }
  
  public void testHowLongItTakesToMakeDesignMatrix() {
    long time = System.currentTimeMillis();
    int numProbes = 16;
    int numSamples = 30000;
    
    DesignMatrixFactory factory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix test = factory.getDesignMatrix();
    
    System.out.println(System.currentTimeMillis() - time);
  }
  
  @Test
  public void testTranspose() {
    int numProbes = 5;
    int numSamples = 6;
    DesignMatrixFactory factory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix test = factory.getDesignMatrix();
    
    Matrix originalTranspose = test.transpose(); // This is done via a naieve double for loop
    Matrix quickTranspose = factory.getDesignMatrixTranspose(); // This is created with prior knowledge on how
    // the matrix should look
    
    int rows = originalTranspose.size()[0];
    int col = originalTranspose.size()[1];
    
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < col; j++) {
        System.out.print(originalTranspose.get(i, j) + "\t");
        assertEquals(originalTranspose.get(i, j), quickTranspose.get(i, j), 0);
      }
      System.out.println();
    }
  }
  
  @Test
  public void testTransposeTimesDesign() {
    int numProbes = 12;
    int numSamples = 20;
    DesignMatrixFactory factory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix test = factory.getDesignMatrix();
    
    long time = System.currentTimeMillis();
    
    Matrix product = test.transpose().times(test);
    int rows = product.size()[0];
    int col = product.size()[1];
    
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < col; j++) {
        System.out.print(product.get(i, j) + "\t");
      }
      if (i < numSamples) {
        assertEquals(product.get(i, i), numProbes, 0);
      }
      System.out.println();
    }
    System.out.println("Took: " + (System.currentTimeMillis() - time));
  }
  
  public void tetHowLongItTakesToAccessMatrix() {
    int numProbes = 12;
    int numSamples = 3000;
    DesignMatrixFactory factory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix test = factory.getDesignMatrix();
    
    int rows = test.size()[0];
    int col = test.size()[1];
    
    long time = System.currentTimeMillis();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < col; j++) {
        test.get(i, j);
      }
    }
    System.out.println(System.currentTimeMillis() - time);
  }
}
