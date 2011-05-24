package uk.ac.ebi.fgpt.hadoopUtils.microarray;

import static org.junit.Assert.*;

import org.apache.mahout.math.Matrix;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrix;

public class DesignMatrixTest {
  private Matrix test;
  private int numProbes = 5;
  private int numSamples = 6;
  @Before
  public void setUp() throws Exception {
    test = DesignMatrix.getDesignMatrix(numProbes, numSamples);
  }
    
  @Test
  public void testDesignMatrix() {
    for(int i = 0;i< numProbes*numSamples; i++){
      for(int j = 0;j<(numProbes + numSamples - 1);j++){
        System.out.print(test.get(i, j)+"\t");
      }
      System.out.println();
    }
  }
  @Test
  public void testTranspose() {
   
    Matrix originalTranspose = test.transpose();
    Matrix quickTranspose = DesignMatrix.getDesignMatrixTranspose(numProbes, numSamples);
    int rows = originalTranspose.size()[0];
    int col = originalTranspose.size()[1];
    
    for(int i = 0; i< rows; i ++ ){
      for(int j = 0; j< col; j++){
        System.out.print(originalTranspose.get(i, j)+"\t");
        assertEquals(originalTranspose.get(i, j), quickTranspose.get(i, j),0);
      }
      System.out.println();
    }
  }
}
