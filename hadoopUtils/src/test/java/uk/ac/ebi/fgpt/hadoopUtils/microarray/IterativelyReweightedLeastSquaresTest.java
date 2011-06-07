package uk.ac.ebi.fgpt.hadoopUtils.microarray;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.math.IterativelyReweightedLeastSquares;
import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

public class IterativelyReweightedLeastSquaresTest {
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
  public void testGetDataVector() {
    Vector vector = IterativelyReweightedLeastSquares.getDataVector(mockProbeset);
    for (int i = 0; i < vector.size(); i++) {
      System.out.println(vector.get(i));
    }
    assertEquals(1, vector.get(0), 0);
    assertEquals(7, vector.get(1), 0);
    assertEquals(9, vector.get(5), 0);
  }
  
  @Test
  public void testSomeBasicMatrixFunctions() {
    DenseMatrix dm = new DenseMatrix(3, 4);
    dm.set(0, 0, 1);
    dm.set(0, 1, 2);
    dm.set(0, 2, 3);
    dm.set(0, 3, 4);
    dm.set(1, 0, 5);
    dm.set(1, 1, 6);
    dm.set(1, 2, 7);
    dm.set(1, 3, 8);
    dm.set(2, 0, 9);
    dm.set(2, 1, 10);
    dm.set(2, 2, 11);
    dm.set(2, 3, 12);
    
    DenseMatrix dm2 = new DenseMatrix(4, 2);
    dm2.set(0, 0, 1);
    dm2.set(0, 1, 2);
    dm2.set(1, 0, 3);
    dm2.set(1, 1, 4);
    dm2.set(2, 0, 5);
    dm2.set(2, 1, 6);
    dm2.set(3, 0, 7);
    dm2.set(3, 1, 8);
    
    DenseVector dv = new DenseVector(4);
    dv.set(0, 1);
    dv.set(1, 2);
    dv.set(2, 3);
    dv.set(3, 4);
    
    Vector product = dm.times(dv);
    assertEquals("Vector of 3", 3, product.size());
    
    Matrix matrixProduct = dm.times(dm2);
    assertEquals("Number of Rows: 3", 3, matrixProduct.size()[0]);
    assertEquals("Number of Columns: 2", 2, matrixProduct.size()[1]);
  }
  
  @Test
  public void testGetDiagonalMatrixFromVector() {
    DenseVector dv = new DenseVector(4);
    dv.set(0, 1);
    dv.set(1, 2);
    dv.set(2, 3);
    dv.set(3, 4);
    
    Matrix output = IterativelyReweightedLeastSquares.getDiagonalMatrixFromVector(dv);
    for (int i = 0; i < output.size()[0]; i++) {
      for (int j = 0; j < output.size()[1]; j++) {
        System.out.print(output.get(i, j) + "\t");
      }
      System.out.println();
    }
  }
  
  @Test
  public void testCalculateSHat() {
    DenseVector dv = new DenseVector(4);
    dv.set(0, 1);
    dv.set(1, 2);
    dv.set(2, 3);
    dv.set(3, 4);
    
    double sHat = IterativelyReweightedLeastSquares.calculateSHat(dv);
    assertEquals(3.70644, sHat, 0.00001);
  }
  
  @Test
  public void testAbs() {
    DenseVector dv = new DenseVector(4);
    dv.set(0, -1);
    dv.set(1, 2);
    dv.set(2, -3);
    dv.set(3, 4);
    
    Vector absVector = IterativelyReweightedLeastSquares.abs(dv);
    // for(int i = 0;i<absVector.size();i++){
    // System.out.println(absVector.get(i));
    // }
    assertEquals(absVector.get(0), 1, 1);
    
  }
  
  @Test
  public void testRun() throws URISyntaxException, IOException {
    Probeset probeset = loadSampleProbeset();
    IrlsOutput output = IterativelyReweightedLeastSquares.run(probeset, 0.0001, 20);
    output.print();
    assertEquals(0.997123910825911, output.getArrayOfWeightVectors()[0].get(0), 0.0001);
    assertEquals(4.34064599510290, output.getVectorOfEstimates().get(0), 0.0001);
  }
  
  @Test
  public void testMyMock() {
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
    IrlsOutput output = IterativelyReweightedLeastSquares.run(mockProbeset, 0.0001, 20);
    output.print();
    
  }
  
  @Test
  public void testRunHuge() throws URISyntaxException, IOException {
    Probeset probeset = loadHUGEProbeset();
    DesignMatrixFactory factory = new DesignMatrixFactory(probeset.getNumProbes(), probeset.getNumSamples());
    Matrix designMatrix = factory.getDesignMatrix();
    System.out.println(designMatrix.size()[0] + " " + designMatrix.size()[1]);
    // long time = System.currentTimeMillis();
    // designMatrix.quicktranspose().times(designMatrix);
    // System.out.println(System.currentTimeMillis()-time);
    
    // IrlsOutput output = IterativelyReweightedLeastSquares.run(probeset, 0.0001, 20);
  }
  
  private Probeset loadSampleProbeset() throws URISyntaxException, IOException {
    
    File file = new File(getClass().getClassLoader().getResource("example.BG.Norm.log2.txt").toURI());
    
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String header = br.readLine(); // Skips one line
    
    Vector[] arrayOfVectors = new Vector[16];
    int i = 0;
    while ((line = br.readLine()) != null) {
      arrayOfVectors[i] = StringToVector.convert(line, 1);
      i++;
    }
    System.out.println(i);
    return new Probeset("mock", arrayOfVectors);
  }
  
  private Probeset loadHUGEProbeset() throws URISyntaxException, IOException {
    
    File file = new File(getClass().getClassLoader().getResource("probeset1.log2.matrix.txt").toURI());
    
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String header = br.readLine(); // Skips one line
    
    Vector[] arrayOfVectors = new Vector[16];
    int i = 0;
    while ((line = br.readLine()) != null) {
      arrayOfVectors[i] = StringToVector.convert(line, 1);
      i++;
    }
    System.out.println(i);
    return new Probeset("mock", arrayOfVectors);
  }
  
}
