package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import org.apache.mahout.SparseMatrixThreaded;
import org.apache.mahout.math.Matrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DesignMatrixFactory {
  private int numProbes;
  private int numSamples;
  private Logger log = LoggerFactory.getLogger(DesignMatrixFactory.class);
  
  public DesignMatrixFactory(int numProbes, int numSamples){
    this.numProbes = numProbes;
    this.numSamples = numSamples;
  }
  
  public Matrix getDesignMatrix() {
    SparseMatrixThreaded matrix = new SparseMatrixThreaded(numProbes * numSamples, (numSamples + numProbes - 1));
    
    int rowPosition = 0;
    for (int column = 0; column < numSamples; column++) {
      for (int probeNumber = 0; probeNumber < numProbes; probeNumber++) {
        matrix.setQuick(rowPosition, column, 1);
        if (rowPosition != 0 && ((rowPosition + 1) % (numProbes) == 0)) {
          for (int i = 0; i < (numProbes - 1); i++) {
            matrix.setQuick(rowPosition, numSamples + i, -1);
          }
        } else {
          matrix.setQuick(rowPosition, (numSamples + probeNumber), 1);
        }
        rowPosition++;
      }
    }
    return matrix;
  }
  
  public Matrix getDesignMatrixTranspose() {
    SparseMatrixThreaded sparseMatrix = new SparseMatrixThreaded((numSamples + numProbes - 1), numProbes * numSamples);
    int column = 0;
    for (int i = 0; i < numSamples; i++) {
      int offset = 0;
      for (int j = 0; j < numProbes; j++) {
        sparseMatrix.setQuick(i, column, 1);
        if(j!=numProbes-1){
          sparseMatrix.setQuick(numSamples + offset, column, 1);
        }
        column++;
        offset++;
      }
      for (int v = 0; v < numProbes - 1; v++) {
        sparseMatrix.setQuick(numSamples + v, column - 1, -1);
      }
    }
    return sparseMatrix;
  }
}
