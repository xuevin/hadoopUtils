package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;

public class DesignMatrix {
  
  public static Matrix getDesignMatrix(int numProbes, int numSamples) {
    SparseMatrix matrix = new SparseMatrix(numProbes * numSamples, (numSamples + numProbes - 1));
    
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
  
  public static Matrix getDesignMatrixTranspose(int numProbes, int numSamples) {
    Matrix sparseMatrix = new SparseMatrix((numSamples + numProbes - 1), numProbes * numSamples);
    int column = 0;
    for (int i = 0; i < numSamples; i++) {
      int offset = 0;
      for (int j = 0; j < numProbes; j++) {
        sparseMatrix.setQuick(i, column, 1);
        sparseMatrix.setQuick(numSamples + offset, column, 1);
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
