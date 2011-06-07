package uk.ac.ebi.fgpt.hadoopUtils.pca.math;

import java.util.StringTokenizer;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

public class StringToVector {
  public static DenseVector convert(String line, int columnsToSkip){
    StringTokenizer tokenizer = new StringTokenizer(line);
    
    int columns = tokenizer.countTokens() - columnsToSkip;
    
    if (columns < 0) {
      System.err.println("Columns to skip is less than total number of columns");
      System.exit(0);
    }
    
    // Make a new vector for each line
    DenseVector vector = new DenseVector(columns);
    
    // Skip a number of tokens
    for (int i = 0; i < columnsToSkip; i++) {
      tokenizer.nextToken();
    }
    
    int i = 0;
    try {
      while (tokenizer.hasMoreTokens()) {
        vector.set(i, Double.parseDouble(tokenizer.nextToken()));
        i++;
      }  
    } catch (NumberFormatException e) {
      throw e;
    }
    
    return vector;
  }
  
  public static DenseVector normalize(String line) {
    StringTokenizer tokenizer = new StringTokenizer(line);
    
    int columns = tokenizer.countTokens();
    
    int i = 0;
    double[] row = new double[columns];
    double sum = 0;
    
    // STEP 1 : PARSE AND FIND SUM
    while (tokenizer.hasMoreTokens()) {
      row[i] = Double.parseDouble(tokenizer.nextToken());
      sum += row[i];
      i++;
    }
    // STEP 2 : FIND MEAN
    double mean = (sum / (double) columns);
    
    // STEP 3 : CALCULATE VARIANCE
    double variance = 0;
    for (double cell : row) {
      variance += Math.pow((cell - mean), 2);
    }
    variance = (variance / (double) (columns - 1));
    
    // STEP 4 : CALCULATE STANDARD DEVIATION
    double standardDeviation = Math.sqrt(variance);
    
    // STEP 5 : FILL IN NEW VECTOR
    DenseVector vector = new DenseVector(columns);
    for (int j = 0; j < row.length; j++) {
      vector.set(j, ((row[j] - mean) / standardDeviation));
    }
    return vector;
  }
  
}
