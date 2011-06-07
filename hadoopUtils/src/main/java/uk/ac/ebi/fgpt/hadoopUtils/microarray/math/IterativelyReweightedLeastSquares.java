package uk.ac.ebi.fgpt.hadoopUtils.microarray.math;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.solver.ConjugateGradientSolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;

/**
 * This is the linear implementation of Iteratively Reweighted Least Squares
 * 
 * @author Vincent Xue
 * 
 */
public class IterativelyReweightedLeastSquares {
  private static Logger log = LoggerFactory.getLogger(IterativelyReweightedLeastSquares.class);
  
  public static IrlsOutput run(Probeset probeset, double tol, int maxIter) {
    long time = System.currentTimeMillis();
    log.info("Starting IRLS...");
    int iteration = 0;
    double error = 1 + tol;
    
    log.info("Creating Data Vector");
    Vector dataVector = getDataVector(probeset);
    
    log.info("Creating Design Matrix");
    DesignMatrixFactory designMatrixFactory = new DesignMatrixFactory(probeset.getNumProbes(), probeset
        .getNumSamples());
    Matrix designMatrix = designMatrixFactory.getDesignMatrix();
    Matrix designMatrixTranspose = designMatrixFactory.getDesignMatrixTranspose();
    
    ConjugateGradientSolver cgs = new ConjugateGradientSolver();
    
    log.info("Create A");
    Matrix A = designMatrixTranspose.times(designMatrix);
    
    log.info("Create b");
    Vector b = designMatrixTranspose.times(dataVector);
    
    log.info("Running Congjugate Gradient Solver");
    Vector vectorOfEstimates = cgs.solve(A, b, null, b.size(), tol);
    
    log.info("Calculating Residuals");
    Vector vectorOfResidualsInitial = dataVector.minus(designMatrix.times(vectorOfEstimates));
    
    // Make a copy of the initial vectorOfResiduals
    Vector vectorOfResidualsCurrent = new DenseVector(vectorOfResidualsInitial);
    Vector weights = null;
    
    while (iteration <= maxIter && error > tol) {
      log.info("Running iteration: " + iteration);
      
      double sHat = calculateSHat(vectorOfResidualsInitial);
      weights = weight(vectorOfResidualsInitial.divide(sHat));
      Matrix weightMatrix = getDiagonalMatrixFromVector(weights);
      
      cgs = new ConjugateGradientSolver();
      
      A = designMatrixTranspose.times(weightMatrix).times(designMatrix);
      b = designMatrixTranspose.times(weightMatrix).times(dataVector);
      vectorOfEstimates = cgs.solve(A, b, null, b.size(), tol);
      
      vectorOfResidualsCurrent = dataVector.minus(designMatrix.times(vectorOfEstimates));
      
      error = abs(vectorOfResidualsCurrent.minus(vectorOfResidualsInitial)).maxValue();
      vectorOfResidualsInitial = new DenseVector(vectorOfResidualsCurrent);
      iteration++;
    }
    Vector[] arrayOfWeightVectors = new Vector[probeset.getNumProbes()];
    for (int i = 0; i < probeset.getNumProbes(); i++) {
      arrayOfWeightVectors[i] = new DenseVector(probeset.getNumSamples());
    }
    
    // Double[][] array = new Double[probeset.getNumProbes()][probeset.getNumSamples()];
    int column = 0;
    int row = 0;
    for (int i = 0; i < weights.size(); i++) {
      arrayOfWeightVectors[row].set(column, weights.get(i));
      row++;
      if (row == probeset.getNumProbes()) {
        row = 0;
      }
      if ((i + 1) % probeset.getNumProbes() == 0) {
        column++;
      }
    }
    
    log.info("IRLS Took: " + (System.currentTimeMillis() - time) + "ms");
    return new IrlsOutput(probeset.getProbesetName(), arrayOfWeightVectors, vectorOfEstimates);
    
  }
  
  public static Vector weight(Vector residualsAfterDivision) {
    double k = 1.345;
    Vector vectorOfWeights = new DenseVector(residualsAfterDivision.size());
    
    for (int i = 0; i < residualsAfterDivision.size(); i++) {
      double weight;
      if (Math.abs(residualsAfterDivision.get(i)) <= k) {
        weight = 1;
      } else {
        weight = (k / Math.abs(residualsAfterDivision.get(i)));
      }
      vectorOfWeights.set(i, weight);
    }
    return vectorOfWeights;
  }
  
  public static Vector getDataVector(Probeset probeset) {
    // Make Data Vector
    Vector[] arrayOfVectors = probeset.getArrayOfVectors();
    
    int numberOfProbes = arrayOfVectors.length;
    if (numberOfProbes == 0) {
      System.err.println("This probeset has no probes!");
    }
    
    int numberOfSamples = arrayOfVectors[0].size();
    if (numberOfSamples == 0) {
      System.err.println("This probeset has no samples");
    }
    
    Vector dataVector = new DenseVector(numberOfSamples * numberOfProbes);
    int position = 0;
    for (int i = 0; i < numberOfSamples; i++) {
      for (int j = 0; j < numberOfProbes; j++) {
        dataVector.setQuick(position, arrayOfVectors[j].get(i));
        position++;
      }
    }
    return dataVector;
  }
  
  public static Matrix getDiagonalMatrixFromVector(Vector vector) {
    Matrix matrix = new SparseMatrix(vector.size(), vector.size());
    for (int i = 0; i < vector.size(); i++) {
      matrix.set(i, i, vector.get(i));
    }
    return matrix;
  }
  
  public static double calculateSHat(Vector vector) {
    ArrayList<Double> values = new ArrayList<Double>();
    
    for (int i = 0; i < vector.size(); i++) {
      values.add(i, Math.abs(vector.get(i)));
    }
    Collections.sort(values);
    
    double median;
    if (values.size() % 2 == 1) {
      median = values.get((values.size() + 1) / 2 - 1);
    } else {
      double lower = values.get(values.size() / 2 - 1);
      double upper = values.get(values.size() / 2);
      median = (lower + upper) / 2.0;
    }
    return median / 0.6745;
  }
  
  public static Vector abs(Vector vector) {
    Vector returnVector = new DenseVector(vector.size());
    for (int i = 0; i < vector.size(); i++) {
      returnVector.set(i, Math.abs(vector.get(i)));
    }
    return returnVector;
  }
}
