package uk.ac.ebi.fgpt.hadoopUtils.microarray.executor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;
import org.apache.mahout.math.hadoop.solver.DistributedConjugateGradientSolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.math.IterativelyReweightedLeastSquares;

/**
 * This is the distributed version of IRLS, which takes advantage of the distributed linear algebra functions
 * 
 * @author Vincent Xue
 * 
 */
public class DistributedIrls extends IterativelyReweightedLeastSquares {
  private static Logger log = LoggerFactory.getLogger(DistributedIrls.class);
  
  public static IrlsOutput run(Probeset probeset, double tol, double conjGTol,int maxIter, Path designPath, Path tempPath, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    
    long time = System.currentTimeMillis();
    log.info("Starting IRLS: " + probeset.getProbesetName());
    int iteration = 0;
    double error = 1 + tol;
    
    log.info("Creating Data Vector");
    Vector dataVector = getDataVector(probeset);
    
    log.info("Creating Design Matrix");
    DistributedDesignMatrixFactory distributedDesignMatrixFactory = new DistributedDesignMatrixFactory(
        probeset.getNumProbes(), probeset.getNumSamples(), probeset.getProbesetName(), tempPath, designPath,
        conf);
    
    DistributedRowMatrix designMatrix = distributedDesignMatrixFactory.getDesignMatrix();
    DistributedRowMatrix designMatrixTranspose = distributedDesignMatrixFactory.getDesignMatrixTranspose();
    
    log.info("Create A");
    DistributedRowMatrix A = distributedDesignMatrixFactory
        .getProductOfDesignMatrixTransposeTimesDesignMatrix();
    
    log.info("Create b");
    Vector b = designMatrixTranspose.times(dataVector);
    
    log.info("Running Congjugate Gradient Solver");
    DistributedConjugateGradientSolver dcgs = new DistributedConjugateGradientSolver();
    Vector vectorOfEstimates = dcgs.solve(A, b, null, b.size(), conjGTol);
    
    log.info("Calculating Residuals");
    Vector vectorOfResidualsInitial = dataVector.minus(designMatrix.times(vectorOfEstimates));
    
    // Make a copy of the initial vectorOfResiduals
    Vector vectorOfResidualsCurrent = new DenseVector(vectorOfResidualsInitial);
    Vector weights = null;
    
    while (iteration <= maxIter && error > tol) {
      log.info("Running iteration: " + iteration);
      
      double sHat = calculateSHat(vectorOfResidualsInitial);
      weights = weight(vectorOfResidualsInitial.divide(sHat));
      DistributedRowMatrix weightMatrix = getDiagonalMatrixFromVector(weights, conf, iteration, tempPath);
      
      dcgs = new DistributedConjugateGradientSolver();
      
      log.info("Calculating W Transpose times DesignMatrix");
      DistributedRowMatrix weightTransposeByDesign = getProductOfATransposeB(weightMatrix, designMatrix,
        conf, tempPath);
      // Because Matrix multiplication is associative, I can transpose the Weight matrix first and multiply
      // it with the Design matrix. Transposing the weight matrix, is itself. (Because it's diagonal)
      
      log.info("Calculating A = (Design Transpose (W Transpose times DesignMatrix))");
      A = getProductOfATransposeB(designMatrix, weightTransposeByDesign, conf, tempPath);
      
      log.info("Deleting W Transpose By DesignMatrix: " + weightTransposeByDesign.getRowPath());
      FileSystem.get(conf).delete(weightTransposeByDesign.getRowPath(), true);
      
      log.info("Calculating designMatrix Transpose By Weight Matrix");
      DistributedRowMatrix designTransposebyWeight = getProductOfATransposeB(designMatrix, weightMatrix,
        conf, tempPath);
      
      log.info("Deleting Weight Matrix: " + weightMatrix.getRowPath());
      FileSystem.get(conf).delete(weightMatrix.getRowPath(), true);
      
      log.info("Calculating b = (Design Transpose (Data Vector))");
      b = designTransposebyWeight.times(dataVector);
      
      log.info("Deleting DesignMatrix Transpose By Weight Matrix: " + designTransposebyWeight.getRowPath());
      FileSystem.get(conf).delete(designTransposebyWeight.getRowPath(), true);
      
      log.info("Running Congjugate Gradient Solver");
      vectorOfEstimates = dcgs.solve(A, b, null, b.size(), conjGTol);
      
      
      log.info("Deleting A: " + A.getRowPath());
      FileSystem.get(conf).delete(A.getRowPath(), true);
      
      log.info("Calculating Residuals");
      vectorOfResidualsCurrent = dataVector.minus(designMatrix.times(vectorOfEstimates));
      
      error = abs(vectorOfResidualsCurrent.minus(vectorOfResidualsInitial)).maxValue();
      vectorOfResidualsInitial = new DenseVector(vectorOfResidualsCurrent);
      iteration++;
    }
    log.info("Finished IRLS");
    Vector[] arrayOfWeightVectors = new Vector[probeset.getNumProbes()];
    for (int i = 0; i < probeset.getNumProbes(); i++) {
      arrayOfWeightVectors[i] = new DenseVector(probeset.getNumSamples());
    }
    
    int column = 0;
    int row = 0;
    for (int i = 0; i < weights.size(); i++) {
      arrayOfWeightVectors[row].set(column, weights.get(i));
      // =============================================
      // FOR DEBUGGING ONLY
      // log.info(row + " " + column + " =>" + weights.get(i));
      // =============================================
      row++;
      if (row == probeset.getNumProbes()) {
        row = 0;
      }
      if ((i + 1) % probeset.getNumProbes() == 0) {
        column++;
      }
    }
    log.info("IRLS Took: " + (System.currentTimeMillis() - time) + "ms");
    fs.delete(tempPath, true);
    return new IrlsOutput(probeset.getProbesetName(), arrayOfWeightVectors, vectorOfEstimates);
    
  }
  
  public static DistributedRowMatrix getDiagonalMatrixFromVector(Vector vector,
                                                                 Configuration conf,
                                                                 int iteration,
                                                                 Path tmpPath) throws IOException {
    Path outpath = new Path(tmpPath, "weight" + iteration); // Prevents matrix multiplication from writing
    // in parent
    Matrix matrix = new SparseMatrix(vector.size(), vector.size());
    for (int i = 0; i < vector.size(); i++) {
      matrix.set(i, i, vector.get(i));
    }
    FileSystem fs = FileSystem.get(conf);
    if (!fs.isFile(outpath)) {
      log.info("Creating Design Matrix: " + outpath.toString());
      fs.makeQualified(outpath);
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outpath, IntWritable.class,
        VectorWritable.class);
      for (int i = 0; i < matrix.size()[0]; i++) {
        writer.append(new IntWritable(i), new VectorWritable(matrix.getRow(i)));
      }
      writer.close();
    } else {
      log.warn("ERROR! The weight matrix was found in temp");
    }
    
    DistributedRowMatrix drm = new DistributedRowMatrix(outpath, tmpPath, matrix.size()[0], matrix.size()[1]);
    drm.setConf(new Configuration(conf));
    
    return drm;
  }
  
  public static DistributedRowMatrix getProductOfATransposeB(DistributedRowMatrix matrixA,
                                                             DistributedRowMatrix matrixB,
                                                             Configuration conf,
                                                             Path tmpPath) throws IOException {
    
    log.info("Creating matrix multiplication outpath");
    Path outPath = new Path(tmpPath, "productWith-" + (System.nanoTime() & 0xFF));
    FileSystem fs = FileSystem.get(conf);
    while (fs.exists(outPath)) {
      log.info("Conflicting outpath!");
      outPath = new Path(tmpPath, "productWith-" + (System.nanoTime() & 0xFF));
    }
    
    log.info("Multiplying the transpose of " + matrixA.getRowPath() + " with " + matrixB.getRowPath()
             + " -> " + outPath.toString());
    
    Configuration initialConf = matrixA.getConf();

    Configuration matrixConf = MatrixMultiplicationJob.createMatrixMultiplyJobConf(initialConf, matrixA
        .getRowPath(), matrixB.getRowPath(), outPath, matrixB.numCols());
    JobClient.runJob(new JobConf(matrixConf));
    
    
    DistributedRowMatrix out = new DistributedRowMatrix(outPath, tmpPath, matrixA.numCols(), matrixB
        .numCols());
    out.setConf(new Configuration(conf));
    return out;
    
  }
}
