package uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;

public class DistributedDesignMatrixFactory {
  
  private Logger log = LoggerFactory.getLogger(DistributedDesignMatrixFactory.class);
  
  private Path designPath;
  private Path tmpPath;
  private int numProbes;
  private int numSamples;
  private String jarString;
  private FileSystem fs;
  private Configuration conf;
  private DesignMatrixFactory designMatrixFactory;
  
  public DistributedDesignMatrixFactory(int numProbes,
                                        int numSamples,
                                        String probesetName,
                                        Path tmpPath,
                                        Path designPath,
                                        String jarString) throws IOException {
    
    log.info("Creating Design Matrix Factory");
    conf = new Configuration();
    fs = FileSystem.get(conf);
    
    log.info("Check if Temp Path Exists");
    if (!fs.exists(tmpPath)) {
      log.info("Making Temp Path");
      fs.mkdirs(tmpPath);
    }
    
    this.designPath = designPath;
    this.tmpPath = tmpPath;
    this.numProbes = numProbes;
    this.numSamples = numSamples;
    this.jarString = jarString;
    this.designMatrixFactory = new DesignMatrixFactory(numProbes, numSamples);
  }
  
  public DistributedRowMatrix getDesignMatrix() throws IOException {
    Path designMatrixPath = new Path(designPath, numProbes + ".des");
    
    log.info("Checking if design matrix exist");
    if (!fs.isFile(designMatrixPath)) {
      log.info("Does not exist! Creating New Design Matrix: " + designMatrixPath.toString());
      writeNewDesignMatrixToHDFS(numProbes, numSamples, designMatrixPath);
    }
    
    log.info("Creating DistributedRowMatrix for DesignMatrix");
    DistributedRowMatrix designMatrix = new DistributedRowMatrix(designMatrixPath, tmpPath,
        (numProbes * numSamples), (numSamples + numProbes - 1));
    
    JobConf jobConf = new JobConf("designMatrix");
    jobConf.setJar(jarString);
    designMatrix.setConf(jobConf);
    
    return designMatrix;
  }
  
  public DistributedRowMatrix getDesignMatrixTranspose() throws IOException {
    Path designMatrixTranposePath = new Path(designPath, numProbes + ".des.t");
    
    log.info("Temporary Path Checking");
    if (!fs.isFile(designMatrixTranposePath)) {
      log.info("Does not exist! Creating New Design Matrix: " + designMatrixTranposePath.toString());
      writeNewDesignMatrixTranposeToHDFS(numProbes, numSamples, designMatrixTranposePath);
    }
    
    log.info("Creating DistributedRowMatrix for Transpose");
    DistributedRowMatrix designMatrixTranpose = new DistributedRowMatrix(designMatrixTranposePath, tmpPath,
        (numSamples + numProbes - 1), (numProbes * numSamples));
    
    JobConf jobConf = new JobConf("designMatrixTranspose");
    jobConf.setJar(jarString);
    designMatrixTranpose.setConf(jobConf);
    
    return designMatrixTranpose;
    
  }
  
  private void writeNewDesignMatrixTranposeToHDFS(int numProbes, int numSamples, Path designMatrixTranposePath) throws IOException {
    
    Matrix designMatrixTranspose = designMatrixFactory.getDesignMatrixTranspose();
    fs.makeQualified(designMatrixTranposePath);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixTranposePath,
      IntWritable.class, VectorWritable.class);
    for (int i = 0; i < designMatrixTranspose.size()[0]; i++) {
      writer.append(new IntWritable(i), new VectorWritable(designMatrixTranspose.getRow(i)));
    }
    writer.close();
    
  }
  
  private void writeNewDesignMatrixToHDFS(int numProbes, int numSamples, Path designMatrixPath) throws IOException {
    
    Matrix designMatrixObject = designMatrixFactory.getDesignMatrix();
    fs.makeQualified(designMatrixPath);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixPath, IntWritable.class,
      VectorWritable.class);
    for (int i = 0; i < designMatrixObject.size()[0]; i++) {
      writer.append(new IntWritable(i), new VectorWritable(designMatrixObject.getRow(i)));
    }
    writer.close();
  }
  
  public DistributedRowMatrix getProductOfDesignMatrixTransposeTimesDesignMatrix() throws IOException {
    Path productPath = new Path(designPath, numProbes + ".prod");
    
    log.info("Temporary Path Checking");
    if (!fs.isFile(productPath)) {
      log.info("Does not exist! Creating New Product Matrix: " + productPath.toString());
      writeNewProductToHDFS(productPath);
    }
    
    log.info("Creating DistributedRowMatrix for Product");
    DistributedRowMatrix designMatrixProduct = new DistributedRowMatrix(productPath, tmpPath,
        (numSamples + numProbes - 1), (numSamples + numProbes - 1));
    
    JobConf jobConf = new JobConf("productMatrix");
    jobConf.setJar(jarString);
    designMatrixProduct.setConf(jobConf);
    
    return designMatrixProduct;
  }
  
  private void writeNewProductToHDFS(Path productPath) throws IOException {
    
    log.info("Creating product of DesignMatrixTranpose x Design Matrix");
    DistributedRowMatrix matrixA = getDesignMatrix();
    DistributedRowMatrix matrixB = getDesignMatrix();
    
    log.info("Multiplying the transpose of " + matrixA.getRowPath() + " with " + matrixB.getRowPath()
             + " -> " + productPath.toString());
    
    Configuration initialConf = matrixA.getConf();
    Configuration conf = MatrixMultiplicationJob.createMatrixMultiplyJobConf(initialConf, matrixA
        .getRowPath(), matrixB.getRowPath(), productPath, matrixB.numCols());
    JobClient.runJob(new JobConf(conf));
  }
  
}
