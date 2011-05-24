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
  private Configuration conf;
  private FileSystem fs;
  
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
    
  }
  
  public DistributedRowMatrix getDesignMatrix() throws IOException {
    Path designMatrixPath = new Path(designPath, numProbes + ".des");
    
    log.info("Path Checking");
    if (!fs.isFile(designMatrixPath)) {
      log.info("Does not exist! Please confirm that the following path exists: "
               + designMatrixPath.toString());
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
    
    log.info("Path Checking");
    if (!fs.isFile(designMatrixTranposePath)) {
      log.info("Does not exist! Please confirm that the following path exists: "
               + designMatrixTranposePath.toString());
    }
    
    log.info("Creating DistributedRowMatrix for Transpose");
    DistributedRowMatrix designMatrixTranpose = new DistributedRowMatrix(designMatrixTranposePath, tmpPath,
        (numSamples + numProbes - 1), (numProbes * numSamples));
    
    JobConf jobConf = new JobConf("designMatrixTranspose");
    jobConf.setJar(jarString);
    designMatrixTranpose.setConf(jobConf);
    
    return designMatrixTranpose;
    
  }
  
  public DistributedRowMatrix getProductOfDesignMatrixTransposeTimesDesignMatrix() throws IOException {
    Path productPath = new Path(designPath, numProbes + ".prod");
    
    log.info("Path Checking");
    if (!fs.exists(productPath)) {
      log.warn("Does not exist! Please confirm that the following path exists: " + productPath.toString());
    }
    
    log.info("Creating DistributedRowMatrix for Product");
    DistributedRowMatrix designMatrixProduct = new DistributedRowMatrix(productPath, tmpPath,
        (numSamples + numProbes - 1), (numSamples + numProbes - 1));
    
    JobConf jobConf = new JobConf("productMatrix");
    jobConf.setJar(jarString);
    designMatrixProduct.setConf(jobConf);
    
    return designMatrixProduct;
  }
}
