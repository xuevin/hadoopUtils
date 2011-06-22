package uk.ac.ebi.fgpt.hadoopUtils.microarray.executor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedDesignMatrixFactory {
  
  private Logger log = LoggerFactory.getLogger(DistributedDesignMatrixFactory.class);
  
  private Path designPath;
  private Path tmpPath;
  private int numProbes;
  private int numSamples;
  private Configuration conf;
  private FileSystem fs;
  
  public DistributedDesignMatrixFactory(int numProbes,
                                        int numSamples,
                                        String probesetName,
                                        Path tmpPath,
                                        Path designPath,
                                        Configuration conf) throws IOException {
    
    this.conf = conf;
    log.info("Creating Design Matrix Factory");
    
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
    
    designMatrix.setConf(new Configuration(conf));
    
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
    
    designMatrixTranpose.setConf(new Configuration(conf));
    
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
    
    designMatrixProduct.setConf(new Configuration(conf));
    
    return designMatrixProduct;
  }
}
