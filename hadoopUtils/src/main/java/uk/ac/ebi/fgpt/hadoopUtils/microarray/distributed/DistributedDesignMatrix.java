package uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrix;

public class DistributedDesignMatrix{
  
  private Path designMatrixPath;
  private Path designMatrixTranposePath;
  private DistributedRowMatrix designMatrix;
  private DistributedRowMatrix designMatrixTranpose;
  private Matrix designMatrixObject;
  
  private Logger log = LoggerFactory.getLogger(DistributedDesignMatrix.class);
  
  public DistributedDesignMatrix(int numProbes,
                                 int numSamples,
                                 String probesetName,
                                 Path tmpPath,
                                 Path designPath,
                                 String jarString) throws IOException {
    
    Configuration conf = new Configuration();
    
    log.info("Creating Design Matricies");
    FileSystem fs = FileSystem.get(conf);
    
    log.info("Temporary Path Checking");
    
    
    if (!fs.exists(tmpPath)) {
      log.info("Making Temp Path");
      fs.mkdirs(tmpPath);
    }
    
    log.info("Checking if design matrix exist");
    designMatrixPath = new Path(designPath, numProbes + ".des");
    if (!fs.isFile(designMatrixPath)) {
      designMatrixObject = DesignMatrix.getDesignMatrix(numProbes, numSamples);
      log.info("Does not exist! Creating New Design Matrix: " + designMatrixPath.toString());
      fs.makeQualified(designMatrixPath);
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixPath, IntWritable.class,
        VectorWritable.class);
      for (int i = 0; i < designMatrixObject.size()[0]; i++) {
        writer.append(new IntWritable(i), new VectorWritable(designMatrixObject.getRow(i)));
      }
      writer.close();
    }
    log.info("Creating DistributedRowMatrix");
    
    designMatrix = new DistributedRowMatrix(designMatrixPath, tmpPath, (numProbes * numSamples),
        (numSamples + numProbes - 1));
    JobConf jobConf = new JobConf("designMatrix");
    jobConf.setJar(jarString);
    designMatrix.setConf(jobConf);
    
    designMatrixObject = null; // Free up some memory
    
    designMatrixTranposePath = new Path(designPath, numProbes + ".des.t");
    log.info("Temporary Path Checking");
    if (!fs.isFile(designMatrixTranposePath)) {
      designMatrixObject = DesignMatrix.getDesignMatrixTranspose(numProbes, numSamples);
      log.info("Does not exist! Creating New Design Matrix: " + designMatrixTranposePath.toString());
      fs.makeQualified(designMatrixTranposePath);
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixTranposePath,
        IntWritable.class, VectorWritable.class);
      for (int i = 0; i < designMatrixObject.size()[0]; i++) {
        writer.append(new IntWritable(i), new VectorWritable(designMatrixObject.getRow(i)));
      }
      writer.close();
    }
    
    log.info("Creating DistributedRowMatrix for Transpose");
    designMatrixTranpose = new DistributedRowMatrix(designMatrixTranposePath, tmpPath,
        (numSamples + numProbes - 1), (numProbes * numSamples));
    JobConf jobConf2 = new JobConf("designMatrix");
    jobConf2.setJar(jarString);
    designMatrixTranpose.setConf(jobConf2);
  }
  
  public DistributedRowMatrix get() {
    return designMatrix;
  }
  
  public DistributedRowMatrix getTranspose() {
    return designMatrixTranpose;
    
  }
  
  public int getNumRows() {
    return designMatrixObject.size()[0];
  }
  
  public int getNumCols() {
    return designMatrixObject.size()[1];
  }
  
}
