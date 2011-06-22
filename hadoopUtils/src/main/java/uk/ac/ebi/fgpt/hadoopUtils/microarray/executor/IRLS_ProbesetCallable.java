package uk.ac.ebi.fgpt.hadoopUtils.microarray.executor;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;

public class IRLS_ProbesetCallable implements Runnable {
  private Probeset probeset;
  private Path outPath;
  private Path designPath;
  private Path tempPath;
  private Configuration conf;
  private Logger logger = LoggerFactory.getLogger(IRLS_ProbesetCallable.class);
  
  public IRLS_ProbesetCallable(Configuration conf,
                               Path inputPath,
                               Path outPath,
                               Path designPath,
                               Path tempPath,
                               String probesetKey) throws IOException {
    this.outPath = outPath;
    this.designPath = designPath;
    this.tempPath = tempPath;
    this.conf = conf;
    // Create Reader
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);
    
    // Make temporary reusable objects
    try {
      Text key = (Text) reader.getKeyClass().newInstance();
      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
      while (reader.next(key, value)) {
        if (key.toString().equals(probesetKey)) {
          probeset = value.get();
          break;
        }
      }
      reader.close();
      
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
  
  public void run() {
    
    try {
      if(probeset.getNumProbes()>30){
        return;
      }
      logger.info("Starting probeset: " + probeset.getProbesetName());
      Path probeTempPath = new Path(tempPath,probeset.getProbesetName());
      FileSystem.get(conf).makeQualified(probeTempPath);
      IrlsOutput irlsOutput = DistributedIrls.run(probeset, 0.0001, 0.0001, 20, designPath, probeTempPath, conf);
      Path probePath = new Path(outPath, irlsOutput.getProbesetName());
      IrlsOutputWritable.writeToPath(conf, probePath, irlsOutput);
      logger.info("Finished: " + probeset.getProbesetName());
      return;
    } catch (Exception e) {
      System.err.println("PROBESET FAILED - WILL CONTINUE WITH OTHERS - " + probeset.getProbesetName());
      return;
    }
  }
}
