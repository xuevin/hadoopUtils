package uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.SequentialTool;

public class CreateDesignMatrixFromProbesetWritable extends SequentialTool {
  private static Logger log = LoggerFactory.getLogger(CreateDesignMatrixFromProbesetWritable.class);
  
  public static void main(String[] args) throws IOException {
    // Create Options
    Options cliOptions = new Options();
    
    Option probesetWritable = OptionBuilder.withArgName("probesetWritable").hasArg().isRequired()
        .withDescription("The directory of the probesetWritable").withLongOpt("probesetWritable").create("p");
    
    Option designPath = OptionBuilder.withArgName("path").hasArg().isRequired().withLongOpt("designPath")
        .withDescription("Use given directory to store design matricies").create("d");
    
    // Add Options
    cliOptions.addOption(probesetWritable);
    cliOptions.addOption(designPath);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("createDesignFromProbesetWritable", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String probesetWritableDir = cmd.getOptionValue("p");
      String pathToDesign = cmd.getOptionValue("d");
      
      run(probesetWritableDir, pathToDesign);
    } catch (ParseException e) {
      formatter.printHelp("createDesignFromProbesetWritable", cliOptions, true);
    }
  }
  
  private static void run(String probesetWritableDir, String pathToDesign) throws IOException {
    // Setup environment
    setup(probesetWritableDir);
    
    // Create Reader
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, config);
    
    try {
      // Make temporary reusable objects
      Text key = (Text) reader.getKeyClass().newInstance();
      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
      
      while (reader.next(key, value)) {
        int numProbes = value.get().getNumProbes();
        int numSamples = value.get().getNumSamples();
        Configuration conf = new Configuration();
        
        Path designMatrixPath = new Path(pathToDesign, numProbes + ".des");
        Path designMatrixTransposePath = new Path(pathToDesign, numProbes + ".des.t");
        Path productPath = new Path(pathToDesign, numProbes + ".prod");
        
        if (!fs.exists(designMatrixPath)) {
          CreateDesignMatrix.writeNewDesignMatrixToHDFS(numProbes, numSamples, designMatrixPath, conf);
          log.info("Creating Design Matrix - " + numProbes);
        }
        if (!fs.exists(designMatrixTransposePath)) {
          CreateDesignMatrix.writeNewDesignMatrixTranposeToHDFS(numProbes, numSamples,
            designMatrixTransposePath, conf);
          log.info("Creating Design Matrix Transpose - " + numProbes);
          
        }
        if (!fs.exists(productPath)) {
          CreateDesignMatrix.writeNewProductToHDFS(designMatrixPath, (numSamples + numProbes - 1),
            productPath, conf);
          log.info("Creating Design Matrix - " + numProbes);
        }
      }
      reader.close();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    
  }
}
