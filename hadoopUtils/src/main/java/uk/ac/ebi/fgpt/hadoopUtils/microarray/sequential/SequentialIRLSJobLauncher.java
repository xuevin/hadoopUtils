package uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential;

import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.SequentialTool;

/**
 * This was when I tried to control how many probeset jobs went into the cluster at once.
 *
 */
public class SequentialIRLSJobLauncher extends SequentialTool {
  private static Logger log = LoggerFactory.getLogger(SequentialIRLSJobLauncher.class);
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("inputFile").hasArg().isRequired().withDescription(
      "use given file as input. Should be a <Text,ProbeSetWritable>").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given directory as output <Text,IrlsOutputWritable>").create("o");
    Option temp = OptionBuilder.withArgName("tempDir").hasArg().isRequired().withLongOpt("temp")
        .withDescription("use given HDFS dir as output").create("t");
    Option designMatrix = OptionBuilder.withArgName("designDir").hasArg().isRequired().withLongOpt(
      "designDir").withDescription("use given HDFS dir for storing Design Matricies").create("d");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(temp);
    cliOptions.addOption(designMatrix);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("IrlsJob", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      String pathToTemp = cmd.getOptionValue("t");
      String pathToDesign = cmd.getOptionValue("d");
      setupBothPathsAndAssertInputIsFile(pathToInput, pathToOutput);
      
      run(inputPath, outputPath, new Path(pathToDesign), new Path(pathToTemp), config);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
  }
  
  public static void run(Path inputPath, Path outputPath, Path designPath, Path tempPath, Configuration conf) throws IOException,
                                                                                                             InterruptedException,
                                                                                                             ClassNotFoundException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);
    
    try {
      // Make temporary reusable objects
      Text key = (Text) reader.getKeyClass().newInstance();
      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
      
      int counter = 0;
      while (reader.next(key, value)) {
        counter++;
        
        // If the output already exists, skip it!
        Path outputResultsPath = new Path(outputPath, key.toString());
        if (fs.exists(outputResultsPath)) {
          System.out.println("Skipping " + key.toString());
          continue;
        }
        
        System.out.println("Starting probeset: " + key.toString());
        // First write out the Probeset into HDFS.
        Path pathToProbeSetWritable = writeProbesetToHDFS(key, value, tempPath, conf);
        
        DistributedIrlsJob job = new DistributedIrlsJob();
        job.setConf(new Configuration());
        
        fs.makeQualified(outputResultsPath);
        Path irlsTemp = new Path(tempPath,"irlsTemp");
        
        if (!fs.exists(irlsTemp)) {
          fs.mkdirs(irlsTemp);
        }
        
        job.run(pathToProbeSetWritable, outputResultsPath, designPath, irlsTemp, 1, false);
        
        System.out.println("Submitted probeset: " + key.toString());
        // Wait for user to press enter
        if (counter >= 100) {
          System.out.println("Press Enter To Continue: ");
          Scanner sc = new Scanner(System.in);
          while (!sc.nextLine().equals(""))
            ;
          counter = 0;
        }
      }
      reader.close();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
  
  public static Path writeProbesetToHDFS(Text key, ProbesetWritable value, Path tempPath, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    
    Path probesetsFolder = new Path(tempPath, "probesets");
    if (!fs.exists(probesetsFolder)) {
      log.info("Making new directory for probeset");
      fs.mkdirs(probesetsFolder);
    }
    
    Path singleProbesetPath = new Path(probesetsFolder, key.toString());
    if (!fs.exists(singleProbesetPath)) {
      log.info("Writting probeset to new directory");
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, singleProbesetPath, Text.class,
        ProbesetWritable.class);
      writer.append(key, value);
      writer.close();
    }
    
    return singleProbesetPath;
  }
}
