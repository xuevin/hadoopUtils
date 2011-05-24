package uk.ac.ebi.fgpt.hadoopUtils.microarray;

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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrls;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.SequentialTool;

public class SequentialIRLSJobLauncher extends SequentialTool {
  
  public static void main(String[] args) throws IOException {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input<text,ProbeSetWritable>").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output <Text,IrlsOutputWritable>").create("o");
    Option temp = OptionBuilder.withArgName("tempDir").hasArg().isRequired().withLongOpt("temp")
        .withDescription("use given HDFS dir as output").create("t");
    Option matrix = OptionBuilder.withArgName("designDir").hasArg().isRequired().withLongOpt("designDir")
        .withDescription("use given HDFS dir for storing Design Matricies").create("d");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(temp);
    cliOptions.addOption(matrix);
    
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
      run(pathToInput, pathToOutput, pathToDesign, pathToTemp);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
  }
  
  public static void run(String pathToInput, String pathToOutput, String pathToDesign, String pathToTemp) throws IOException {
//    setup(pathToInput);
//    config.set("temp", pathToTemp);
//    config.set("design", pathToDesign);
//    config.set("mapred.child.java.opts", "-Xmx2000m");
//    
//    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, config);
//    try {
//      // Make temporary reusable objects
//      Text key = (Text) reader.getKeyClass().newInstance();
//      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
//      while (reader.next(key, value)) {
//        Configuration conf = new Configuration(config);
//        DistributedIrls.run(value.get(), 0.0001, 20, conf);
//        // Maybe when it finishes, you can send the writer to it so that it can append to the file.
//      }
//      reader.close();
//    } catch (InstantiationException e) {
//      e.printStackTrace();
//    } catch (IllegalAccessException e) {
//      e.printStackTrace();
//    }
  }
}
