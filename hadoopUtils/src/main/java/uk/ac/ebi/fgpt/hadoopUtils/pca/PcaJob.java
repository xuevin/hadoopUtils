package uk.ac.ebi.fgpt.hadoopUtils.pca;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.SequentialTool;

public class PcaJob extends SequentialTool {
  private static Logger log = LoggerFactory.getLogger(PcaJob.class);
  
  public static void main(String[] args) throws Exception {
    
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
    Option reduces = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("reduces")
        .withDescription("the number of reduces that should run").create("r");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(temp);
    cliOptions.addOption(matrix);
    cliOptions.addOption(reduces);
    
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
      int numReduces = Integer.parseInt(cmd.getOptionValue("r"));
//      run(pathToInput, pathToOutput, pathToDesign, pathToTemp, numReduces);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
  }
  
  public void run() {
    
    // Step 1 - Normalize Data
    // Step 2 
    


  }
}