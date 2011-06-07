package uk.ac.ebi.fgpt.hadoopUtils.pca.sequential;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.VectorWritable;

public class ReadVectors extends SequentialTool {
  public static void main(String[] args) throws IOException {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    
    // Add Options
    cliOptions.addOption(input);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("Read Vectors", cliOptions, true);
      return;
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      run(pathToInput);
    } catch (ParseException e) {
      formatter.printHelp("Read Vectors", cliOptions, true);
    }
  }
  
  public static void run(String stringToInput) throws IOException {
    run(stringToInput, null, true);
  }
  
  public static void run(String stringToInput, TreeMap<IntWritable,VectorWritable> mapToFill, boolean verbose) throws IOException {
    
    // Setup environment
    setup(stringToInput);
    
    // Create Reader
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, config);
    
    try {
      // Make temporary reusable objects
      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
      
      while (reader.next(key, value)) {
        if (mapToFill != null) {
          mapToFill.put(new IntWritable(key.get()), new VectorWritable(value.get()));
        }
        int cardinality = value.get().size();
        for (int i = 0; i < cardinality - 1; i++) {
          if (verbose) {
            System.out.print(value.get().get(i) + "\t");
          }
        }
        if (verbose) {
          System.out.print(value.get().get(cardinality - 1));
          System.out.println();
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
