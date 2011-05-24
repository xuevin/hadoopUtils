package uk.ac.ebi.fgpt.hadoopUtils.sequential;

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

public class Tail extends SequentialTool {
  public static void main(String[] args) throws IOException {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output").create("o");
    Option numVectors = OptionBuilder.withArgName("n").hasArg().isRequired().withLongOpt("topVectors")
        .withDescription("Extract the top n vectors").create("t");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(numVectors);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("tail", cliOptions, true);
      return;
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      int numberOfVectors = Integer.parseInt(cmd.getOptionValue('t'));
      run(pathToInput, pathToOutput, numberOfVectors);
      
    } catch (ParseException e) {
      formatter.printHelp("tail", cliOptions, true);
    }
  }
  
  public static void run(String stringToInput, String stringToOutput, int numVectors) throws IOException {
    setup(stringToInput, stringToOutput);
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, config);
    
    try {
      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
      
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, config, outputPath, IntWritable.class,
        VectorWritable.class);
      
      TreeMap<Integer,VectorWritable> treeMap = new TreeMap<Integer,VectorWritable>();
      
      // Find the last n keys
      while (reader.next(key, value)) {
        // If more vectors than needed, discard the the ones before it
        if (treeMap.size() > numVectors - 1) {
          treeMap.pollFirstEntry();
          treeMap.put(key.get(), new VectorWritable(value.get()));
        } else {
          treeMap.put(key.get(), new VectorWritable(value.get()));
        }
      }
      // Reindex the matrix to start at zero
      int index = 0;
      for (Integer keyValue : treeMap.keySet()) {
        writer.append(new IntWritable(index), treeMap.get(keyValue));
        index++;
      }
      writer.close();
      reader.close();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
