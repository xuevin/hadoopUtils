package uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential;

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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Vector;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.SequentialTool;

public class ReadProbesets extends SequentialTool {
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
      formatter.printHelp("readProbesets", cliOptions, true);
      return;
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      run(pathToInput);
    } catch (ParseException e) {
      formatter.printHelp("readProbesets", cliOptions, true);
    }
  }
  
  public static void run(String stringToInput) throws IOException {
    run(stringToInput, null, true);
  }
  
  public static void run(String stringToInput, TreeMap<Text,ProbesetWritable> mapToFill, boolean verbose) throws IOException {
    //TODO FIXME -- This setup should be in the main class. Also, Use Path instead of String
    // Setup environment
    setupandAssertInputPathIsFile(stringToInput);
    
    // Create Reader
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, config);
    
    try {
      // Make temporary reusable objects
      Text key = (Text) reader.getKeyClass().newInstance();
      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
      
      while (reader.next(key, value)) {
        if (mapToFill != null) {
          mapToFill.put(new Text(key.toString()), new ProbesetWritable(value.get()));
        }
        if (verbose) {
          StringBuilder builder = new StringBuilder();
          String name = key.toString();
          Probeset probeset = value.get();
          // For each vector, spit it out to stdout
          for (int i = 0; i < probeset.getArrayOfVectors().length; i++) {
            builder.append(name + "\t");
            
            Vector vec = probeset.getArrayOfVectors()[i];
            for (int j = 0; j < vec.size(); j++) {
              builder.append(vec.get(j));
              builder.append("\t");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append("\n");
          }
          System.out.print(builder.toString());
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
