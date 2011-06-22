package uk.ac.ebi.fgpt.hadoopUtils.microarray.executor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;

public class IRLSLoader {
  
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
    Option running = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("running")
    .withDescription("the number of threads that should run").create("r");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(temp);
    cliOptions.addOption(matrix);
    cliOptions.addOption(running);
    
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
      int numThreads = Integer.parseInt(cmd.getOptionValue("r"));

      run(new Path(pathToInput), new Path(pathToOutput), new Path(pathToDesign), new Path(pathToTemp),numThreads);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
  }
  
  public static void run(Path inputPath, Path outputPath, Path designPath, Path tempPath, int numThreads) throws IOException,
                                                                                         InterruptedException,
                                                                                         ClassNotFoundException {
    
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    
    Configuration conf = new Configuration();
    
    // getConf().set("mapred.child.java.opts", "-Xmx30000m");
    conf.set("mapred.task.timeout", "10800000"); // Time out after 3 hours
    
    FileSystem fs = FileSystem.get(conf);
    
    // Create Reader
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);
    
    // Make temporary reusable objects
    try {
      Text key = (Text) reader.getKeyClass().newInstance();
      ProbesetWritable value = (ProbesetWritable) reader.getValueClass().newInstance();
      while (reader.next(key, value)) {
        pool.execute(new IRLS_ProbesetCallable(conf, inputPath, outputPath, designPath, tempPath, key
            .toString()));
      }
      reader.close();
      
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    
  }
}
