package uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed;

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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;

/**
 * This performs IRLS using a distributed model. Each node receives probeset but the matrix it creates is also
 * distributed. Matrix multiplication follows the map/reduce paradigm.
 * 
 * Perhaps this does not work because the Task Tracker sends this map tasks to many nodes, without waiting for
 * the first one to complete.
 * 
 * @author Vincent Xue
 * 
 */
public class DistributedIrlsJob extends Configured implements Tool {
  private static Logger log = LoggerFactory.getLogger(DistributedIrlsJob.class);
  
  public static class Map extends Mapper<Text,ProbesetWritable,Text,IrlsOutputWritable> {
    private static final int MAXNUMBERPROBES = 30;
    
    @Override
    protected void map(Text key,
                       ProbesetWritable value,
                       Mapper<Text,ProbesetWritable,Text,IrlsOutputWritable>.Context context) throws IOException,
                                                                                             InterruptedException {
      
      if (value.get().getNumProbes() < MAXNUMBERPROBES) {
        IrlsOutput output = DistributedIrls.run(value.get(), 0.0001, 20, context);
        context.write(key, new IrlsOutputWritable(output));
      } else {
        log.warn("SKIPPED: " + value.get().getProbesetName());
      }
    }
  }
  
  public static class Reduce extends Reducer<Text,IrlsOutputWritable,Text,IrlsOutputWritable> {
    @Override
    protected void reduce(Text key,
                          Iterable<IrlsOutputWritable> value,
                          Reducer<Text,IrlsOutputWritable,Text,IrlsOutputWritable>.Context context) throws IOException,
                                                                                                   InterruptedException {
      Iterator<IrlsOutputWritable> iterator = value.iterator();
      while (iterator.hasNext()) {
        context.write(key, iterator.next());
      }
      
    }
  }
  
  // public static class Map extends Mapper<Text,ProbesetWritable,Text,ProbesetWritable> {
  // @Override
  // protected void map(Text key,
  // ProbesetWritable value,
  // Mapper<Text,ProbesetWritable,Text,ProbesetWritable>.Context context) throws IOException,
  // InterruptedException {
  //      
  // context.write(key, value);
  // }
  // }
  //  
  // public static class Reduce extends Reducer<Text,ProbesetWritable,Text,IrlsOutputWritable> {
  // @Override
  // protected void reduce(Text key,
  // Iterable<ProbesetWritable> value,
  // Reducer<Text,ProbesetWritable,Text,IrlsOutputWritable>.Context context) throws IOException,
  // InterruptedException {
  // Iterator<ProbesetWritable> iterator = value.iterator();
  // while (iterator.hasNext()) {
  // IrlsOutput output = DistributedIrls.run(iterator.next().get(), 0.0001, 20, context);
  // context.write(key, new IrlsOutputWritable(output));
  // }
  //      
  // }
  // }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DistributedIrlsJob(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
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
      run(pathToInput, pathToOutput, pathToDesign, pathToTemp, numReduces);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
    return 0;
  }
  
  public void run(String pathToInput,
                  String pathToOutput,
                  String pathToDesign,
                  String pathToTemp,
                  int numReduces) throws IOException, InterruptedException, ClassNotFoundException {
    getConf().set("temp", pathToTemp);
    getConf().set("design", pathToDesign);
    getConf().set("mapred.child.java.opts", "-Xmx30000m");
    getConf().set("mapred.task.timeout", "10800000"); // Time out after 3 hours
    
    Job job = new Job(getConf());
    job.setNumReduceTasks(numReduces);
    
    job.setJobName("Performing IRLS on : " + pathToInput + " output -> " + pathToOutput);
    job.setJarByClass(DistributedIrlsJob.class);
    
    Path inputPath = new Path(pathToInput);
    Path outputPath = new Path(pathToOutput);
    
    // Set Mappers and Reducers
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    // Set Input and Output Paths
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setMapOutputKeyClass(Text.class); // Probeset Name
    job.setMapOutputValueClass(IrlsOutputWritable.class); // Vector of estimates and Vector of weights
    // job.setMapOutputValueClass(ProbesetWritable.class);
    
    // Establish the Output of the Job
    job.setOutputKeyClass(Text.class); // Probeset Name
    job.setOutputValueClass(IrlsOutputWritable.class); // Vector of estimates and Vector of weights
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.waitForCompletion(true);
    
  }
}
