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

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.math.IterativelyReweightedLeastSquares;

/**
 * This version of IterativelyReweightedLeastSquares sends out a probeset to each node. On the node, it
 * performs IRLS (with sequential linear algebra) 
 * 
 * This method is like load balencing.
 * 
 * @author Vincent Xue
 * 
 */
public class IterativelyReweightedLeastSquaresJob extends Configured implements Tool {
  public static class Map extends Mapper<Text,ProbesetWritable,Text,IrlsOutputWritable> {
    @Override
    protected void map(Text key,
                       ProbesetWritable value,
                       Mapper<Text,ProbesetWritable,Text,IrlsOutputWritable>.Context context) throws IOException,
                                                                                             InterruptedException {
      
      IrlsOutput output = IterativelyReweightedLeastSquares.run(value.get(), 0.0001, 20);
      context.write(key, new IrlsOutputWritable(output));
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
  
  // static class MultiFileOutput extends MultipleTextOutputFormat<Text,Text> {
  // protected String generateFileNameForKeyValue(Text key, Text value, String name) {
  // return key.toString();
  // }
  // }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new IterativelyReweightedLeastSquaresJob(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input<text,ProbeSetWritable>").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output <Text,IrlsOutputWritable>").create("o");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    
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
      run(pathToInput, pathToOutput);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
    return 0;
  }
  
  public void run(String pathToInput, String pathToOutput) throws IOException,
                                                          InterruptedException,
                                                          ClassNotFoundException {
    
    Job job = new Job(getConf());
    job.setJobName("Performing IRLS on : " + pathToInput + " output -> " + pathToOutput);
    job.setJarByClass(IterativelyReweightedLeastSquaresJob.class);
    
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
    
    // Establish the Output of the Job
    job.setOutputKeyClass(Text.class); // Probeset Name
    job.setOutputValueClass(IrlsOutputWritable.class); // Vector of estimates and Vector of weights
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.waitForCompletion(true);
    
  }
}
