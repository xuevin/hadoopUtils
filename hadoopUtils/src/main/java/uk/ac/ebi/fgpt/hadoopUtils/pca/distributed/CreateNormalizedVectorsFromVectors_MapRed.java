package uk.ac.ebi.fgpt.hadoopUtils.pca.distributed;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;

import uk.ac.ebi.fgpt.hadoopUtils.pca.math.VectorOperations;

/**
 * This application creates a SequenceFile with <IntWritable,VectorWritable> keyvalue pairs. arg[0] is the
 * input file arg[1] is the output file
 * 
 * @author vincent@ebi.ac.uk
 * 
 */
public class CreateNormalizedVectorsFromVectors_MapRed extends Configured implements Tool {
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CreateNormalizedVectorsFromVectors_MapRed(), args);
    System.exit(res);
  }
  
  public static class Map extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    @Override
    protected void map(IntWritable key,
                       VectorWritable value,
                       Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable>.Context context) throws IOException,
                                                                                                     InterruptedException {
      context.write(key, new VectorWritable(VectorOperations.normalize(value.get())));
    }
  }
  
  public static class Reduce extends Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    @Override
    protected void reduce(IntWritable key,
                          Iterable<VectorWritable> value,
                          Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable>.Context context) throws IOException,
                                                                                                         InterruptedException {
      Iterator<VectorWritable> iterator = value.iterator();
      context.write(key, iterator.next());
    }
  }
  
  public int run(String[] args) throws Exception {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output").create("o");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("Create Normalized Vector From Vectors Using Map Reduce", cliOptions, true);
      System.exit(0);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      run(new Path(pathToInput), new Path(pathToOutput));
    } catch (ParseException e) {
      formatter.printHelp("Create Normalized Vector From Vectors Using Map Reduce", cliOptions, true);
      
    }
    return 0;
  }
  
  public void run(Path inputPath, Path outputPath) throws IOException,
                                                  InterruptedException,
                                                  ClassNotFoundException {
    Job job = new Job(getConf());
    job.setJarByClass(CreateNormalizedVectorsFromVectors_MapRed.class);
    job.setJobName("Create Normalized Vector " + "From Vectors Using Map Reduce");
    
    // Set Input and Output Paths
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    // Establish the Output of the Job
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    
    // Set Mappers and Reducers
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
    job.waitForCompletion(true);
  }
  
}
