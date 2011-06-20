package uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

public class DistributedSortProbesJob extends Configured implements Tool {
  private static Logger log = LoggerFactory.getLogger(DistributedSortProbesJob.class);
  public static class MapToVector extends Mapper<LongWritable,Text,Text,VectorWritable> {
    @Override
    protected void map(LongWritable key,
                       Text value,
                       Mapper<LongWritable,Text,Text,VectorWritable>.Context context) throws IOException,
                                                                                     InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String probeName = tokenizer.nextToken();
      probeName = probeName.replaceAll("\"","");
      
      try {
        DenseVector probeValues = StringToVector.convert(line, 1);
        context.write(new Text(probeName), new VectorWritable(probeValues));
      } catch (NumberFormatException e) {
        context.setStatus("A line was ignored: " + line.substring(0, 100));
        log.info("A line was ignored: " + line.substring(0, 100));
      }
    }
  }
  
  public static class ReduceToFile extends Reducer<Text,VectorWritable,Text,ProbesetWritable> {
    @Override
    protected void reduce(Text key, // Key was previous column and now is a row
                          Iterable<VectorWritable> value,
                          Reducer<Text,VectorWritable,Text,ProbesetWritable>.Context context) throws IOException,
                                                                                             InterruptedException {
      ArrayList<Vector> arrayListOfVectors = new ArrayList<Vector>();
      
      Iterator<VectorWritable> iterator = value.iterator();
      
      while (iterator.hasNext()) {
        VectorWritable e = iterator.next();
        arrayListOfVectors.add(e.get());
      }
      Vector[] vectorArray = new Vector[arrayListOfVectors.size()];
      arrayListOfVectors.toArray(vectorArray);
      
      context.write(key, new ProbesetWritable(new Probeset(key.toString(), vectorArray)));
    }
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DistributedSortProbesJob(), args);
  }
  
  public int run(String[] args) throws Exception {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.txt").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output <Text,ProbesetWritable>").create("o");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("SortProbesJob", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      run(pathToInput, pathToOutput);
    } catch (ParseException e) {
      formatter.printHelp("SortProbesJob", cliOptions, true);
    }
    return 0;
  }
  
  // Step 00 - Read in the Probeset as a txt file
  // Step 01 - MapA - Take each line and make it into a vector
  // Step 02 - ReduceA - Reduce the lines with the same key(probeset name) into a probeset object.
  // Step 03 - MapB - Take each probset object and map it to a node. On each node, perform IRLS
  // Step 04 - ReduceB - Filter the results with the key as probeset name and the value is an IRLSOutput
  
  public void run(String pathToInput, String pathToOutput) throws IOException,
                                                          InterruptedException,
                                                          ClassNotFoundException {
    Job job = new Job(getConf());
    job.setJobName("Sorting probes into probesets: " + pathToInput + " output -> " + pathToOutput);
    job.setJarByClass(DistributedSortProbesJob.class);
    
    Path inputPath = new Path(pathToInput);
    Path outputPath = new Path(pathToOutput);
    
    // Part 1 - Create Probesets
    
    // Set Mappers and Reducers
    job.setMapperClass(MapToVector.class);
    job.setReducerClass(ReduceToFile.class);
    
    // Set Input and Output Paths
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setMapOutputKeyClass(Text.class); // Probeset Name
    job.setMapOutputValueClass(VectorWritable.class); // Probe Vector
    
    // Establish the Output of the Job
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ProbesetWritable.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.waitForCompletion(true);
  }
}
