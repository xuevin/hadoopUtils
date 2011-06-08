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
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This application creates a SequenceFile with <IntWritable,VectorWritable> keyvalue pairs. arg[0] is the
 * input file arg[1] is the output file
 * 
 * @author vincent@ebi.ac.uk
 * 
 */
public class Transpose_newAPI extends Configured implements Tool {
  
  private static final Logger log = LoggerFactory.getLogger(Transpose_newAPI.class);
  
  public static class Map extends
      Mapper<IntWritable,VectorWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable> {
    @Override
    protected void map(IntWritable key,
                       VectorWritable value,
                       Mapper<IntWritable,VectorWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable>.Context context) throws IOException,
                                                                                                                               InterruptedException {
      
      DistributedRowMatrix.MatrixEntryWritable entry = new DistributedRowMatrix.MatrixEntryWritable();
      Iterator<Vector.Element> it = value.get().iterator();
      int newColumn = key.get();
      entry.setCol(newColumn);
      entry.setRow(-1); // output "row" is captured in the key
      
      while (it.hasNext()) {
        Vector.Element e = it.next();
        key.set(e.index());
        entry.setVal(e.get());
        context.write(key, entry);
      }
    }
  }
  
  public static class Reduce extends
      Reducer<IntWritable,DistributedRowMatrix.MatrixEntryWritable,IntWritable,VectorWritable> {
    @Override
    protected void reduce(IntWritable key, // Key was previous column and now is a row
                          Iterable<DistributedRowMatrix.MatrixEntryWritable> value,
                          Reducer<IntWritable,DistributedRowMatrix.MatrixEntryWritable,IntWritable,VectorWritable>.Context context) throws IOException,
                                                                                                                                   InterruptedException {
      
      // When the reducer comes along it has a set of elements which all
      // have the same new row.
      // It will take the elements and then put them in the right order
      // (based on the cols)
      // Now all the columns will be in the right order and so it will
      // print it out
      
      int newNumCols = Integer.parseInt(context.getConfiguration().get("numRows"));
      RandomAccessSparseVector tmp = new RandomAccessSparseVector(newNumCols, 100);
      Iterator<DistributedRowMatrix.MatrixEntryWritable> iterator = value.iterator();
      while (iterator.hasNext()) {
        DistributedRowMatrix.MatrixEntryWritable e = iterator.next();
        tmp.setQuick(e.getCol(), e.getVal()); // For this horizontal
        // vector, set the
        // column index
        // (which used to be a row) to the value
      }
      SequentialAccessSparseVector outVector = new SequentialAccessSparseVector(tmp);
      context.write(key, new VectorWritable(outVector));
    }
  }
  
  public static class Combiner
      extends
      Reducer<IntWritable,DistributedRowMatrix.MatrixEntryWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable> {
    public void reduce(IntWritable key, // Key was previous column and now is a row
                       Iterable<DistributedRowMatrix.MatrixEntryWritable> value,
                       Reducer<IntWritable,DistributedRowMatrix.MatrixEntryWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable>.Context context) throws IOException,
                                                                                                                                                          InterruptedException {
      
      Iterator<DistributedRowMatrix.MatrixEntryWritable> iterator = value.iterator();
      while (iterator.hasNext()) {
        context.write(key, iterator.next());
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Transpose_newAPI(), args);
    System.exit(res);
  }
  
  public static void runMain(Configuration conf, String args[]) throws Exception {
    ToolRunner.run(conf, new Transpose_newAPI(), args);
  }
  
  public int run(String[] args) throws Exception {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given directory as output").create("o");
    Option rows = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numRows")
        .withDescription("The number of rows").create("nr");
    Option cols = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numColumns")
        .withDescription("The number of columns").create("nc");
    Option reduces = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("reduces")
    .withDescription("The number of reducers").create("r");

    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(rows);
    cliOptions.addOption(cols);
    cliOptions.addOption(reduces);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("Transpose_newAPI", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      int numCols = Integer.parseInt(cmd.getOptionValue("nc"));
      int numRow = Integer.parseInt(cmd.getOptionValue("nr"));
      int numReduces = Integer.parseInt(cmd.getOptionValue("r"));
      run(new Path(pathToInput), new Path(pathToOutput), numRow, numCols, numReduces);
      
    } catch (ParseException e) {
      formatter.printHelp("Transpose_newAPI", cliOptions, true);
    }
    return 0;
  }
  
  public void run(Path inputPath, Path outputPath, int numRows, int numCols, int numReduces) throws IOException,
                                                                                            InterruptedException,
                                                                                            ClassNotFoundException {
    getConf().set("numRows", numRows + "");
    
    Job job = new Job(getConf());
    job.setJobName("TransposeJob: " + inputPath + " transpose -> " + outputPath);
    job.setJarByClass(Transpose_newAPI.class);
    
    job.setNumReduceTasks(numReduces);
    
    // Set Mappers and Reducers
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combiner.class);
    
    // Set Input and Output Paths
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DistributedRowMatrix.MatrixEntryWritable.class);
    
    // Establish the Output of the Job
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    // Maybe I don't need this
    job.waitForCompletion(true);
  }
}
