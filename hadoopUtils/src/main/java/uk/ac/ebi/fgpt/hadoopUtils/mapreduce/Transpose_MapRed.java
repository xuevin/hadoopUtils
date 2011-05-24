package uk.ac.ebi.fgpt.hadoopUtils.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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
public class Transpose_MapRed extends Configured implements Tool {
  
  private static final Logger log = LoggerFactory.getLogger(Transpose_MapRed.class);
  
  public static class Map extends
      Mapper<IntWritable,VectorWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable> {
    @Override
    protected void map(IntWritable key,
                       VectorWritable value,
                       Mapper<IntWritable,VectorWritable,IntWritable,DistributedRowMatrix.MatrixEntryWritable>.Context context) throws IOException,
                                                                                                                               InterruptedException {
      
      int startCol = Integer.parseInt(context.getConfiguration().get("startCol"));
      int endCol = Integer.parseInt(context.getConfiguration().get("endCol"));
      
      log.info("Working on Columns:" + startCol + "-" + endCol);
      
      // A down to earth explanation of what is happening:
      // First this Map process retrieves a single row(A Vector) of the
      // matrix.
      // It iterates through the matrix from left to right and if the
      // index is in between
      // startCol and endCol, then it sends some output to the reduce. The
      // output
      // of the map is a
      // == Key which is the old column and the new row
      // == Entry is a value with the new column and no row. The new row
      // info is stored in the key
      
      DistributedRowMatrix.MatrixEntryWritable entry = new DistributedRowMatrix.MatrixEntryWritable();
      Iterator<Vector.Element> it = value.get().iterator();
      int newColumn = key.get(); // Old Row (designated by key) becomes
      // the new column
      entry.setCol(newColumn);
      entry.setRow(-1); // output "row" is captured in the key
      
      while (it.hasNext()) {
        Vector.Element e = it.next();
        if (e.index() >= startCol && e.index() <= endCol) {
          key.set(e.index());// reuse of the same object. This column
          // index is now the new Row
          entry.setVal(e.get());
          context.write(key, entry);
        }
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
    int res = ToolRunner.run(new Configuration(), new Transpose_MapRed(), args);
    System.exit(res);
  }
  
  public static void runMain(Configuration conf, String args[]) throws Exception {
    ToolRunner.run(conf, new Transpose_MapRed(), args);
  }
  
  public int run(String[] args) throws Exception {
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use given file as input").withLongOpt("input").create("i");
    Option output = OptionBuilder.withArgName("output.seqFile").hasArg().isRequired().withLongOpt("output")
        .withDescription("use given file as output").create("o");
    Option splits = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numSplits")
        .withDescription("The number of splits").create("s");
    Option rows = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numReduceTasks")
        .withDescription("The number of rows").create("nr");
    Option cols = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numReduceTasks")
        .withDescription("The number of columns").create("nc");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(rows);
    cliOptions.addOption(cols);
    cliOptions.addOption(splits);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("Create Normalized Vector From Vectors Using Map Reduce", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      int numSplits = Integer.parseInt(cmd.getOptionValue("s"));
      int numCols = Integer.parseInt(cmd.getOptionValue("nc"));
      int numRow = Integer.parseInt(cmd.getOptionValue("nr"));
      run(pathToInput, pathToOutput, numRow, numCols, numSplits);
    } catch (ParseException e) {
      formatter.printHelp("Create Normalized Vector From Vectors Using Map Reduce", cliOptions, true);
    }
    return 0;
  }
  
  public void run(String pathToInput, String pathToOutput, int numRows, int numCols, int splits) throws IOException,
                                                                                                InterruptedException,
                                                                                                ClassNotFoundException {
    
    int roughSplitSize = (int) (numCols / splits);
    
    System.out.println("The matrix with " + numCols + " columns will be split into " + splits
                       + " groups with approximately " + roughSplitSize + " elements in each group.");
    if (roughSplitSize < 0) {
      System.err.println("There are more splits than there are columns!");
      return;
    }
    int endPosition = -1;
    
    getConf().set("numCols", numCols + "");
    getConf().set("numRows", numRows + "");
    
    Path matrixInputPath = new Path(pathToInput);
    
    LinkedHashSet<Path> transposeParts = new LinkedHashSet<Path>();
    
    // =================
    // Create the parts of the file
    // =================
    for (int i = 0; i < splits; i++) {
      Path matrixOutputPath = new Path(pathToOutput + i);
      transposeParts.add(matrixOutputPath);
      
      log.info("Starting split:" + i);
      
      getConf().set("startCol", endPosition + 1 + "");
      if (i == (splits - 1)) {
        // last split
        endPosition = numCols;
        getConf().set("endCol", (numCols - 1) + "");
      } else {
        endPosition += roughSplitSize;
        getConf().set("endCol", endPosition + "");
      }
      
      Job job = new Job(getConf());
      job.setJobName("TransposeJob: " + matrixInputPath + " transpose -> " + matrixOutputPath);
      job.setJarByClass(Transpose_MapRed.class);
      job.setJobName("Create transposed matrix " + "from vectors using map reduce");
      
      // Set Mappers and Reducers
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Combiner.class);
      
      // Set Input and Output Paths
      FileInputFormat.setInputPaths(job, matrixInputPath);
      FileOutputFormat.setOutputPath(job, matrixOutputPath);
      System.out.println(pathToOutput.toString());
      
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
    // ===============
    // Combine the parts back to one large file
    // ===============
    
    // Create writer
    SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(getConf()), getConf(), new Path(
        pathToOutput), IntWritable.class, VectorWritable.class);
    
    for (Path path : transposeParts) {
      Path pathToPart = new Path(path, "part-r-00000");
      // Create Reader
      SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(getConf()), pathToPart, getConf());
      try {
        // Make temporary reusable objects
        IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
        VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
        
        while (reader.next(key, value)) {
          writer.append(key, value);
        }
        reader.close();
        
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    writer.close();
    
    System.out.println("Done");
  }
}
