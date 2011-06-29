package uk.ac.ebi.fgpt.hadoopUtils.microarray.singlejob;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob.Map;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob.Reduce;

public class IRLSJob extends Configured implements Tool {
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
      run(new Path(pathToInput), new Path(pathToOutput), new Path(pathToDesign), new Path(pathToTemp),
        numReduces, true);
    } catch (ParseException e) {
      formatter.printHelp("IrlsJob", cliOptions, true);
    }
    return 0;
  }
  public void run(Path inputPath,
                  Path outputPath,
                  Path pathToDesign,
                  Path tempPath,
                  int numReduces,
                  boolean waitForCompletion) throws IOException, InterruptedException, ClassNotFoundException{
   
    loadIntoCache(getConf());
    
    getConf().set("temp", tempPath.toString());
    getConf().set("design", pathToDesign.toString());
    // getConf().set("mapred.child.java.opts", "-Xmx30000m");
    getConf().set("mapred.task.timeout", "10800000"); // Time out after 3 hours
    
    Job job = new Job(getConf());
    job.setNumReduceTasks(numReduces);
    
    job.setJobName("Performing IRLS on : " + inputPath);
    job.setJarByClass(DistributedIrlsJob.class);
    
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
    
    if (waitForCompletion) {
      job.waitForCompletion(true);
    } else {
      job.submit();
    }
    
  }
  public static void loadIntoCache(Configuration conf){
    try {
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/11.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/11.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/11.prod"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/13.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/13.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/13.prod"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/14.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/14.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/14.prod"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/15.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/15.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/15.prod"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/16.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/16.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/16.prod"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/20.des"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/20.des.t"), conf);
      DistributedCache.addCacheFile(new URI("/user/vincent/RMA/design/20.prod"), conf);
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
