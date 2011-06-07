package uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedDesignMatrixFactory;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrls;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.SequentialTool;

/**
 * This class creates a design matrix depending on how many probes/samples the user specifies.
 * 
 * @author vincent@ebi.ac.uk
 *
 */
public class CreateDesignMatrix extends SequentialTool {
  
  public static void main(String[] args) throws IOException {
    // Create Options
    Options cliOptions = new Options();
    
    Option probes = OptionBuilder.withArgName("num").hasArg().isRequired().withDescription(
      "Create design matrix With <num> of probes").withLongOpt("numProbes").create("np");
    
    Option samples = OptionBuilder.withArgName("num").hasArg().isRequired().withDescription(
      "Create design matrix With <num> of samples").withLongOpt("numSamples").create("ns");
    
    Option designPath = OptionBuilder.withArgName("path").hasArg().isRequired().withLongOpt("designPath")
        .withDescription("Use given directory to store design matricies").create("d");
    
    // Add Options
    cliOptions.addOption(probes);
    cliOptions.addOption(samples);
    cliOptions.addOption(designPath);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("createDesign", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      int numSamples = Integer.parseInt(cmd.getOptionValue("ns"));
      int numProbes = Integer.parseInt(cmd.getOptionValue("np"));
      String pathToDesign = cmd.getOptionValue("d");
      
      run(numProbes, numSamples, pathToDesign);
    } catch (ParseException e) {
      formatter.printHelp("createDesign", cliOptions, true);
    }
  }
  
  private static void run(int numProbes, int numSamples, String pathToDesign) throws IOException {
    Configuration conf = new Configuration();
    
    Path designMatrixPath = new Path(pathToDesign, numProbes + ".des");
    Path designMatrixTransposePath = new Path(pathToDesign, numProbes + ".des.t");
    Path productPath = new Path(pathToDesign, numProbes + ".prod");
    
    writeNewDesignMatrixToHDFS(numProbes, numSamples, designMatrixPath, conf);
    writeNewDesignMatrixTranposeToHDFS(numProbes, numSamples, designMatrixTransposePath, conf);
    writeNewProductToHDFS(designMatrixPath, (numSamples + numProbes - 1), productPath, conf);
  }
  
  public static void writeNewDesignMatrixTranposeToHDFS(int numProbes,
                                                        int numSamples,
                                                        Path designMatrixTranposePath,
                                                        Configuration conf) throws IOException {
    DesignMatrixFactory designMatrixFactory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix designMatrixTranspose = designMatrixFactory.getDesignMatrixTranspose();
    
    FileSystem fs = FileSystem.get(conf);
    fs.makeQualified(designMatrixTranposePath);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixTranposePath,
      IntWritable.class, VectorWritable.class);
    for (int i = 0; i < designMatrixTranspose.size()[0]; i++) {
      writer.append(new IntWritable(i), new VectorWritable(designMatrixTranspose.getRow(i)));
    }
    writer.close();
    
  }
  
  public static void writeNewDesignMatrixToHDFS(int numProbes,
                                                int numSamples,
                                                Path designMatrixPath,
                                                Configuration conf) throws IOException {
    DesignMatrixFactory designMatrixFactory = new DesignMatrixFactory(numProbes, numSamples);
    Matrix designMatrixObject = designMatrixFactory.getDesignMatrix();
    FileSystem fs = FileSystem.get(conf);
    
    fs.makeQualified(designMatrixPath);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, designMatrixPath, IntWritable.class,
      VectorWritable.class);
    for (int i = 0; i < designMatrixObject.size()[0]; i++) {
      writer.append(new IntWritable(i), new VectorWritable(designMatrixObject.getRow(i)));
    }
    writer.close();
  }
  
  public static void writeNewProductToHDFS(Path designMatrixPath,
                                           int numCols,
                                           Path productPath,
                                           Configuration initialConf) throws IOException {
    Configuration conf = MatrixMultiplicationJob.createMatrixMultiplyJobConf(initialConf, designMatrixPath,
      designMatrixPath, productPath, numCols);
    JobClient.runJob(new JobConf(conf));
  }
}
