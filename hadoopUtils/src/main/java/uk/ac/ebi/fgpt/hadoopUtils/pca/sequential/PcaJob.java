package uk.ac.ebi.fgpt.hadoopUtils.pca.sequential;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;
import org.apache.mahout.math.hadoop.decomposer.DistributedLanczosSolver;
import org.apache.mahout.math.hadoop.decomposer.DistributedLanczosSolver.DistributedLanczosSolverJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutputWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.ProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.CreateNormalizedVectorFromTxt_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.CreateNormalizedVectorsFromVectors_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.Transpose_newAPI;

public class PcaJob extends SequentialTool {
  private static Logger log = LoggerFactory.getLogger(PcaJob.class);
  
  public static void main(String[] args) throws Exception {
    
    // Create Options
    Options cliOptions = new Options();
    Option input = OptionBuilder.withArgName("input.seqFile").hasArg().isRequired().withDescription(
      "use the given file as Matrix to perform PCA on").withLongOpt("input").create("i");
    
    Option output = OptionBuilder.withArgName("outputDir").hasArg().isRequired().withLongOpt("output")
        .withDescription("use the given directory as the output").create("o");
    Option rows = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numRows")
        .withDescription("The number of rows").create("nr");
    Option cols = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("numColumns")
        .withDescription("The number of columns").create("nc");
    
    Option reduces = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("reduces")
        .withDescription("the number of reduces that should run for the transpose").create("r");
    
    Option rank = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("rank").withDescription(
      "the rank of the SVD").create("rnk");
    
    Option eigenVectors = OptionBuilder.withArgName("num").hasArg().isRequired().withLongOpt("eigenVectors")
        .withDescription("the number of output eigenVectors").create("e");
    
    // Add Options
    cliOptions.addOption(input);
    cliOptions.addOption(output);
    cliOptions.addOption(rows);
    cliOptions.addOption(cols);
    cliOptions.addOption(reduces);
    cliOptions.addOption(eigenVectors);
    cliOptions.addOption(rank);
    
    HelpFormatter formatter = new HelpFormatter();
    
    // Try to parse options
    CommandLineParser parser = new PosixParser();
    
    if (args.length <= 1) {
      formatter.printHelp("pcaJob", cliOptions, true);
      System.exit(1);
    }
    
    try {
      CommandLine cmd = parser.parse(cliOptions, args, true);
      String pathToInput = cmd.getOptionValue("i");
      String pathToOutput = cmd.getOptionValue("o");
      int numReduces = Integer.parseInt(cmd.getOptionValue("r"));
      int numRank = Integer.parseInt(cmd.getOptionValue("rnk"));
      int numRows = Integer.parseInt(cmd.getOptionValue("nr"));
      int numCols = Integer.parseInt(cmd.getOptionValue("nc"));
      int numEigenVectors = Integer.parseInt(cmd.getOptionValue("e"));
      setup(pathToInput, pathToOutput);
      run(inputPath, outputPath, numRows, numCols, numRank, numEigenVectors, numReduces, config);
    } catch (ParseException e) {
      formatter.printHelp("pcaJob", cliOptions, true);
    }
  }
  
  public static void run(Path inputMatrixPath,
                         Path outPath,
                         int numRows,
                         int numCols,
                         int rank,
                         int numEigenVectors,
                         int numReduces,
                         Configuration conf) throws Exception {
    log.info("Starting pcaJob...Output will consist of " + numEigenVectors + " columns and " + numRows
             + " rows.");
    
    FileSystem fs = FileSystem.get(conf);
    
    Path tempPath = new Path(outPath, "temp");
    
    // Step 1 - Normalize Data for each Vector
    log.info("Normalizing Matrix - " + numRows + " rows will be normalized");
    Path normalizedMatrixPath = new Path(outPath, "normalized");
    
    CreateNormalizedVectorsFromVectors_MapRed normalizeJob = new CreateNormalizedVectorsFromVectors_MapRed();
    normalizeJob.setConf(new Configuration());
    normalizeJob.run(inputMatrixPath, normalizedMatrixPath);
    
    // Step 2 - Transpose the Normalized Matrix
    log.info("Transposing the normalized matrix");
    Path normalizedTransposePath = new Path(outPath, "normalized.transpose");
    
    Transpose_newAPI tranposeJob = new Transpose_newAPI();
    tranposeJob.setConf(new Configuration());
    tranposeJob.run(normalizedMatrixPath, normalizedTransposePath, numRows, numCols, numReduces);
    
    // Step 3 - Run Singluar Value Decomposition
    log.info("Running Singluar Value Decomposition");
    DistributedLanczosSolver svdJob = new DistributedLanczosSolver();
    svdJob.setConf(new Configuration());
    // Run SVD on the original matrix.
    svdJob.run(normalizedMatrixPath, outPath, tempPath, null, numRows, numCols, false, rank);
    
    // The rawEigenVectorPath has <rank> number of vectors and <numCols> number of elements in each vector
    Path rawEigenVectorPath = new Path(outPath, DistributedLanczosSolver.RAW_EIGENVECTORS);
    
    if (!fs.exists(rawEigenVectorPath)) {
      log.warn("rawEigenVectorPath not found!");
    }
    
    // Step 4 Get the last N EigenVectors
    log.info("Fetch the Top N EigenVectors");
    Path topNEigenVectorPath = new Path(outPath, "topNEigenVectors");
    Tail.run(rawEigenVectorPath, topNEigenVectorPath, numEigenVectors, new Configuration());
    
    // Step 5
    log.info("Transpose the top N EigenVectors");
    Path topNEigenVectorsTransposed = new Path(outPath, "topNEigenVectors.tranpose");
    Transpose_newAPI tranposeTheTopNVectorsJob = new Transpose_newAPI();
    tranposeTheTopNVectorsJob.setConf(new Configuration());
    tranposeTheTopNVectorsJob.run(topNEigenVectorPath, topNEigenVectorsTransposed, numEigenVectors, numRows,
      numReduces);
    
    // Step 6 Matrix multiplication between original matrix and this transposed
    log.info("Performing final Matrix multiplication");
    // Again, I switched the rows and columns
    DistributedRowMatrix originalTranspose = new DistributedRowMatrix(normalizedTransposePath, tempPath,
        numCols, numRows);
    originalTranspose.setConf(conf);
    
    DistributedRowMatrix topNEigenVectorsTranpose = new DistributedRowMatrix(topNEigenVectorsTransposed,
        tempPath, numCols, numEigenVectors);
    topNEigenVectorsTranpose.setConf(conf);
    
    originalTranspose.transposeTimes(topNEigenVectorsTranpose);
    
  }
}