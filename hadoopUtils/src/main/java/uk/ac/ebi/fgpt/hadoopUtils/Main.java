package uk.ac.ebi.fgpt.hadoopUtils;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedSortProbesJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.IterativelyReweightedLeastSquaresJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.CreateDesignMatrix;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.CreateDesignMatrixFromProbesetWritable;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.ReadIrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.ReadProbesets;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.SequentialIRLSJobLauncher;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.CreateNormalizedVectorsFromVectors_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.Transpose_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.pca.distributed.Transpose_newAPI;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.CreateNormalizedVectorFromVector;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.CreateVectorFromTxt;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.PcaJob;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.ReadVectors;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.ReindexSequenceFile;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.Tail;
import uk.ac.ebi.fgpt.hadoopUtils.supervisedPCA.SupervisedPCA;

public class Main {
  public static void main(String args[]) throws Exception {
    
    if (args.length == 0) {
      printOptions();
      return;
    }
    String[] newArgs = new String[args.length - 1];
    for (int i = 1; i < args.length; i++) {
      newArgs[i - 1] = args[i];
    }
    
    if (args[0].equals("createVectorFromTxt")) {
      CreateVectorFromTxt.main(newArgs);
    } else if (args[0].equals("readVector")) {
      ReadVectors.main(newArgs);
    } else if (args[0].equals("normalizeVector")) {
      CreateNormalizedVectorFromVector.main(newArgs);
    } else if (args[0].equals("normalizeVectorWithMapRed")) {
      CreateNormalizedVectorsFromVectors_MapRed.main(newArgs);
    } else if (args[0].equals("transpose")) {
      Transpose_newAPI.main(newArgs);
    } else if (args[0].equals("reindex")) {
      ReindexSequenceFile.main(newArgs);
    } else if (args[0].equals("tail")) {
      Tail.main(newArgs);
    } else if (args[0].equals("pcaJob")) {
      PcaJob.main(newArgs);
    } else if (args[0].equals("irlsJob")) {
      IterativelyReweightedLeastSquaresJob.main(newArgs);
    } else if (args[0].equals("sortProbesJob")) {
      DistributedSortProbesJob.main(newArgs);
    } else if (args[0].equals("distIrlsJob")) {
//      SequentialIRLSJobLauncher.main(newArgs);
      DistributedIrlsJob.main(newArgs);
    } else if (args[0].equals("readProbeset")) {
      ReadProbesets.main(newArgs);
    } else if (args[0].equals("readIrlsOutput")) {
      ReadIrlsOutput.main(newArgs);
    } else if (args[0].equals("createDesign")) {
      CreateDesignMatrix.main(newArgs);
    } else if (args[0].equals("createDesignFromProbesetWritable")) {
      CreateDesignMatrixFromProbesetWritable.main(newArgs);
    } else {
      printOptions();
    }
  }
  
  public static void printOptions() {
    System.out.println("Possible Apps Are:\n" + "\tnormalizeTxt\n" + "\tcreateVectorFromTxt\n"
                       + "\tnormalizeVector\n" + "\tnormalizeVectorWithMapRed\n" + "\treindex\n"
                       + "\tpcaJob\n" + "\ttranspose\n" + "\ttail\n" + "\tirlsJob\n" + "\tsortProbesJob\n"
                       + "\tdistIrlsJob\n" + "\treadProbeset\n" + "\treadVector\n" + "\treadIrlsOutput\n"
                       + "\tcreateDesign\n" + "\tcreateDesignFromProbesetWritable\n");
  }
}
