package uk.ac.ebi.fgpt.hadoopUtils;

import uk.ac.ebi.fgpt.hadoopUtils.mapreduce.CreateNormalizedVectorsFromVectors_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.mapreduce.Transpose_MapRed;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedIrlsJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.distributed.DistributedSortProbesJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.IterativelyReweightedLeastSquaresJob;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.ReadIrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.sequential.ReadProbesets;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.CreateNormalizedVectorFromVector;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.CreateVectorFromTxt;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.ReadVectors;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.ReindexSequenceFile;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.Tail;
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
      Transpose_MapRed.main(newArgs);
    } else if (args[0].equals("reindex")) {
      ReindexSequenceFile.main(newArgs);
    } else if (args[0].equals("tail")) {
      Tail.main(newArgs);
    } else if (args[0].equals("supervisedPCA")) {
      SupervisedPCA.main(newArgs);
    } else if (args[0].equals("irlsJob")) {
      IterativelyReweightedLeastSquaresJob.main(newArgs);
    } else if (args[0].equals("sortProbesJob")) {
      DistributedSortProbesJob.main(newArgs);
    } else if (args[0].equals("distIrlsJob")) {
      // SequentialIRLSJobLauncher.main(newArgs);
      DistributedIrlsJob.main(newArgs);
    } else if (args[0].equals("readProbeset")) {
      ReadProbesets.main(newArgs);
    } else if (args[0].equals("readIrlsOutput")) {
      ReadIrlsOutput.main(newArgs);
    } else {
      printOptions();
    }
  }
  
  public static void printOptions() {
    System.out.println("Possible Apps Are:\n" + "\tnormalizeTxt\n" + "\tcreateVectorFromTxt\n"
                       + "\tnormalizeVector\n" + "\tnormalizeVectorWithMapRed\n" + "\treindex\n"
                       + "\tsupervisedPCA\n" + "\ttranspose\n" + "\ttail\n" + "\tirlsJob\n"
                       + "\tsortProbesJob\n" + "\tdistIrlsJob\n" + "\treadProbeset\n" + "\treadVector\n"
                       + "\treadIrlsOutput\n");
  }
}
