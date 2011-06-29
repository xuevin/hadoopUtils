package uk.ac.ebi.fgpt.hadoopUtils.microarray.singlejob;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.mahout.math.Matrix;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.DesignMatrixFactory;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.IrlsOutput;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.microarray.math.IterativelyReweightedLeastSquares;

public class OneoffTest {
  public static void main() throws URISyntaxException, IOException {
    long time = System.currentTimeMillis();
    OneOff one = new OneOff();
    Probeset probeset = one.loadHUGEProbeset();
    
    DesignMatrixFactory factory = new DesignMatrixFactory(probeset.getNumProbes(), probeset.getNumSamples());
    Matrix designMatrix = factory.getDesignMatrix();
    System.out.println(designMatrix.size()[0] + " " + designMatrix.size()[1]);
    // If you want your computer to die..
    IrlsOutput output = IterativelyReweightedLeastSquares.run(probeset, 0.0001, 20);
    System.out.println(System.currentTimeMillis() - time);
    
  }
}
