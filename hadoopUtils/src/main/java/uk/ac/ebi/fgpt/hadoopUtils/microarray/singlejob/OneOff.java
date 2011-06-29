package uk.ac.ebi.fgpt.hadoopUtils.microarray.singlejob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.mahout.math.Vector;

import uk.ac.ebi.fgpt.hadoopUtils.microarray.data.Probeset;
import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

public class OneOff{
  public OneOff(){
    
  }
  public Probeset loadHUGEProbeset() throws URISyntaxException, IOException {
    
    
    BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResource("probeset1.log2.matrix.txt").openStream()));
    String line;
    String header = br.readLine(); // Skips one line
    
    Vector[] arrayOfVectors = new Vector[16];
    int i = 0;
    while ((line = br.readLine()) != null) {
      arrayOfVectors[i] = StringToVector.convert(line, 1);
      i++;
    }
    System.out.println(i);
    return new Probeset("mock", arrayOfVectors);
  }
}