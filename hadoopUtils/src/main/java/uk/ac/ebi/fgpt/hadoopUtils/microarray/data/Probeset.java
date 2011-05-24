package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import org.apache.mahout.math.Vector;

public class Probeset {
  private String probesetName;
  private Vector[] arrayOfProbes;
  private int numProbes;
  private int numSamples;
  
  public Probeset() {

  }
  
  public Probeset(String probesetName, Vector[] arrayOfProbes) {
    this.arrayOfProbes = arrayOfProbes;
    this.numProbes = arrayOfProbes.length;
    this.probesetName = probesetName;
    this.numSamples = getNumSamples(arrayOfProbes);
  }
  
  private int getNumSamples(Vector[] arrayOfProbes) {
    int size = 0;
    for (Vector vec : arrayOfProbes) {
      if (size == 0) {
        size = vec.size();
      } else {
        if (vec.size() != size) {
          System.err.println("The number of samples in each probe is inconsistent!");
        }
      }
    }
    return size;
  }
  
  public Vector[] getArrayOfVectors() {
    return arrayOfProbes;
  }
  
  public void setArrayOfProbes(Vector[] arrayOfProbes) {
    this.arrayOfProbes = arrayOfProbes;
    this.numProbes = arrayOfProbes.length;
    this.numSamples = getNumSamples(arrayOfProbes);
  }
  
  public int getNumProbes() {
    return numProbes;
  }
  
  public void setProbesetName(String probesetName) {
    this.probesetName = probesetName;
  }
  
  public String getProbesetName() {
    return probesetName;
  }
  
  public int getNumSamples() {
    return numSamples;
  }
}
