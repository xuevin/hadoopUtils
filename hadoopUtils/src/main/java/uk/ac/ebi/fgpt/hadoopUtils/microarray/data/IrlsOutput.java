package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import org.apache.mahout.math.Vector;

public class IrlsOutput {
  private Vector[] arrayOfWeightVectors;
  private Vector vectorOfEstimates;
  private String probesetName;
  
  public IrlsOutput() {
  }
  
  public IrlsOutput(String probesetName, Vector[] arrayOfWeightVectors, Vector vectorOfEstimates) {
    this.arrayOfWeightVectors=arrayOfWeightVectors;
    this.vectorOfEstimates=vectorOfEstimates;
    this.probesetName=probesetName;
  }
  
  public void setArrayOfWeightVectors(Vector[] arrayOfWeightVectors) {
    this.arrayOfWeightVectors = arrayOfWeightVectors;
  }
  
  public Vector[] getArrayOfWeightVectors() {
    return arrayOfWeightVectors;
  }
  
  public void setVectorOfEstimates(Vector vectorOfEstimates) {
    this.vectorOfEstimates = vectorOfEstimates;
  }
  
  public Vector getVectorOfEstimates() {
    return vectorOfEstimates;
  }

  public void setProbesetName(String probesetName) {
    this.probesetName = probesetName;
  }

  public String getProbesetName() {
    return probesetName;
  }
  public int getNumProbes(){
    return arrayOfWeightVectors.length;
  }
  public int getNumSamples(){
    //TODO Not the best way .. but will do for now
    return arrayOfWeightVectors[0].size();
  }
  
  public void print(){
    for(int i = 0; i< arrayOfWeightVectors.length;i++){
      for(int j = 0;j<arrayOfWeightVectors[i].size();j++){
        System.out.print(arrayOfWeightVectors[i].get(j) + "\t");
      }
      System.out.println();
    }
    
    System.out.println();
    for(int i = 0;i<vectorOfEstimates.size();i++){
      System.out.print(vectorOfEstimates.get(i)+ "\t");
    }
  }
  
}
