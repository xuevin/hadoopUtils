package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class IrlsOutputWritable implements Writable {
  private IrlsOutput irlsOutput;
  
  public IrlsOutputWritable() {}
  
  public IrlsOutputWritable(IrlsOutput irlsOutput) {
    this.irlsOutput = irlsOutput;
  }
  
  public IrlsOutput get() {
    return this.irlsOutput;
  }
  
  public void set(IrlsOutput irlsOutput) {
    this.irlsOutput = irlsOutput;
  }
  
  public void readFields(DataInput in) throws IOException {
    this.irlsOutput = readIrlsOutput(in);
  }
  
  public void write(DataOutput out) throws IOException {
    writeIrlsOutput(out, irlsOutput);
  }
  
  public static IrlsOutput readIrlsOutput(DataInput in) throws IOException {
    String probesetName = in.readUTF();
    int numProbes = in.readInt();
    
    Vector[] arrayOfWeightVectors = new Vector[numProbes];
    
    for (int i = 0; i < numProbes; i++) {
      arrayOfWeightVectors[i] = VectorWritable.readVector(in);
    }
    
    Vector vectorOfEstimates = VectorWritable.readVector(in);
    
    IrlsOutput irlsOutput = new IrlsOutput(probesetName, arrayOfWeightVectors, vectorOfEstimates);
    return irlsOutput;
  }
  
  public static void writeIrlsOutput(DataOutput out, IrlsOutput irlsOutput) throws IOException {
    out.writeUTF(irlsOutput.getProbesetName());
    out.writeInt(irlsOutput.getNumProbes());
    for (int i = 0; i < irlsOutput.getNumProbes(); i++) {
      VectorWritable.writeVector(out, irlsOutput.getArrayOfWeightVectors()[i]);
    }
    VectorWritable.writeVector(out, irlsOutput.getVectorOfEstimates());
  }
  
}
