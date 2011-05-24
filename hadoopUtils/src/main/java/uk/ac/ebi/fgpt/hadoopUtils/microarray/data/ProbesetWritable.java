package uk.ac.ebi.fgpt.hadoopUtils.microarray.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ProbesetWritable implements Writable {
  
  private Probeset probeset;
  
  public ProbesetWritable() {}
  
  public ProbesetWritable(Probeset probeset) {
    this.probeset = probeset;
  }
  
  public Probeset get() {
    return this.probeset;
  }
    
  public void set(Probeset probeset) {
    this.probeset = probeset;
  }
  
  public void readFields(DataInput in) throws IOException {
    this.probeset = readProbeset(in);
    
  }
  
  public void write(DataOutput out) throws IOException {
    writeProbeset(out, probeset);
  }
  
  public static Probeset readProbeset(DataInput in) throws IOException {
    String probesetName = in.readUTF();
    
    int numProbes = in.readInt();
    
    Vector[] arrayOfProbes = new Vector[numProbes];
    
    for (int i = 0; i < numProbes; i++) {
      arrayOfProbes[i] = VectorWritable.readVector(in);
    }
    
    Probeset probeset = new Probeset(probesetName, arrayOfProbes);
    return probeset;
  }
  
  public static void writeProbeset(DataOutput out, Probeset probeset) throws IOException {
    out.writeUTF(probeset.getProbesetName());
    out.writeInt(probeset.getNumProbes());
    for (int i = 0; i < probeset.getNumProbes(); i++) {
      VectorWritable.writeVector(out, probeset.getArrayOfVectors()[i]);
    }
  }
}
