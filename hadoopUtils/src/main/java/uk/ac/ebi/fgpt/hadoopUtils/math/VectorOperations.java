package uk.ac.ebi.fgpt.hadoopUtils.math;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

public class VectorOperations {
	public static Vector normalize(Vector inputVector){
		int columns = inputVector.size();
		
		//STEP 1 : FIND SUM
		double sum = 0;
		for(int i =0;i<columns;i++){
			sum+=inputVector.get(i);
		}

		//STEP 2 : FIND MEAN
		double mean = (sum/(double)columns);
		
		//STEP 3 : CALCULATE VARIANCE
		double variance=0;
		for(int i =0;i<columns;i++){
			variance+=Math.pow((inputVector.get(i)-mean), 2);
		}
		variance = (variance/(double)(columns-1));
		
		//STEP 4 : CALCULATE STANDARD DEVIATION			
		double standardDeviation = Math.sqrt(variance);
		
		//STEP 5 : FILL IN NEW VECTOR
		Vector vector = new SequentialAccessSparseVector(columns);
		for(int j = 0;j<columns;j++){
			vector.set(j, ((inputVector.get(j)-mean)/standardDeviation));
		}
		return vector;
		
	}

}
