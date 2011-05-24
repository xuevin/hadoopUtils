package uk.ac.ebi.fgpt.hadoopUtils.supervisedPCA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.decomposer.lanczos.LanczosSolver;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.decomposer.DistributedLanczosSolver;

/**
 * This application creates a SequenceFile with <IntWritable,VectorWritable> keyvalue pairs.
 * arg[0] is the input file
 * arg[1] is the output file
 * 
 * @author vincent@ebi.ac.uk
 *
 */
public class SupervisedPCA extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new SupervisedPCA(), args);
		System.exit(res);
	}
	public static class DistributeAndFilter extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		public DistributeAndFilter(){
			
		}

		protected void map(IntWritable key, VectorWritable value,
				Mapper<IntWritable, VectorWritable,IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}

	}

	public static class ReduceAndCreateMatrix extends
			Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		public void reduce(IntWritable key, Iterator<VectorWritable> value,
				Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, value.next());
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SupervisedPCA.class);
		
		Path outputPath = new Path(args[1]);
		Path inputPath =  new Path(args[0]);
		Path tempPath =  new Path(args[2]);
		int numCols = Integer.parseInt(args[3]);
		int rank = Integer.parseInt(args[4]);
		
		System.out.println(args[0]);
		
		//Set Input and Output Paths
		FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
       
		
		//Establish the Output of the Job
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		//Set Mappers and Reducers
		job.setMapperClass(DistributeAndFilter.class);
		job.setReducerClass(ReduceAndCreateMatrix.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.waitForCompletion(true);
		
		int rows = job.getCounters().getGroup("").size();
		
		Path pathToVector = new Path(outputPath, "part-r-00000");
		
		//Now new data set has been created.
		DistributedRowMatrix matrix =
	        new DistributedRowMatrix(pathToVector, tempPath, rows, numCols);
		matrix.setConf(new Configuration(job.getConfiguration()));
		
	   
	    //Get Eigen Vectors and Values
	    Matrix eigenVectors = new DenseMatrix(rank, numCols);
	    List<Double> eigenValues = new ArrayList<Double>();
	    //tODO fix this
//	    new DistributedLanczosSolver().solve(matrix, rank, eigenVectors, eigenValues, false);
		
	    for(Double value:eigenValues){
	    	System.out.println("Value:" + value);
	    }
		
		
		return 0;
	}

}
