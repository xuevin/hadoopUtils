package uk.ac.ebi.fgpt.hadoopUtils.deprecated;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import uk.ac.ebi.fgpt.hadoopUtils.pca.math.StringToVector;

public class CreateNormalizedVectorFromTxt {
	public static void main(String[] args) throws IOException{
		String pathToInput = args[0];
		String pathToOutput = args[1];
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		Path inputPath = new Path(pathToInput);
		Path outputPath = new Path(pathToOutput);
		
		if (!fs.exists(inputPath))
		  printAndExit("Input file not found");
		if (!fs.isFile(inputPath))
		  printAndExit("Input should be a file");
		if (fs.exists(outputPath))
		  printAndExit("Output already exists");
		
		// Open a file for reading
		FSDataInputStream in = fs.open(inputPath);
		
		try {
			//Create Writer
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, config,
					outputPath, IntWritable.class, VectorWritable.class);
			
			//Create Buffered Reader (which reads from input stream)
			BufferedReader reader = new BufferedReader(new InputStreamReader(in,"UTF-8"));

	
			int index =0;
			
			String line;
			while ((line = reader.readLine())!=null) {
				writer.append(new IntWritable(index), new VectorWritable(StringToVector.normalize(line)));
				index++;
			}
			writer.close();
		} finally {
			in.close();
		}
	}

	private static void printAndExit(String string) {
		System.out.println(string);
		System.exit(0);
	}
}
