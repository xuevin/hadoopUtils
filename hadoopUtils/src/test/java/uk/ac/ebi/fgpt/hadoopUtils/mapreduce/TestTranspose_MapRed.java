package uk.ac.ebi.fgpt.hadoopUtils.mapreduce;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.sequential.HadoopTestCase;
import uk.ac.ebi.fgpt.hadoopUtils.sequential.ReadVectors;

public class TestTranspose_MapRed extends HadoopTestCase {
	private String[][] data = new String[][] { { "2", "4", "6" },
			{ "8", "10", "12" } };
	private Path tmpDir;
	private Path inputPath;
	private Configuration conf;
	private Path outputPath;
	private TreeMap<IntWritable, VectorWritable> vectorMap;

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();

		// create dummy data
		tmpDir = getTestTempDirPath(conf);
		inputPath = new Path(tmpDir, "inputSeqFile");
		outputPath = new Path(tmpDir, "outputSeqFile");
//		FileSystem.get(conf).mkdirs(outputPath);

		createSequenceFileFromData(conf, inputPath, data);

		// Make new vectorMap
		vectorMap = new TreeMap<IntWritable, VectorWritable>();
	}

	@Test
	public void testMain() throws Exception {
		Transpose_MapRed.runMain(conf, new String[] { "-i", inputPath.toString(),
				"-o", outputPath.toString(), "-s", "3", "-nc", "3","-nr","2" });
		for(int i =0;i<3;i++){
			FileStatus[] status = FileSystem.get(conf).listStatus(new Path(outputPath.toString()+i));
			for(FileStatus a: status){
				ReadVectors.run(a.getPath().toString());
			}
		}
		
		ReadVectors.run(outputPath.toString());

	}

}
