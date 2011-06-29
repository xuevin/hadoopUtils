package uk.ac.ebi.fgpt.hadoopUtils.pca.sequential;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.HadoopTestCase;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.ReadVectors;

public class TestReadVectors extends HadoopTestCase {
	private String[][] data = new String[][] { { "2", "4", "6" },
			{ "8", "10", "12" } };
	private Path tmpDir;
	private Path inputPath;
	private Configuration conf;
	private TreeMap<IntWritable, VectorWritable> vectorMap;

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
		
		// create some dummy data
		tmpDir = getTestTempDirPath(conf);
		inputPath = new Path(tmpDir, "inputSeqFile");
		createSequenceFileFromData(conf, inputPath, data);

		// Make new vectorMap
		vectorMap = new TreeMap<IntWritable, VectorWritable>();
	}

	@Test
	public void testReader() throws IOException {
		ReadVectors.run(inputPath.toString(), vectorMap, true);
		IntWritable firstKey = vectorMap.firstKey();
		IntWritable lastKey = vectorMap.lastKey();
		assertEquals("First key is not equals to 0", 0, firstKey.get());
		assertEquals("Position 1,1 is not equal to 2.0", 2.0, vectorMap.get(
				firstKey).get().get(0), 0);
		assertEquals("Position 2,3 is not equal to 12.0", 12.0, vectorMap.get(
				lastKey).get().get(2), 0);
	}
}
