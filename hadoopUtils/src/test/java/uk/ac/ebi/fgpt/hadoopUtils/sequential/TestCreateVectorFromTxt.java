package uk.ac.ebi.fgpt.hadoopUtils.sequential;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.CreateVectorFromTxt;
import uk.ac.ebi.fgpt.hadoopUtils.pca.sequential.ReadVectors;

public class TestCreateVectorFromTxt extends HadoopTestCase {
	private String[][] data = new String[][] { { "2", "4", "6" },
			{ "8", "10", "12" } };
	private Path tmpDir;
	private Path inputPath;
	private Configuration conf;
	private FileSystem fs;
	private Path outputPath;
	private TreeMap<IntWritable, VectorWritable> vectorMap;

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
		fs = FileSystem.get(conf);

		// create
		tmpDir = getTestTempDirPath(conf);
		inputPath = new Path(tmpDir, "inputFile");
		outputPath = new Path(tmpDir, "outputFile");
		createTsvFilesFromArrays(conf, inputPath, data);

		// Make new vectorMap
		vectorMap = new TreeMap<IntWritable, VectorWritable>();
	}

	@Test
	public void testCreateVector_NoSkip() throws IOException {
		CreateVectorFromTxt.main((new String[] { "-i", inputPath.toString(),
				"-o", outputPath.toString() }));
		System.out.println("A 2x3 Matrix is Below");
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertTrue(fs.exists(outputPath));
		assertEquals("Rows not equal to 2", 2, vectorMap.size());
	}

	@Test
	public void testCreateVector_SkipOneRow() throws IOException {
		CreateVectorFromTxt.main((new String[] { "-i", inputPath.toString(),
				"-o", outputPath.toString(), "-hr", "1" }));
		System.out.println("A 1x3 Matrix is Below");
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("Rows not equal to 1", 1, vectorMap.size());
		assertEquals("Column Cardinality not equal to 3", 3, vectorMap
				.firstEntry().getValue().get().size());
	}

	@Test
	public void testCreateVector_SkipOneColumn() throws IOException {
		CreateVectorFromTxt.main((new String[] { "-i", inputPath.toString(),
				"-o", outputPath.toString(), "-hc", "1" }));
		System.out.println("A 2x2 Matrix is Below");
		ReadVectors.run(outputPath.toString(), vectorMap, true);
		assertEquals("Rows not equal to 2", 2, vectorMap.size());
		assertEquals("Column Cardinality not equal to 2", 2, vectorMap
				.firstEntry().getValue().get().size());
	}
}
