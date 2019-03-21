/*
 * Copyright 2015-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.hdfs.dataset.sink;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.hdfs.dataset.domain.TestPojo;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

/**
 * @author Thomas Risberg
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {DatasetSinkIntegrationTests.HdfsDatasetSinkApplication.class, HdfsDatasetSinkConfiguration.class})
@DirtiesContext
public abstract class DatasetSinkIntegrationTests {

	@Autowired
	ConfigurableApplicationContext applicationContext;

	@Value("${hdfs.dataset.directory}#{T(java.io.File).separator}${hdfs.dataset.namespace}")
	protected String testDir;

	@Autowired
	protected Configuration hadoopConfiguration;

	@Autowired
	protected DatasetOperations datasetOperations;

	@Autowired
	protected Sink sink;

	@BeforeClass
	public static void setupClass() {
		Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
	}

	@Before
	public void setup() throws IOException {
		FileSystem fs = FileSystem.get(hadoopConfiguration);
		FsShell fsShell = new FsShell(hadoopConfiguration, fs);
		if (fsShell.test(testDir)) {
			fsShell.rmr(testDir);
		}
	}

	@TestPropertySource(properties = {"server.port:0",
			"spring.hadoop.fsUri=file:///",
			"hdfs.dataset.directory=${java.io.tmpdir}/dataset",
			"hdfs.dataset.namespace=pojo",
			"hdfs.dataset.allowNullValues=false",
			"hdfs.dataset.format=avro",
			"hdfs.dataset.partitionPath=year('timestamp')",
			"hdfs.dataset.batchSize=2",
			"hdfs.dataset.idleTimeout=2000"})
	public static class PartitionedDatasetTests extends DatasetSinkIntegrationTests {

		Long systemTime = 1446570266690L; // Date is 2015/11/03

		@Test
		public void testWritingSomething() throws IOException {
			TestPojo t1 = new TestPojo();
			t1.setId(1);
			t1.setTimestamp(systemTime + 10000);
			t1.setDescription("foo");
			sink.input().send(MessageBuilder.withPayload(t1).build());
			TestPojo t2 = new TestPojo();
			t2.setId(2);
			t2.setTimestamp(systemTime + 50000);
			t2.setDescription("x");
			sink.input().send(MessageBuilder.withPayload(t2).build());

			File testOutput = new File(testDir);
			assertTrue("Dataset path created", testOutput.exists());
			assertTrue("Dataset storage created",
					new File(testDir + File.separator + datasetOperations.getDatasetName(TestPojo.class)).exists());
			assertTrue("Dataset metadata created",
					new File(testDir + File.separator + datasetOperations.getDatasetName(TestPojo.class) +
							File.separator + ".metadata").exists());
			String year = new SimpleDateFormat("yyyy").format(systemTime);
			assertTrue("Dataset partition path created",
					new File(testDir + File.separator +
							datasetOperations.getDatasetName(TestPojo.class) + "/year=" + year).exists());
			File testDatasetFiles =
					new File(testDir + File.separator +
							datasetOperations.getDatasetName(TestPojo.class) + "/year=" + year);
			File[] files = testDatasetFiles.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if (name.endsWith(".avro")) {
						return true;
					}
					return false;
				}
			});
			assertTrue("Dataset data files created", files.length > 0);
		}
	}

	@TestPropertySource(properties = {"server.port:0",
			"spring.hadoop.fsUri=file:///",
			"hdfs.dataset.directory=${java.io.tmpdir}/dataset",
			"hdfs.dataset.namespace=parquet",
			"hdfs.dataset.allowNullValues=false",
			"hdfs.dataset.format=parquet",
			"hdfs.dataset.batchSize=2",
			"hdfs.dataset.idleTimeout=2000"})
	public static class ParquetDatasetTests extends DatasetSinkIntegrationTests {

		@Test
		public void testWritingSomething() throws IOException {
			TestPojo t1 = new TestPojo();
			t1.setId(1);
			t1.setTimestamp(System.currentTimeMillis());
			t1.setDescription("foo");
			sink.input().send(MessageBuilder.withPayload(t1).build());
			TestPojo t2 = new TestPojo();
			t2.setId(2);
			t2.setTimestamp(System.currentTimeMillis());
			t2.setDescription("x");
			sink.input().send(MessageBuilder.withPayload(t2).build());

			File testOutput = new File(testDir);
			assertTrue("Dataset path created", testOutput.exists());
			assertTrue("Dataset storage created",
					new File(testDir + File.separator + datasetOperations.getDatasetName(TestPojo.class)).exists());
			assertTrue("Dataset metadata created",
					new File(testDir + File.separator + datasetOperations.getDatasetName(TestPojo.class) +
							File.separator + ".metadata").exists());
			File testDatasetFiles =
					new File(testDir + File.separator + datasetOperations.getDatasetName(TestPojo.class));
			File[] files = testDatasetFiles.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if (name.endsWith(".parquet")) {
						return true;
					}
					return false;
				}
			});
			assertTrue("Dataset data files created", files.length > 0);
		}
	}

	@SpringBootApplication
	static class HdfsDatasetSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(HdfsDatasetSinkApplication.class, args);
		}
	}
}