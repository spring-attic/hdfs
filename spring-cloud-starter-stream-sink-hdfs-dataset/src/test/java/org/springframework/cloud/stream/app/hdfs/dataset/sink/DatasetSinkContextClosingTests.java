/*
 * Copyright 2015-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.hdfs.dataset.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Thomas Risberg
 */
public class DatasetSinkContextClosingTests {

	AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

	protected String testDir;

	protected String nameSpace;

	protected String datasetName;

	protected Configuration hadoopConfiguration;

	protected Sink sink;

	@BeforeClass
	public static void setupClass() {
		Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
	}

	@Before
	public void setup() throws IOException {
		this.testDir=System.getProperty("java.io.tmpdir") + "/dataset";
		this.nameSpace = "test";
		System.out.println("**** TESTDIR: " + testDir + "/" + nameSpace);
		String[] env = {"server.port:0",
				"spring.hadoop.fsUri=file:///",
				"hdfs.dataset.directory=" + this.testDir,
				"hdfs.dataset.namespace=" + this.nameSpace,
				"hdfs.dataset.batchSize=2",
				"hdfs.dataset.idleTimeout=2000"};
		EnvironmentTestUtils.addEnvironment(this.context, env);
		this.context.register(HdfsDatasetSinkApplication.class);
		this.context.refresh();
		this.datasetName = context.getBean(DatasetOperations.class).getDatasetName(String.class);
		this.hadoopConfiguration = context.getBean(Configuration.class);
		this.sink = context.getBean(Sink.class);
		FileSystem fs = FileSystem.get(hadoopConfiguration);
		FsShell fsShell = new FsShell(hadoopConfiguration, fs);
		if (fsShell.test(testDir)) {
			fsShell.rmr(testDir);
		}
	}

	@Test
	public void testWritingSomething() throws IOException, InterruptedException {
		sink.input().send(new GenericMessage<>("Foo"));
		sink.input().send(new GenericMessage<>("Bar"));
		sink.input().send(new GenericMessage<>("Baz"));

		this.context.close();

		File testOutput = new File(testDir);
		assertTrue("Dataset path created", testOutput.exists());
		assertTrue("Dataset storage created",
				new File(testDir + File.separator + this.nameSpace + File.separator + datasetName).exists());
		assertTrue("Dataset metadata created",
				new File(testDir + File.separator + this.nameSpace + File.separator + datasetName +
						File.separator + ".metadata").exists());
		File testDatasetFiles =
				new File(testDir + File.separator + this.nameSpace + File.separator + datasetName);
		File[] files = testDatasetFiles.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".avro")) {
					return true;
				}
				return false;
			}
		});
		assertTrue("Dataset data files created", files.length > 1);
	}

	@SpringBootApplication
	static class HdfsDatasetSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(HdfsDatasetSinkApplication.class, args);
		}
	}
}