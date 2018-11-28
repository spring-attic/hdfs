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

package org.springframework.cloud.stream.app.hdfs.sink;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.hdfs.hadoop.fs.FsShell;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.messaging.support.GenericMessage;

import static org.junit.Assert.assertTrue;

/**
 * @author Thomas Risberg
 */
public class HdfsSinkConfigurationTests {

	AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

	private String testDir;

	private FsShell fsShell;

	private Sink sink;

	@BeforeClass
	public static void setupClass() {
		Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
	}

	@Before
	public void setup() {
		this.testDir=System.getProperty("java.io.tmpdir") + "/hdfs-sink";
		String[] env = {"server.port=0",
				"spring.hadoop.fsUri=file:///",
				"hdfs.directory=" + this.testDir,
				"hdfs.closeTimeout=100"};
		TestPropertyValues.of(env).applyTo(context);
		this.context.register(HdfsSinkApplication.class);
		this.context.refresh();
		this.fsShell = context.getBean(FsShell.class);
		this.sink = context.getBean(Sink.class);
		if (fsShell.test(testDir)) {
			fsShell.rmr(testDir);
		}
	}

	@Test
	public void testWritingSomething() throws IOException, InterruptedException {
		sink.input().send(new GenericMessage<>("Foo"));
		Thread.sleep(150);
		sink.input().send(new GenericMessage<>("Bar"));
		sink.input().send(new GenericMessage<>("Baz"));
	}

	@After
	public void checkFilesClosedOK() throws IOException {
		context.close();
		File testOutput = new File(testDir);
		assertTrue(testOutput.exists());
		File[] files = testOutput.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".txt");
			}

		});
		assertTrue(files.length > 1);
	}

	@SpringBootApplication
	static class HdfsSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(HdfsSinkApplication.class, args);
		}
	}

}