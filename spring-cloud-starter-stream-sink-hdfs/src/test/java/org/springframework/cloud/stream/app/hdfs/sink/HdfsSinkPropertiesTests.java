/*
 * Copyright 2015-2017 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.hdfs.sink;

import org.junit.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.hdfs.hadoop.store.codec.Codecs;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Thomas Risberg
 */
public class HdfsSinkPropertiesTests {

	@Test
	public void fsUriCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.fsUri=hdfs://localhost:8020").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getFsUri(), equalTo("hdfs://localhost:8020"));
	}

	@Test
	public void directoryCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.directory=/tmp/test").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getDirectory(), equalTo("/tmp/test"));
	}

	@Test
	public void fileNameCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.fileName=mydata").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getFileName(), equalTo("mydata"));
	}

	@Test
	public void fileExtensionCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.fileExtension=test").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getFileExtension(), equalTo("test"));
	}

	@Test
	public void codecCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.codec=snappy").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getCodec(), equalTo(Codecs.SNAPPY.getAbbreviation()));
	}

	@Test
	public void fileUuidCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.fileUuid=true").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.isFileUuid(), equalTo(true));
	}

	@Test
	public void overwriteCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.overwrite=true").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.isOverwrite(), equalTo(true));
	}

	@Test
	public void rolloverCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.rollover=5555555").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getRollover(), equalTo(5555555));
	}

	@Test
	public void idleTimeoutCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.idleTimeout=12345").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getIdleTimeout(), equalTo(12345L));
	}

	@Test
	public void closeTimeoutCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.closeTimeout=12345").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getCloseTimeout(), equalTo(12345L));
	}

	@Test
	public void enableSyncCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.enableSync=true").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.isEnableSync(), equalTo(true));
	}

	@Test
	public void flushTimeoutCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.flushTimeout=12345").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getFlushTimeout(), equalTo(12345L));
	}

	@Test
	public void inUsePrefixCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.inUsePrefix=_").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getInUsePrefix(), equalTo("_"));
	}

	@Test
	public void inUseSuffixCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.inUseSuffix=tmp").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getInUseSuffix(), equalTo("tmp"));
	}

	@Test
	public void fileOpenAttemptsCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.fileOpenAttempts=5").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getFileOpenAttempts(), equalTo(5));
	}

	@Test
	public void partitionPathCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("hdfs.partitionPath=dateFormat('yyyy/MM/dd')").applyTo(context);
		context.register(Conf.class);
		context.refresh();
		HdfsSinkProperties properties = context.getBean(HdfsSinkProperties.class);
		assertThat(properties.getPartitionPath(), equalTo("dateFormat('yyyy/MM/dd')"));
	}

	@Configuration
	@EnableConfigurationProperties(HdfsSinkProperties.class)
	static class Conf {
	}

}