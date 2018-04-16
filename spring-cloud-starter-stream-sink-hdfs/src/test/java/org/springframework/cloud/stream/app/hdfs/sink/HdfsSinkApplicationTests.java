package org.springframework.cloud.stream.app.hdfs.sink;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsSinkApplicationTests {

	@Test
	public void contextLoads() {
	}

	@SpringBootApplication
	@Import(HdfsSinkConfiguration.class)
	public static class Config {
	}
}
