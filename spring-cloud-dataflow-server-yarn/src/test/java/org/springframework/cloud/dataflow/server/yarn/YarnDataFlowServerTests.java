/*
 * Copyright 2015-2017 the original author or authors.
 *
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

package org.springframework.cloud.dataflow.server.yarn;

import java.util.Map;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.yarn.YarnAppDeployer;
import org.springframework.cloud.deployer.spi.yarn.YarnTaskLauncher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class YarnDataFlowServerTests {

	@Test
	public void testSimpleBootstrap() throws Exception {
		SpringApplication app = new SpringApplication(YarnDataFlowServer.class);
		ConfigurableApplicationContext context = app.run(new String[] { "--server.port=0",
				"--spring.cloud.dataflow.yarn.version=fake", "--spring.hadoop.fsUri=hdfs://localhost:8020" });
		assertThat(context.containsBean("appDeployer"), is(true));
		assertThat(context.getBean("appDeployer"), instanceOf(YarnAppDeployer.class));
		assertThat(context.containsBean("taskLauncher"), is(true));
		assertThat(context.getBean("taskLauncher"), instanceOf(YarnTaskLauncher.class));
		assertThat(context.containsBean("delegatingResourceLoader"), is(true));
		DelegatingResourceLoader delegatingResourceLoader = context.getBean(DelegatingResourceLoader.class);
		Map<String, ResourceLoader> loaders = TestUtils.readField("loaders", delegatingResourceLoader);
		assertThat(loaders.size(), is(4));
		assertThat(loaders.get("file"), notNullValue());
		assertThat(loaders.get("http"), notNullValue());
		assertThat(loaders.get("maven"), notNullValue());
		assertThat(loaders.get("hdfs"), notNullValue());
		ResourceLoader resourceLoader = loaders.get("hdfs");
		assertThat(resourceLoader, instanceOf(HdfsResourceLoader.class));
		assertThat(((HdfsResourceLoader)resourceLoader).getFileSystem(), instanceOf(DistributedFileSystem.class));
		context.close();
	}

}
