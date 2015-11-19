/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.yarn.buildtests;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.context.MiniYarnClusterTest;
import org.springframework.yarn.test.context.YarnCluster;
import org.springframework.yarn.test.junit.ApplicationInfo;
import org.springframework.yarn.test.support.ContainerLogUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@MiniYarnClusterTest
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource("file:target/spring-cloud-dataflow-yarn-build-tests/test.properties")
public class AbstractCliBootYarnClusterTests implements ApplicationContextAware, EnvironmentAware {

	private ApplicationContext applicationContext;
	private Environment environment;
	private Configuration configuration;
	private YarnCluster yarnCluster;
	private YarnClient yarnClient;
	private String projectVersion;
	
	@Before
	public void setup() {
		projectVersion = getEnvironment().getProperty("projectVersion");
	}
	
	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	@Autowired
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
	
	@Autowired
	public void setYarnCluster(YarnCluster yarnCluster) {
		this.yarnCluster = yarnCluster;
	}
	
	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	
	public Environment getEnvironment() {
		return environment;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setYarnClient(YarnClient yarnClient) {
		this.yarnClient = yarnClient;
	}
	
	public YarnClient getYarnClient() {
		return yarnClient;
	}
	
	public YarnCluster getYarnCluster() {
		return yarnCluster;
	}
	
	public String getProjectVersion() {
		return projectVersion;
	}

	protected ApplicationInfo submitApplicationAndWait(Object source, String[] args) throws Exception {
		return submitApplicationAndWait(source, args, 60, TimeUnit.SECONDS);
	}

	protected ApplicationInfo submitApplicationAndWait(Object source, String[] args, long timeout, final TimeUnit unit) throws Exception {
		return submitApplicationAndWaitState(source, args, timeout, unit, YarnApplicationState.FINISHED, YarnApplicationState.FAILED);
	}
	
	protected ApplicationInfo submitApplicationAndWaitState(long timeout, TimeUnit unit, YarnApplicationState... applicationStates) throws Exception {
		Assert.notEmpty(applicationStates, "Need to have atleast one state");
		Assert.notNull(getYarnClient(), "Yarn client must be set");

		YarnApplicationState state = null;
		ApplicationReport report = null;

		ApplicationId applicationId = submitApplication();
		Assert.notNull(applicationId, "Failed to get application id from submit");

		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done:
		do {
			report = findApplicationReport(getYarnClient(), applicationId);
			if (report == null) {
				break;
			}
			state = report.getYarnApplicationState();
			for (YarnApplicationState stateCheck : applicationStates) {
				if (state.equals(stateCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return new ApplicationInfo(applicationId, report);
	}
	
	protected ApplicationInfo waitState(ApplicationId applicationId, long timeout, TimeUnit unit, YarnApplicationState... applicationStates) throws Exception {
		YarnApplicationState state = null;
		ApplicationReport report = null;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done:
		do {
			report = findApplicationReport(getYarnClient(), applicationId);
			if (report == null) {
				break;
			}
			state = report.getYarnApplicationState();
			for (YarnApplicationState stateCheck : applicationStates) {
				if (state.equals(stateCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return new ApplicationInfo(applicationId, report);		
	}
	
	
	protected ApplicationInfo submitApplicationAndWaitState(Object source, String[] args, final long timeout,
			final TimeUnit unit, final YarnApplicationState... applicationStates) throws Exception {

		SpringApplicationBuilder builder = new SpringApplicationBuilder(source);
		builder.initializers(new HadoopConfigurationInjectingInitializer(getConfiguration()));

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<ApplicationInfo>() {

			@Override
			public ApplicationInfo runWithSpringApplication(ApplicationContext context) throws Exception {
				setYarnClient(context.getBean(YarnClient.class));
				return submitApplicationAndWaitState(timeout, unit, applicationStates);
			}

		}, args);
	}

	protected File assertWaitFileContent(long timeout, TimeUnit unit, ApplicationId applicationId, String search) throws Exception {
		File file = null;

		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		done:
		do {

			List<Resource> resources = ContainerLogUtils.queryContainerLogs(
					getYarnCluster(), applicationId);
			for (Resource res : resources) {
				File f = res.getFile();
				String content = ContainerLogUtils.getFileContent(f);
				if (content.contains(search)) {
					file = f;
					break done;
				}
			}
			
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		
		
		assertThat(file, notNullValue());
		return file;
	}

	protected String dumpFs() throws IOException {
		StringBuilder buf = new StringBuilder();
		FileSystem fs = FileSystem.get(getConfiguration());
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while (files.hasNext()) {
			buf.append(files.next().toString());
			buf.append("\n");
		}
		return buf.toString();
	}
	
	protected void waitHdfsFile(String path, long timeout, TimeUnit unit) throws Exception {
		Path p = new Path(path);
		FileSystem fs = FileSystem.get(getConfiguration());
		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		boolean found = false;
		do {
			if (fs.exists(p)) {
				found = true;
				break;
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		assertThat(found, is(true));		
	}
		
	private ApplicationReport findApplicationReport(YarnClient client, ApplicationId applicationId) {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				return report;
			}
		}
		return null;
	}
	
	protected ApplicationId submitApplication() {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		ApplicationId applicationId = getYarnClient().submitApplication();
		Assert.notNull(applicationId, "Failed to get application id from submit");
		return applicationId;
	}
	
	public static class HadoopConfigurationInjectingInitializer
			implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private final Configuration configuration;

		public HadoopConfigurationInjectingInitializer(Configuration configuration) {
			this.configuration = configuration;
		}

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
			applicationContext.getBeanFactory().registerSingleton("miniYarnConfiguration", configuration);
		}

	}
	
}
