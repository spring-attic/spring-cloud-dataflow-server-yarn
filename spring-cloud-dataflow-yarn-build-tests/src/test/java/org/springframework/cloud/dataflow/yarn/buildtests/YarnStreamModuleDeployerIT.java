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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.dataflow.admin.spi.yarn.YarnAdminConfiguration;
import org.springframework.cloud.dataflow.core.ArtifactCoordinates;
import org.springframework.cloud.dataflow.core.ModuleDefinition;
import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnStreamModuleDeployer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;
import org.springframework.yarn.test.support.ContainerLogUtils;

/**
 * Integration tests for {@link YarnStreamModuleDeployer}.
 *
 * Tests can be run in sts if project is build first. Build
 * prepares needed yarn files in expected paths.
 * $ mvn clean package -DskipTests
 *
 * @author Janne Valkealahti
 *
 */
@Ignore("Keeps failing, problem with deployer?")
public class YarnStreamModuleDeployerIT extends AbstractCliBootYarnClusterTests {

	private static final String GROUP_ID = "org.springframework.cloud.stream.module";
	private static final String VERSION = "1.0.0.BUILD-SNAPSHOT";
	private AnnotationConfigApplicationContext context;

	@Before
	public void setup() {
		context = new AnnotationConfigApplicationContext();
		context.getEnvironment().setActiveProfiles("yarn");
		context.register(TestYarnConfiguration.class);
		context.setParent(getApplicationContext());
		context.refresh();
	}

	@After
	public void clean() {
		if (context != null) {
			context.close();
		}
		context = null;
	}

	@Test
	public void testStreamTimeLog() throws Exception {
		assertThat(context.containsBean("processModuleDeployer"), is(true));
		assertThat(context.getBean("processModuleDeployer"), instanceOf(YarnStreamModuleDeployer.class));
		ModuleDeployer deployer = context.getBean("processModuleDeployer", ModuleDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);

		ModuleDefinition timeDefinition = new ModuleDefinition.Builder()
				.setGroup("ticktock")
				.setName("time")
				.setParameter("spring.cloud.stream.bindings.output", "ticktock.0")
				.build();
		ModuleDefinition logDefinition = new ModuleDefinition.Builder()
				.setGroup("ticktock")
				.setName("log")
				.setParameter("spring.cloud.stream.bindings.input", "ticktock.0")
				.setParameter("expression", "new String(payload + ' hello')")
				.build();
		ArtifactCoordinates timeCoordinates = new ArtifactCoordinates.Builder()
				.setGroupId(GROUP_ID)
				.setArtifactId("time-source")
				.setVersion(VERSION)
				.setClassifier("exec")
				.build();
		ArtifactCoordinates logCoordinates = new ArtifactCoordinates.Builder()
				.setGroupId(GROUP_ID)
				.setArtifactId("log-sink")
				.setVersion(VERSION)
				.setClassifier("exec")
				.build();
		ModuleDeploymentRequest time = new ModuleDeploymentRequest(timeDefinition, timeCoordinates);
		ModuleDeploymentRequest log = new ModuleDeploymentRequest(logDefinition, logCoordinates);

		ModuleDeploymentId timeId = deployer.deploy(time);
		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");

		ModuleDeploymentId logId = deployer.deploy(log);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started LogSinkApplication");

		deployer.undeploy(timeId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.ticktock.0");

		deployer.undeploy(logId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.ticktock.0");

		Collection<CloudAppInstanceInfo> instances = yarnCloudAppService.getInstances(CloudAppType.STREAM);
		assertThat(instances.size(), is(1));

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));

		for (Resource res : resources) {
			File file = res.getFile();
			String content = ContainerLogUtils.getFileContent(file);
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("stderr")) {
				assertThat("stderr with content: " + content, file.length(), is(0l));
			}
		}
	}

	@Test
	public void testStreamTimeHdfs() throws Exception {
		ModuleDeployer deployer = context.getBean("processModuleDeployer", ModuleDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);
		String fsUri = getConfiguration().get("fs.defaultFS");

		ModuleDefinition timeDefinition = new ModuleDefinition.Builder()
				.setGroup("timehdfs")
				.setName("time")
				.setParameter("spring.cloud.stream.bindings.output", "timehdfs.0")
				.build();
		ModuleDefinition hdfsDefinition = new ModuleDefinition.Builder()
				.setGroup("timehdfs")
				.setName("hdfs")
				.setParameter("spring.cloud.stream.bindings.input", "timehdfs.0")
				.setParameter("spring.hadoop.fsUri", fsUri)
				.build();
		ArtifactCoordinates timeCoordinates = new ArtifactCoordinates.Builder()
				.setGroupId(GROUP_ID)
				.setArtifactId("time-source")
				.setVersion(VERSION)
				.setClassifier("exec")
				.build();
		ArtifactCoordinates hdfsCoordinates = new ArtifactCoordinates.Builder()
				.setGroupId(GROUP_ID)
				.setArtifactId("hdfs-sink")
				.setVersion(VERSION)
				.setClassifier("exec")
				.build();
		ModuleDeploymentRequest time = new ModuleDeploymentRequest(timeDefinition, timeCoordinates);
		ModuleDeploymentRequest hdfs = new ModuleDeploymentRequest(hdfsDefinition, hdfsCoordinates);

		ModuleDeploymentId timeId = deployer.deploy(time);
		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");

		ModuleDeploymentId hdfsId = deployer.deploy(hdfs);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started HdfsSinkApplication");

		waitHdfsFile("/tmp/hdfs-sink/data-0.txt", 2, TimeUnit.MINUTES);

		deployer.undeploy(timeId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.timehdfs.0");

		deployer.undeploy(hdfsId);
		// TODO: why "inbound.timehdfs.0" is not logged?
		//assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.timehdfs.0");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped hdfsSink.hdfsSink.serviceActivator");

		Collection<CloudAppInstanceInfo> instances = yarnCloudAppService.getInstances(CloudAppType.STREAM);
		assertThat(instances.size(), is(1));

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));

		for (Resource res : resources) {
			File file = res.getFile();
			String content = ContainerLogUtils.getFileContent(file);
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("stderr")) {
				assertThat("stderr with content: " + content, file.length(), is(0l));
			}
		}
	}

	private ApplicationId assertWaitApp(long timeout, TimeUnit unit, YarnCloudAppService yarnCloudAppService) throws Exception {
		ApplicationId applicationId = null;
		Collection<CloudAppInstanceInfo> instances;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		do {
			instances = yarnCloudAppService.getInstances(CloudAppType.STREAM);
			if (instances.size() == 1) {
				CloudAppInstanceInfo cloudAppInstanceInfo = instances.iterator().next();
				if (StringUtils.hasText(cloudAppInstanceInfo.getAddress())) {
					applicationId = ConverterUtils.toApplicationId(cloudAppInstanceInfo.getApplicationId());
					break;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);

		assertThat(applicationId, notNullValue());
		return applicationId;
	}

	@Configuration
	public static class TestYarnConfiguration extends YarnAdminConfiguration {

		@Autowired
		private org.apache.hadoop.conf.Configuration configuration;

		@Autowired
		private Environment environment;

		@Override
		@Bean
		public YarnCloudAppService yarnCloudAppService() {
			ApplicationContextInitializer<?>[] initializers = new ApplicationContextInitializer<?>[] {
					new HadoopConfigurationInjectingInitializer(configuration) };
			String dataflowVersion = environment.getProperty("projectVersion");
			return new DefaultYarnCloudAppService(dataflowVersion, initializers);
		}

	}

}
