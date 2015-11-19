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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppServiceApplication;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.junit.ApplicationInfo;

/**
 * Integration tests for {@link YarnCloudAppServiceApplication}.
 *
 * Tests can be run in sts if project is build first. Build
 * prepares needed yarn files in expected paths.
 * $ mvn clean package -DskipTests
 *
 * @author Janne Valkealahti
 *
 */
public class YarnCloudAppServiceApplicationIT extends AbstractCliBootYarnClusterTests {

	@Test
	public void testStream1() throws Exception {
		Properties instanceProperties = new Properties();
		instanceProperties.setProperty("spring.yarn.applicationVersion", "app");
		instanceProperties.setProperty("spring.cloud.dataflow.yarn.version", getProjectVersion());
		ApplicationContextInitializer<?>[] initializers = new ApplicationContextInitializer<?>[] {
				new HadoopConfigurationInjectingInitializer(getConfiguration()) };
		YarnCloudAppServiceApplication app = new YarnCloudAppServiceApplication("app", getProjectVersion(),
				"application.properties", instanceProperties, null, initializers);

		app.afterPropertiesSet();
		setYarnClient(app.getContext().getBean(YarnClient.class));

		app.pushApplication("app");
		Collection<CloudAppInfo> pushedApplications = app.getPushedApplications();
		assertThat(pushedApplications.size(), is(1));

		String appId = app.submitApplication("app");
		ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
		ApplicationInfo info = waitState(applicationId, 60, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertThat(info.getYarnApplicationState(), is(YarnApplicationState.RUNNING));

		Collection<CloudAppInstanceInfo> submittedApplications = app.getSubmittedApplications();
		assertThat(submittedApplications.size(), is(1));

		Collection<String> clustersInfo = app.getClustersInfo(applicationId);
		assertThat(clustersInfo.size(), is(0));

		// before SHDP-532 gets in, create/start individually
		Map<String, Object> extraProperties1 = new HashMap<String, Object>();
		extraProperties1.put("containerModules", "org.springframework.cloud.stream.module:log-sink:jar:exec:1.0.0.BUILD-SNAPSHOT");
		extraProperties1.put("containerArg1", "spring.cloud.stream.bindings.input.destination=ticktock.0");
		app.createCluster(applicationId, "ticktock:log", "module-template", "default", 1, null, null, null, extraProperties1);

		app.startCluster(applicationId, "ticktock:log");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started LogSinkApplication");

		Map<String, Object> extraProperties2 = new HashMap<String, Object>();
		extraProperties2.put("containerModules", "org.springframework.cloud.stream.module:time-source:jar:exec:1.0.0.BUILD-SNAPSHOT");
		extraProperties2.put("containerArg1", "spring.cloud.stream.bindings.output.destination=ticktock.0");
		app.createCluster(applicationId, "ticktock:time", "module-template", "default", 1, null, null, null, extraProperties2);

		app.startCluster(applicationId, "ticktock:time");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");

		app.destroy();
	}

	@Test
	public void testStream2() throws Exception {
		String fsUri = getConfiguration().get("fs.defaultFS");
		Properties instanceProperties = new Properties();
		instanceProperties.setProperty("spring.yarn.applicationVersion", "app");
		instanceProperties.setProperty("spring.cloud.dataflow.yarn.version", getProjectVersion());
		instanceProperties.setProperty("spring.hadoop.fsUri", fsUri);
		ApplicationContextInitializer<?>[] initializers = new ApplicationContextInitializer<?>[] {
				new HadoopConfigurationInjectingInitializer(getConfiguration()) };
		YarnCloudAppServiceApplication app = new YarnCloudAppServiceApplication("app", getProjectVersion(),
				"application.properties", instanceProperties, null, initializers);

		app.afterPropertiesSet();
		setYarnClient(app.getContext().getBean(YarnClient.class));

		app.pushApplication("app");
		Collection<CloudAppInfo> pushedApplications = app.getPushedApplications();
		assertThat(pushedApplications.size(), is(1));

		String appId = app.submitApplication("app");
		ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
		ApplicationInfo info = waitState(applicationId, 60, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertThat(info.getYarnApplicationState(), is(YarnApplicationState.RUNNING));

		Collection<CloudAppInstanceInfo> submittedApplications = app.getSubmittedApplications();
		assertThat(submittedApplications.size(), is(1));

		Collection<String> clustersInfo = app.getClustersInfo(applicationId);
		assertThat(clustersInfo.size(), is(0));

		// before SHDP-532 gets in, create/start individually
		Map<String, Object> extraProperties1 = new HashMap<String, Object>();
		extraProperties1.put("containerModules", "org.springframework.cloud.stream.module:hdfs-sink:jar:exec:1.0.0.BUILD-SNAPSHOT");
		extraProperties1.put("containerArg1", "spring.cloud.stream.bindings.input.destination=ticktock.0");
		app.createCluster(applicationId, "ticktock:hdfs", "module-template", "default", 1, null, null, null, extraProperties1);

		app.startCluster(applicationId, "ticktock:hdfs");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started HdfsSinkApplication");

		Map<String, Object> extraProperties2 = new HashMap<String, Object>();
		extraProperties2.put("containerModules", "org.springframework.cloud.stream.module:time-source:jar:exec:1.0.0.BUILD-SNAPSHOT");
		extraProperties2.put("containerArg1", "spring.cloud.stream.bindings.output.destination=ticktock.0");
		app.createCluster(applicationId, "ticktock:time", "module-template", "default", 1, null, null, null, extraProperties2);

		app.startCluster(applicationId, "ticktock:time");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");

		waitHdfsFile("/tmp/hdfs-sink/data-0.txt", 2, TimeUnit.MINUTES);

		app.destroy();
	}

}
