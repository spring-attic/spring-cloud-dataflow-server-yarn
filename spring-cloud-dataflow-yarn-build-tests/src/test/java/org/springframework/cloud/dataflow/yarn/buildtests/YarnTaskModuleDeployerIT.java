package org.springframework.cloud.dataflow.yarn.buildtests;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
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
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnTaskModuleDeployer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;
import org.springframework.yarn.test.support.ContainerLogUtils;

/**
 * Integration tests for {@link YarnTaskModuleDeployer}.
 *
 * Tests can be run in sts if project is build first. Build
 * prepares needed yarn files in expected paths.
 * $ mvn clean package -DskipTests
 *
 * @author Janne Valkealahti
 *
 */
public class YarnTaskModuleDeployerIT extends AbstractCliBootYarnClusterTests {

	private static final String GROUP_ID = "org.springframework.cloud.task.module";
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
	public void testTaskTimestamp() throws Exception {
		assertThat(context.containsBean("taskModuleDeployer"), is(true));
		assertThat(context.getBean("taskModuleDeployer"), instanceOf(YarnTaskModuleDeployer.class));
		ModuleDeployer deployer = context.getBean("taskModuleDeployer", ModuleDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);

		ModuleDefinition taskDefinition = new ModuleDefinition.Builder()
				.setGroup("task")
				.setName("timestamp")
				.setParameter("spring.cloud.stream.bindings.output", "task.0")
				.build();
		ArtifactCoordinates taskCoordinates = new ArtifactCoordinates.Builder()
				.setGroupId(GROUP_ID)
				.setArtifactId("timestamp-task")
				.setVersion(VERSION)
				.setClassifier("exec")
				.build();
		ModuleDeploymentRequest task = new ModuleDeploymentRequest(taskDefinition, taskCoordinates);

		ModuleDeploymentId taskId = deployer.deploy(task);
		assertThat(taskId, notNullValue());
		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimestampTaskApplication");

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(4));

//		for (Resource res : resources) {
//			File file = res.getFile();
//			String content = ContainerLogUtils.getFileContent(file);
//			if (file.getName().endsWith("stdout")) {
//				assertThat(file.length(), greaterThan(0l));
//			} else if (file.getName().endsWith("stderr")) {
//				assertThat("stderr with content: " + content, file.length(), is(0l));
//			}
//		}
	}

	private ApplicationId assertWaitApp(long timeout, TimeUnit unit, YarnCloudAppService yarnCloudAppService) throws Exception {
		ApplicationId applicationId = null;
		Collection<CloudAppInstanceInfo> instances;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		do {
			instances = yarnCloudAppService.getInstances(CloudAppType.TASK);
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
