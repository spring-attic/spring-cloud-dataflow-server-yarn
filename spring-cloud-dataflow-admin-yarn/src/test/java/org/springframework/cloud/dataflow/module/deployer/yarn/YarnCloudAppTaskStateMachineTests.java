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

package org.springframework.cloud.dataflow.module.deployer.yarn;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppTaskStateMachine.Events;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppTaskStateMachine.States;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.statemachine.test.StateMachineTestPlan;
import org.springframework.statemachine.test.StateMachineTestPlanBuilder;

public class YarnCloudAppTaskStateMachineTests {

	@Test
	public void testDeployShouldPushAndStart() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		TestYarnCloudAppService yarnCloudAppService = new TestYarnCloudAppService();
		TaskExecutor taskExecutor = context.getBean(TaskExecutor.class);
		YarnCloudAppTaskStateMachine ycasm = new YarnCloudAppTaskStateMachine(yarnCloudAppService, taskExecutor);
		StateMachine<States, Events> stateMachine = ycasm.buildStateMachine(false);
		TestStateMachineListener listener = new TestStateMachineListener();
		stateMachine.addStateListener(listener);
		stateMachine.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS), is(true));

		ArrayList<String> contextRunArgs = new ArrayList<String>();

		Message<Events> message = MessageBuilder.withPayload(Events.DEPLOY)
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_APP_VERSION, "fakeApp")
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_MODULE, "fakeModule")
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_DEFINITION_PARAMETERS, new HashMap<Object, Object>())
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();

		StateMachineTestPlan<States, Events> plan =
				StateMachineTestPlanBuilder.<States, Events>builder()
					.defaultAwaitTime(10)
					.stateMachine(stateMachine)
					.step()
						.expectStates(States.READY)
						.and()
					.step()
						.sendEvent(message)
						.expectStateChanged(5)
						.expectStates(States.READY)
						.and()
					.build();
		plan.test();

		assertThat(yarnCloudAppService.getApplicationsLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.getApplicationsCount, is(1));

		assertThat(yarnCloudAppService.pushApplicationLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.pushApplicationCount.size(), is(1));
		assertThat(yarnCloudAppService.pushApplicationCount.get(0).appVersion, is("fakeApp"));

		assertThat(yarnCloudAppService.submitApplicationLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.submitApplicationCount.size(), is(1));
		assertThat(yarnCloudAppService.submitApplicationCount.get(0).appVersion, is("fakeApp"));

		context.close();
	}

	@Test
	public void testDeployTwice() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		TestYarnCloudAppService yarnCloudAppService = new TestYarnCloudAppService();
		TaskExecutor taskExecutor = context.getBean(TaskExecutor.class);
		YarnCloudAppTaskStateMachine ycasm = new YarnCloudAppTaskStateMachine(yarnCloudAppService, taskExecutor);
		StateMachine<States, Events> stateMachine = ycasm.buildStateMachine(false);
		TestStateMachineListener listener = new TestStateMachineListener();
		stateMachine.addStateListener(listener);
		stateMachine.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS), is(true));

		ArrayList<String> contextRunArgs = new ArrayList<String>();

		Message<Events> message = MessageBuilder.withPayload(Events.DEPLOY)
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_APP_VERSION, "fakeApp")
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_MODULE, "fakeModule")
				.setHeader(YarnCloudAppStreamStateMachine.HEADER_DEFINITION_PARAMETERS, new HashMap<Object, Object>())
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();

		StateMachineTestPlan<States, Events> plan =
				StateMachineTestPlanBuilder.<States, Events>builder()
					.defaultAwaitTime(10)
					.stateMachine(stateMachine)
					.step()
						.expectStates(States.READY)
						.and()
					.step()
						.sendEvent(message)
						.sendEvent(message)
						.expectStateChanged(9)
						.expectStates(States.READY)
						.and()
					.build();
		plan.test();

		assertThat(yarnCloudAppService.getApplicationsLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.getApplicationsCount, is(2));

		assertThat(yarnCloudAppService.pushApplicationLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.pushApplicationCount.size(), is(1));
		assertThat(yarnCloudAppService.pushApplicationCount.get(0).appVersion, is("fakeApp"));

		assertThat(yarnCloudAppService.submitApplicationLatch.await(2, TimeUnit.SECONDS), is(true));
		assertThat(yarnCloudAppService.submitApplicationCount.size(), is(2));
		assertThat(yarnCloudAppService.submitApplicationCount.get(0).appVersion, is("fakeApp"));

		context.close();
	}

	@Configuration
	static class Config {

		@Bean
		TaskExecutor taskExecutor() {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(1);
			return taskExecutor;
		}

	}

	private static class TestYarnCloudAppService implements YarnCloudAppService {

		volatile String app = null;
		volatile String instance = null;

		final CountDownLatch getApplicationsLatch = new CountDownLatch(1);
		final CountDownLatch pushApplicationLatch = new CountDownLatch(1);
		final CountDownLatch submitApplicationLatch = new CountDownLatch(1);
		final CountDownLatch createClusterLatch = new CountDownLatch(1);
		final CountDownLatch startClusterLatch = new CountDownLatch(1);
		final CountDownLatch stopClusterLatch = new CountDownLatch(1);

		volatile int getApplicationsCount = 0;

		final List<Wrapper> pushApplicationCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> submitApplicationCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> createClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> startClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> stopClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());

		@Override
		public Collection<CloudAppInfo> getApplications(CloudAppType cloudAppType) {
			ArrayList<CloudAppInfo> infos = new ArrayList<CloudAppInfo>();
			if (app != null) {
				infos.add(new CloudAppInfo(app));
			}
			getApplicationsCount++;
			getApplicationsLatch.countDown();
			return infos;
		}

		@Override
		public Collection<CloudAppInstanceInfo> getInstances(CloudAppType cloudAppType) {
			ArrayList<CloudAppInstanceInfo> infos = new ArrayList<CloudAppInstanceInfo>();
			if (instance != null) {
				infos.add(new CloudAppInstanceInfo("fakeApplicationId", instance, "fakestate", "http://fakeAddress"));
			}
			return infos;
		}

		@Override
		public void pushApplication(String appVersion, CloudAppType cloudAppType) {
			app = appVersion;
			pushApplicationCount.add(new Wrapper(appVersion));
			pushApplicationLatch.countDown();
		}

		@Override
		public String submitApplication(String appVersion, CloudAppType cloudAppType) {
			instance = "spring-cloud-dataflow-yarn-app_" + appVersion;
			submitApplicationCount.add(new Wrapper(appVersion));
			submitApplicationLatch.countDown();
			return "fakeApplicationId";
		}

		@Override
		public String submitApplication(String appVersion, CloudAppType cloudAppType, List<String> contextRunArgs) {
			instance = "spring-cloud-dataflow-yarn-app_" + appVersion;
			submitApplicationCount.add(new Wrapper(appVersion));
			submitApplicationLatch.countDown();
			return "fakeApplicationId";
		}

		@Override
		public void killApplications(String appName, CloudAppType cloudAppType) {
		}

		@Override
		public void createCluster(String yarnApplicationId, String clusterId, int count, String module,
				Map<String, String> definitionParameters) {
			createClusterCount.add(new Wrapper(yarnApplicationId, clusterId, count, module, definitionParameters));
			createClusterLatch.countDown();
		}

		@Override
		public void startCluster(String yarnApplicationId, String clusterId) {
			startClusterCount.add(new Wrapper(yarnApplicationId, clusterId));
			startClusterLatch.countDown();
		}

		@Override
		public void stopCluster(String yarnApplicationId, String clusterId) {
			stopClusterCount.add(new Wrapper(yarnApplicationId, clusterId));
			stopClusterLatch.countDown();
		}

		@Override
		public Map<String, String> getClustersStates() {
			return null;
		}

		@Override
		public Collection<String> getClusters(String yarnApplicationId) {
			return null;
		}

		@Override
		public void destroyCluster(String yarnApplicationId, String clusterId) {
		}

		@SuppressWarnings("unused")
		static class Wrapper {
			String appVersion;
			String yarnApplicationId;
			String clusterId;
			int count;
			String module;
			Map<?, ?> definitionParameters;

			public Wrapper(String appVersion) {
				this.appVersion = appVersion;
			}

			public Wrapper(String yarnApplicationId, String clusterId) {
				this.yarnApplicationId = yarnApplicationId;
				this.clusterId = clusterId;
			}

			public Wrapper(String yarnApplicationId, String clusterId, int count, String module,
					Map<?, ?> definitionParameters) {
				this.yarnApplicationId = yarnApplicationId;
				this.clusterId = clusterId;
				this.count = count;
				this.module = module;
				this.definitionParameters = definitionParameters;
			}

		}

	}

	private static class TestStateMachineListener extends StateMachineListenerAdapter<States, Events> {

		final CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void stateMachineStarted(StateMachine<States, Events> stateMachine) {
			latch.countDown();
		}
	}

}
