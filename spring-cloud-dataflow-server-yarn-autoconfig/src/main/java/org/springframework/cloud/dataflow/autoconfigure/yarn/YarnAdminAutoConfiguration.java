/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.dataflow.autoconfigure.yarn;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.deployer.spi.yarn.AppDeployerStateMachine;
import org.springframework.cloud.deployer.spi.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.TaskLauncherStateMachine;
import org.springframework.cloud.deployer.spi.yarn.YarnAppDeployer;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.YarnTaskLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for dataflow
 * YARN admin.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnClass({ AppDeployer.class, TaskLauncher.class })
@ConditionalOnProperty(prefix = "dataflow.server.yarn", name = "enabled", havingValue = "true", matchIfMissing = true)
public class YarnAdminAutoConfiguration {

	@Value("${spring.cloud.dataflow.yarn.version}")
	private String dataflowVersion;

	@Bean
	public YarnCloudAppService yarnCloudAppService() {
		return new DefaultYarnCloudAppService(dataflowVersion);
	}

	@Bean
	public TaskExecutor yarnModuleDeployerTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(1);
		return executor;
	}

	@Configuration
	@ConditionalOnMissingBean(name = "appDeployer")
	public static class ProcessModuleDeployerConfig {

		@Bean
		public AppDeployerStateMachine appDeployerStateMachine(YarnCloudAppService yarnCloudAppService,
				TaskExecutor yarnModuleDeployerTaskExecutor, BeanFactory beanFactory) throws Exception {
			return new AppDeployerStateMachine(yarnCloudAppService, yarnModuleDeployerTaskExecutor, beanFactory);
		}

		@Bean
		public AppDeployer appDeployer(YarnCloudAppService yarnCloudAppService,
				AppDeployerStateMachine appDeployerStateMachine) throws Exception {
			return new YarnAppDeployer(yarnCloudAppService, appDeployerStateMachine.buildStateMachine());
		}
	}

	@Configuration
	@ConditionalOnMissingBean(name = "taskLauncher")
	public static class TaskModuleDeployerConfig {

		@Bean
		public TaskLauncherStateMachine taskLauncherStateMachine(YarnCloudAppService yarnCloudAppService,
				TaskExecutor yarnModuleDeployerTaskExecutor, BeanFactory beanFactory) throws Exception {
			return new TaskLauncherStateMachine(yarnCloudAppService, yarnModuleDeployerTaskExecutor, beanFactory);
		}

		@Bean
		public TaskLauncher taskLauncher(YarnCloudAppService yarnCloudAppService,
				TaskLauncherStateMachine taskLauncherStateMachine) throws Exception {
			return new YarnTaskLauncher(yarnCloudAppService, taskLauncherStateMachine.buildStateMachine());
		}
	}
}
