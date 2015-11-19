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

package org.springframework.cloud.dataflow.admin.spi.yarn;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppStateMachine;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnModuleDeployer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configuration for creating module deployers for Apache Yarn.
 *
 * @author Janne Valkealahti
 * @author Ilayaperumal Gopinathan
 */
@Configuration
public class YarnAdminConfiguration {

	@Value("${spring.cloud.bootstrap.name:admin}")
	private String bootstrapName;

	@Value("${spring.cloud.dataflow.yarn.version}")
	private String dataflowVersion;

	@Bean
	public ModuleDeployer processModuleDeployer() throws Exception {
		return new YarnModuleDeployer(yarnCloudAppService(), yarnCloudAppStateMachine().buildStateMachine());
	}

	@Bean
	public ModuleDeployer taskModuleDeployer() throws Exception {
		// TODO: not yet supported but using same deployer for admin not to fail
		return new YarnModuleDeployer(yarnCloudAppService(), yarnCloudAppStateMachine().buildStateMachine(false));
	}

	@Bean
	public YarnCloudAppStateMachine yarnCloudAppStateMachine() throws Exception {
		return new YarnCloudAppStateMachine(yarnCloudAppService(), yarnModuleDeployerTaskExecutor());
	}

	@Bean
	public YarnCloudAppService yarnCloudAppService() {
		return new DefaultYarnCloudAppService(bootstrapName, dataflowVersion);
	}

	@Bean
	public TaskExecutor yarnModuleDeployerTaskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(1);
		return taskExecutor;
	}

}
