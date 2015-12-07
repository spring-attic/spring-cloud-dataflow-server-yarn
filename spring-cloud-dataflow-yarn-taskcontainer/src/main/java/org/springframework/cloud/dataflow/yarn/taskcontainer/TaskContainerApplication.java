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

package org.springframework.cloud.dataflow.yarn.taskcontainer;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.dataflow.yarn.common.DataflowModuleYarnProperties;
import org.springframework.cloud.stream.module.launcher.ModuleLaunchRequest;
import org.springframework.cloud.stream.module.launcher.ModuleLauncher;
import org.springframework.cloud.stream.module.launcher.ModuleLauncherConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.util.Assert;

@SpringBootApplication
@Import(ModuleLauncherConfiguration.class)
@EnableConfigurationProperties(DataflowModuleYarnProperties.class)
public class TaskContainerApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(TaskContainerApplication.class);

	@Autowired
	private ModuleLauncher moduleLauncher;

	@Autowired
	private DataflowModuleYarnProperties dataflowModuleYarnProperties;

	@Override
	public void run(String... args) throws Exception {
		Assert.hasText(dataflowModuleYarnProperties.getCoordinates(), "Module coordinates must be set");
		logger.info("Launching task module with {}", dataflowModuleYarnProperties);
		moduleLauncher.launch(Arrays.asList(new ModuleLaunchRequest(
				dataflowModuleYarnProperties.getCoordinates(),
				dataflowModuleYarnProperties.getParameters())));
	}

	public static void main(String[] args) {
		SpringApplication.run(TaskContainerApplication.class, args);
	}

}
