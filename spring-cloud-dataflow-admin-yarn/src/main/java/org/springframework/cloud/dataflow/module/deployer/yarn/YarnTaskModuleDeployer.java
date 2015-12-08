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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.dataflow.core.ArtifactCoordinates;
import org.springframework.cloud.dataflow.core.ModuleDefinition;
import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;
import org.springframework.cloud.dataflow.module.ModuleStatus;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppTaskStateMachine.Events;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppTaskStateMachine.States;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateMachine;

/**
 * {@link ModuleDeployer} which is launching tasks as Yarn applications.
 *
 * @author Janne Valkealahti
 */
public class YarnTaskModuleDeployer implements ModuleDeployer {

	private static final Logger logger = LoggerFactory.getLogger(YarnTaskModuleDeployer.class);
	private final YarnCloudAppService yarnCloudAppService;
	private final StateMachine<States, Events> stateMachine;

	/**
	 * Instantiates a new yarn task module deployer.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param stateMachine the state machine
	 */
	public YarnTaskModuleDeployer(YarnCloudAppService yarnCloudAppService, StateMachine<States, Events> stateMachine) {
		this.yarnCloudAppService = yarnCloudAppService;
		this.stateMachine = stateMachine;
	}

	@Override
	public ModuleDeploymentId deploy(ModuleDeploymentRequest request) {
		logger.info("Deploy request for {}", request);
		ArtifactCoordinates coordinates = request.getCoordinates();
		ModuleDefinition definition = request.getDefinition();
		ModuleDeploymentId id = ModuleDeploymentId.fromModuleDefinition(definition);
		String module = coordinates.toString();
		Map<String, String> definitionParameters = definition.getParameters();
		Map<String, String> deploymentProperties = request.getDeploymentProperties();
		String appName = moduleDeploymentIdToAppName(id);

		logger.info("deploying request for definition: " + definition);
		logger.info("deploying module: " + module);
		logger.info("definitionParameters: " + definitionParameters);
		logger.info("deploymentProperties: " + deploymentProperties);

		ArrayList<String> contextRunArgs = new ArrayList<String>();
		contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--dataflow.module.coordinates=" + module);
		contextRunArgs.add("--spring.yarn.appName=" + appName);
		for (Entry<String, String> entry : definitionParameters.entrySet()) {
			contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--dataflow.module.parameters." + entry.getKey() + ".='" + entry.getValue() + "'");
		}

		Message<Events> message = MessageBuilder.withPayload(Events.DEPLOY)
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_APP_VERSION, "app")
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_MODULE, module)
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_DEFINITION_PARAMETERS, definitionParameters)
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();
		stateMachine.sendEvent(message);
		return id;
	}

	@Override
	public void undeploy(ModuleDeploymentId id) {
		String appName = moduleDeploymentIdToAppName(id);
		logger.info("Undeploy request for module {}, killing appName {}", id, appName);
		Message<Events> message = MessageBuilder.withPayload(Events.UNDEPLOY)
				.setHeader(YarnCloudAppTaskStateMachine.HEADER_APP_NAME, appName)
				.build();
		stateMachine.sendEvent(message);
	}

	@Override
	public ModuleStatus status(ModuleDeploymentId id) {
		logger.info("Status request for module {}", id);
		ModuleStatus moduleStatus = status().get(id);
		if (moduleStatus == null) {
			moduleStatus = ModuleStatus.of(id)
					.with(new YarnModuleInstanceStatus(id.toString(), false, Collections.<String, String> emptyMap()))
					.build();
		}
		return moduleStatus;
	}

	@Override
	public Map<ModuleDeploymentId, ModuleStatus> status() {
		logger.info("Status request for all modules");
		Map<ModuleDeploymentId, ModuleStatus> statuses = new HashMap<ModuleDeploymentId, ModuleStatus>();
		Collection<CloudAppInstanceInfo> instances = yarnCloudAppService.getInstances(CloudAppType.TASK);
		for (CloudAppInstanceInfo instance : instances) {
			if (instance.getName().startsWith("scdtask:")) {
				ModuleDeploymentId id = appNameToModuleDeploymentId(instance.getName());
				YarnModuleInstanceStatus status = new YarnModuleInstanceStatus(id.toString(),
						instance.getState() == "RUNNING", null);
				statuses.put(id, ModuleStatus.of(id).with(status).build());
			}
		}
		return statuses;
	}

	private static String moduleDeploymentIdToAppName(ModuleDeploymentId id) {
		return "scdtask:" + id.getGroup() + ":" + id.getLabel();
	}

	private static ModuleDeploymentId appNameToModuleDeploymentId(String appName) {
		String[] split = appName.split(":");
		if (split.length == 3) {
			return new ModuleDeploymentId(split[1], split[2]);
		} else {
			throw new IllegalArgumentException("Invalid appName=[" + appName + "]");
		}
	}

}
