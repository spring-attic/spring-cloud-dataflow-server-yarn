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

import java.util.Collection;
import java.util.List;

import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.StateMachineBuilder.Builder;
import org.springframework.statemachine.guard.Guard;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Class keeping all {@link StateMachine} logic in one place and is used
 * to dynamically build a machine.
 *
 * @author Janne Valkealahti
 */
public class YarnCloudAppTaskStateMachine {

	static final String VAR_APP_VERSION = "appVersion";
	static final String VAR_APPLICATION_ID = "applicationId";
	static final String HEADER_APP_VERSION = "appVersion";
	static final String HEADER_APP_NAME = "appName";
	static final String HEADER_MODULE = "module";
	static final String HEADER_DEFINITION_PARAMETERS = "definitionParameters";
	static final String HEADER_CONTEXT_RUN_ARGS = "contextRunArgs";
	static final String HEADER_ERROR = "error";

	private final YarnCloudAppService yarnCloudAppService;
	private final TaskExecutor taskExecutor;

	/**
	 * Instantiates a new yarn cloud app state machine.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param taskExecutor the task executor
	 */
	public YarnCloudAppTaskStateMachine(YarnCloudAppService yarnCloudAppService, TaskExecutor taskExecutor) {
		Assert.notNull(yarnCloudAppService, "YarnCloudAppService must be set");
		Assert.notNull(taskExecutor, "TaskExecutor must be set");
		this.yarnCloudAppService = yarnCloudAppService;
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Builds the state machine and instructs it to start automatically.
	 *
	 * @return the state machine
	 * @throws Exception the exception
	 */
	public StateMachine<States, Events> buildStateMachine() throws Exception {
		return buildStateMachine(true);
	}

	/**
	 * Builds the state machine.
	 *
	 * @param autoStartup the auto startup
	 * @return the state machine
	 * @throws Exception the exception
	 */
	public StateMachine<States, Events> buildStateMachine(boolean autoStartup) throws Exception {
		Builder<States, Events> builder = StateMachineBuilder.builder();

		builder.configureConfiguration()
			.withConfiguration()
				.autoStartup(autoStartup)
				.taskExecutor(taskExecutor);

		builder.configureStates()
			.withStates()
				.initial(States.READY)
				.state(States.ERROR)
				.state(States.DEPLOYMODULE, new ResetVariablesAction(), null)
				.state(States.DEPLOYMODULE, Events.DEPLOY, Events.UNDEPLOY)
				.state(States.UNDEPLOYMODULE, new ResetVariablesAction(), null)
				.state(States.UNDEPLOYMODULE, Events.DEPLOY, Events.UNDEPLOY)
				.and()
				.withStates()
					.parent(States.DEPLOYMODULE)
					.initial(States.CHECKAPP)
					.state(States.CHECKAPP, new CheckAppAction(), null)
					.choice(States.PUSHAPPCHOICE)
					.state(States.PUSHAPP, new PushAppAction(), null)
					.state(States.STARTINSTANCE, new StartInstanceAction(), null)
					.and()
				.withStates()
					.parent(States.UNDEPLOYMODULE)
					.initial(States.STOPINSTANCE)
					.state(States.STOPINSTANCE, new StopInstanceAction(), null);

		builder.configureTransitions()
			.withExternal()
				.source(States.DEPLOYMODULE).target(States.ERROR)
				.event(Events.ERROR)
				.and()
			.withExternal()
				.source(States.DEPLOYMODULE).target(States.READY)
				.event(Events.CONTINUE)
				.and()
			.withExternal()
				.source(States.UNDEPLOYMODULE).target(States.READY)
				.event(Events.CONTINUE)
				.and()
			.withExternal()
				.source(States.STARTINSTANCE).target(States.READY)
				.and()
			.withExternal()
				.source(States.STOPINSTANCE).target(States.READY)
				.and()
			.withExternal()
				.source(States.READY).target(States.DEPLOYMODULE)
				.event(Events.DEPLOY)
				.and()
			.withExternal()
				.source(States.READY).target(States.UNDEPLOYMODULE)
				.event(Events.UNDEPLOY)
				.and()
			.withExternal()
				.source(States.CHECKAPP).target(States.PUSHAPPCHOICE)
				.and()
			.withChoice()
				.source(States.PUSHAPPCHOICE)
				.first(States.PUSHAPP, new PushAppGuard())
				.last(States.STARTINSTANCE)
				.and()
			.withExternal()
				.source(States.PUSHAPP).target(States.STARTINSTANCE);

		return builder.build();
	}

	/**
	 * {@link Action} which clears existing extended state variables.
	 */
	private class ResetVariablesAction implements Action<States, Events> {

		@Override
		public void execute(StateContext<States, Events> context) {
			context.getExtendedState().getVariables().clear();
		}
	}

	/**
	 * {@link Action} which queries {@link YarnCloudAppService} and checks if
	 * passed {@code appVersion} from event headers exists and sends {@code ERROR}
	 * event into state machine if it doesn't exist. Add to be used {@code appVersion}
	 * into extended state variables which later used by other guards and actions.
	 */
	private class CheckAppAction implements Action<States, Events> {

		@Override
		public void execute(StateContext<States, Events> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);

			if (!StringUtils.hasText(appVersion)) {
				context.getStateMachine().sendEvent(
						MessageBuilder.withPayload(Events.ERROR).setHeader(HEADER_ERROR, "appVersion not defined")
								.build());
			} else {
				Collection<CloudAppInfo> appInfos = yarnCloudAppService.getApplications(CloudAppType.TASK);
				for (CloudAppInfo appInfo : appInfos) {
					if (appInfo.getName().equals(appVersion)) {
						context.getExtendedState().getVariables().put(VAR_APP_VERSION, appVersion);
					}
				}
			}
		}
	}

	/**
	 * {@link Guard} which is used to protect state where application push
	 * into hdfs would happen. Assumes that if {@code appVersion} variable
	 * exists, application is installed.
	 */
	private class PushAppGuard implements Guard<States, Events> {

		@Override
		public boolean evaluate(StateContext<States, Events> context) {
			return !context.getExtendedState().getVariables().containsKey(VAR_APP_VERSION);
		}
	}

	/**
	 * {@link Action} which pushes application version into hdfs found
	 * from variable {@code appVersion}.
	 */
	private class PushAppAction implements Action<States, Events> {

		@Override
		public void execute(StateContext<States, Events> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);
			yarnCloudAppService.pushApplication(appVersion, CloudAppType.TASK);
		}
	}

	/**
	 * {@link Action} which launches new application instance.
	 */
	private class StartInstanceAction implements Action<States, Events> {

		@Override
		public void execute(StateContext<States, Events> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);

			// we control type so casting is safe
			@SuppressWarnings("unchecked")
			List<String> contextRunArgs = (List<String>) context.getMessageHeader(HEADER_CONTEXT_RUN_ARGS);

			String applicationId = yarnCloudAppService.submitApplication(appVersion, CloudAppType.TASK, contextRunArgs);
			context.getExtendedState().getVariables().put(VAR_APPLICATION_ID, applicationId);
		}

	}

	/**
	 * {@link Action} which stops an application instance.
	 */
	private class StopInstanceAction implements Action<States, Events> {

		@Override
		public void execute(StateContext<States, Events> context) {
			// we don't have no way to match running task instance
			String appName = (String) context.getMessageHeader(HEADER_APP_NAME);
			yarnCloudAppService.killApplications(appName, CloudAppType.TASK);
		}
	}

	/**
	 * Enumeration of module handling states.
	 */
	public enum States {

		/** Main state where machine is ready for either deploy or undeploy requests. */
		READY,

		/** State where possible errors are handled. */
		ERROR,

		/** Super state of all other states handling deployment. */
		DEPLOYMODULE,

		/** State where app presence in hdfs is checked. */
		CHECKAPP,

		/** Pseudostate where choice to enter {@code PUSHAPP} is made. */
		PUSHAPPCHOICE,

		/** State where application is pushed into hdfs. */
		PUSHAPP,

		/** State where app instance is started. */
		STARTINSTANCE,

		/** Super state of all other states handling undeployment. */
		UNDEPLOYMODULE,

		/** State where app instance is stopped. */
		STOPINSTANCE;
	}

	/**
	 * Enumeration of module handling events.
	 */
	public enum Events {

		/** Event indicating that machine should handle deploy request. */
		DEPLOY,

		/** Event indicating that machine should handle undeploy request. */
		UNDEPLOY,

		/** Event indicating that machine should move into error handling logic. */
		ERROR,

		/** Event indicating that machine should move back into ready state. */
		CONTINUE
	}

}
