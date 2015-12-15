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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoReportData;

/**
 * Default implementation of {@link YarnCloudAppService} which talks to rest
 * api's exposed by specific yarn controlling container clusters.
 *
 * @author Janne Valkealahti
 * @author Mark Fisher
 */
public class DefaultYarnCloudAppService implements YarnCloudAppService, InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(DefaultYarnCloudAppService.class);
	private final ApplicationContextInitializer<?>[] initializers;
	private final String dataflowVersion;
	private final Map<String, YarnCloudAppServiceApplication> appCache = new HashMap<String, YarnCloudAppServiceApplication>();

	/**
	 *
	 * @param dataflowVersion
	 */
	public DefaultYarnCloudAppService(String dataflowVersion) {
		this(dataflowVersion, null);
	}

	/**
	 *
	 * @param dataflowVersion
	 * @param initializers
	 */
	public DefaultYarnCloudAppService(String dataflowVersion, ApplicationContextInitializer<?>[] initializers) {
		this.dataflowVersion = dataflowVersion;
		this.initializers = initializers;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
	}

	@Override
	public Collection<CloudAppInfo> getApplications(CloudAppType cloudAppType) {
		return getApp(null, null, cloudAppType).getPushedApplications();
	}

	@Override
	public Collection<CloudAppInstanceInfo> getInstances(CloudAppType cloudAppType) {
		return getApp(null, null, cloudAppType).getSubmittedApplications();
	}

	@Override
	public void pushApplication(String appVersion, CloudAppType cloudAppType) {
		getApp(appVersion, dataflowVersion, cloudAppType, null).pushApplication(appVersion);
	}

	@Override
	public String submitApplication(String appVersion, CloudAppType cloudAppType) {
		return submitApplication(appVersion, cloudAppType, null);
	}

	@Override
	public String submitApplication(String appVersion, CloudAppType cloudAppType, List<String> contextRunArgs) {
		return getApp(appVersion, dataflowVersion, cloudAppType, contextRunArgs).submitApplication(appVersion);
	}

	@Override
	public void killApplications(String appName, CloudAppType cloudAppType) {
		YarnCloudAppServiceApplication app = getApp(null, null, cloudAppType);
		Collection<CloudAppInstanceInfo> submittedApplications = app.getSubmittedApplications();
		for (CloudAppInstanceInfo info : submittedApplications) {
			if (info.getName() == appName) {
				app.killApplication(info.getApplicationId());
			}
		}
	}

	@Override
	public void createCluster(String yarnApplicationId, String clusterId, int count, String module,
			Map<String, String> definitionParameters) {

		Map<String, Object> extraProperties = new HashMap<String, Object>();
		extraProperties.put("containerModules", module);

		int i = 0;
		for (Map.Entry<String, String> entry : definitionParameters.entrySet()) {
			String value = entry.getValue();
			if (value.startsWith("\"") && value.endsWith("\"")) {
				// escape existing double quotes
				extraProperties.put("containerArg" + i++, entry.getKey() + "=\\" + value.substring(0, value.length()-1) + "\\\"");
			} else {
				// escape with extra double quotes
				extraProperties.put("containerArg" + i++, entry.getKey() + "=\\\"" + value + "\\\"");
			}
		}
		getApp(null, null, CloudAppType.STREAM).createCluster(ConverterUtils.toApplicationId(yarnApplicationId), clusterId, "module-template",
				"default", 1, null, null, null, extraProperties);
	}

	@Override
	public void startCluster(String yarnApplicationId, String clusterId) {
		getApp(null, null, CloudAppType.STREAM).startCluster(ConverterUtils.toApplicationId(yarnApplicationId), clusterId);
	}

	@Override
	public void stopCluster(String yarnApplicationId, String clusterId) {
		getApp(null, null, CloudAppType.STREAM).stopCluster(ConverterUtils.toApplicationId(yarnApplicationId), clusterId);
	}

	@Override
	public Map<String, String> getClustersStates() {
		HashMap<String, String> states = new HashMap<String, String>();
		for (CloudAppInstanceInfo instanceInfo : getInstances(CloudAppType.STREAM)) {
			if (instanceInfo.getName().equals("scdstream:app") && instanceInfo.getState().equals("RUNNING")) {
				for (String cluster : getClusters(instanceInfo.getApplicationId())) {
					states.putAll(getInstanceClustersStates(instanceInfo.getApplicationId(), cluster));
				}
			}
		}
		return states;
	}

	@Override
	public Collection<String> getClusters(String yarnApplicationId) {
		return getApp(null, null, CloudAppType.STREAM).getClustersInfo(ConverterUtils.toApplicationId(yarnApplicationId));
	}

	@Override
	public void destroyCluster(String yarnApplicationId, String clusterId) {
		getApp(null, null, CloudAppType.STREAM).destroyCluster(ConverterUtils.toApplicationId(yarnApplicationId), clusterId);
	}

	private Map<String, String> getInstanceClustersStates(String yarnApplicationId, String clusterId) {
		HashMap<String, String> states = new HashMap<String, String>();
		List<ClustersInfoReportData> clusterInfo = getApp(null, null, CloudAppType.STREAM)
				.getClusterInfo(ConverterUtils.toApplicationId(yarnApplicationId), clusterId);
		if (clusterInfo.size() == 1) {
			states.put(clusterId, clusterInfo.get(0).getState());
		}
		return states;
	}

	private synchronized YarnCloudAppServiceApplication getApp(String appVersion, String dataflowVersion, CloudAppType cloudAppType) {
		return getApp(appVersion, dataflowVersion, cloudAppType, null);
	}

	private synchronized YarnCloudAppServiceApplication getApp(String appVersion, String dataflowVersion, CloudAppType cloudAppType,
			List<String> contextRunArgs) {
		String cacheKey = cloudAppType + appVersion + StringUtils.collectionToCommaDelimitedString(contextRunArgs);
		YarnCloudAppServiceApplication app = appCache.get(cacheKey);
		logger.info("Cachekey {} found YarnCloudAppServiceApplication {}", cacheKey, app);
		if (app == null) {
			Properties configFileProperties = new Properties();
			if (StringUtils.hasText(appVersion)) {
				configFileProperties.setProperty("spring.yarn.applicationVersion", appVersion);
			}
			if (StringUtils.hasText(dataflowVersion)) {
				configFileProperties.setProperty("spring.cloud.dataflow.yarn.version", dataflowVersion);
			}

			logger.info("Bootsrapping YarnCloudAppServiceApplication with {}", cloudAppType.toString().toLowerCase());
			ArrayList<String> runArgs = new ArrayList<String>();
			runArgs.add("--spring.config.name=" + cloudAppType.toString().toLowerCase());
			if (!ObjectUtils.isEmpty(contextRunArgs)) {
				runArgs.addAll(contextRunArgs);
			}

			app = new YarnCloudAppServiceApplication(appVersion, dataflowVersion, "application.properties", configFileProperties,
					runArgs.toArray(new String[0]), initializers);
			try {
				app.afterPropertiesSet();
			} catch (Exception e) {
				throw new RuntimeException("Error initializing YarnCloudAppServiceApplication", e);
			}
			logger.info("Set cache with key {} and YarnCloudAppServiceApplication {}", cacheKey, app);
			appCache.put(cacheKey, app);
		}
		return app;
	}

}
