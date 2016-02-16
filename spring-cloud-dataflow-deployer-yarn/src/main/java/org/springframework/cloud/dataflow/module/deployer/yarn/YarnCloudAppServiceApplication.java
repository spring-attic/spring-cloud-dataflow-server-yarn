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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.EndpointMBeanExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInfo;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.AbstractContainerClusterRequest.ProjectionDataType;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterCreateRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.ContainerClusterModifyRequest;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.ContainerClusterResource;
import org.springframework.yarn.boot.actuate.endpoint.mvc.domain.YarnContainerClusterEndpointResource;
import org.springframework.yarn.boot.app.YarnContainerClusterOperations;
import org.springframework.yarn.boot.app.YarnContainerClusterTemplate;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.client.ApplicationDescriptor;
import org.springframework.yarn.client.ApplicationYarnClient;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoReportData;

/**
 * Boot application wrapper combining needed features from application classes
 * from 'org.springframework.yarn.boot.app'. Allows to instantiate context only
 * once making individual command execution much faster still providing all
 * goodies from boot.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnCloudAppServiceApplication implements InitializingBean, DisposableBean {

	private ConfigurableApplicationContext context;
	private ApplicationContextInitializer<?>[] initializers;
	private List<Object> sources = new ArrayList<Object>();
	private List<String> profiles = new ArrayList<String>();
	private Properties appProperties = new Properties();
	private Map<String, Properties> configFilesContents = new HashMap<String, Properties>();
	private String[] args = new String[0];

	private final HashMap<String, YarnContainerClusterOperations> operationsCache = new HashMap<String, YarnContainerClusterOperations>();

	private ApplicationYarnClient yarnClient;
	private SpringYarnProperties springYarnProperties;
	private RestTemplate restTemplate;

	public YarnCloudAppServiceApplication(String applicationVersion, String dataflowVersion, String configFileName,
			Properties configFileProperties, String[] runArgs, ApplicationContextInitializer<?>... initializers) {
		if (StringUtils.hasText(applicationVersion)) {
			appProperties.setProperty("spring.yarn.applicationVersion", applicationVersion);
		}
		if (StringUtils.hasText(dataflowVersion)) {
			appProperties.setProperty("spring.cloud.dataflow.yarn.version", dataflowVersion);
		}
		if (StringUtils.hasText(configFileName) && configFileProperties != null) {
			configFilesContents.put(configFileName, configFileProperties);
		}
		if (runArgs != null) {
			this.args = runArgs;
		}
		this.initializers = initializers;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(Config.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		SpringYarnBootUtils.addApplicationListener(builder, appProperties);
		if (initializers != null) {
			builder.initializers(initializers);
		}
		SpringYarnBootUtils.addConfigFilesContents(builder, configFilesContents);
		context = builder.run(args);
		YarnClient client = context.getBean(YarnClient.class);
		if (client instanceof ApplicationYarnClient) {
			yarnClient = ((ApplicationYarnClient) client);
		} else {
			throw new IllegalArgumentException("YarnClient need to be instanceof ApplicationYarnClient");
		}
		restTemplate = context.getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE, RestTemplate.class);
		springYarnProperties = context.getBean(SpringYarnProperties.class);
	}

	@Override
	public void destroy() throws Exception {
		if (context != null) {
			context.close();
		}
		context = null;
	}

	public ApplicationContext getContext() {
		return context;
	}

	public Collection<CloudAppInfo> getPushedApplications() {
		List<CloudAppInfo> apps = new ArrayList<CloudAppInfo>();
		YarnConfiguration yarnConfiguration = context.getBean("yarnConfiguration", YarnConfiguration.class);
		SpringYarnProperties springYarnProperties = context.getBean(SpringYarnProperties.class);

		String applicationBaseDir = springYarnProperties.getApplicationBaseDir();
		Path path = new Path(applicationBaseDir);
		FileStatus[] listStatus;
		try {
			FileSystem fs = path.getFileSystem(yarnConfiguration);
			listStatus = new FileStatus[0];
			if (fs.exists(path)) {
				listStatus = fs.listStatus(path);
			}
			for (FileStatus status : listStatus) {
				apps.add(new CloudAppInfo(status.getPath().getName()));
			}
		} catch (Exception e) {
		}
		return apps;
	}

	public Collection<CloudAppInstanceInfo> getSubmittedApplications() {
		List<CloudAppInstanceInfo> appIds = new ArrayList<CloudAppInstanceInfo>();
		for (ApplicationReport report : yarnClient.listApplications("DATAFLOW")) {
			appIds.add(new CloudAppInstanceInfo(report.getApplicationId().toString(), report.getName(),
					report.getYarnApplicationState().toString(), report.getOriginalTrackingUrl()));
		}
		return appIds;
	}

	public void pushApplication(String applicationName) {
		yarnClient.installApplication(
				new ApplicationDescriptor(resolveApplicationdir(springYarnProperties, applicationName)));
	}

	public String submitApplication(String applicationName) {
		ApplicationId appId = null;
		appId = yarnClient.submitApplication(
				new ApplicationDescriptor(resolveApplicationdir(springYarnProperties, applicationName)));
		return appId != null ? appId.toString() : null;
	}

	public void killApplication(String applicationId) {
		yarnClient.killApplication(ConverterUtils.toApplicationId(applicationId));
	}

	public Collection<String> getClustersInfo(ApplicationId applicationId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);
		YarnContainerClusterEndpointResource response = operations.getClusters();
		return response.getClusters();
	}

	public List<ClustersInfoReportData> getClusterInfo(ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);
		ContainerClusterResource response = operations.clusterInfo(clusterId);
		List<ClustersInfoReportData> data = new ArrayList<ClustersInfoReportData>();

		Integer pany = response.getGridProjection().getProjectionData().getAny();
		Map<String, Integer> phosts = response.getGridProjection().getProjectionData().getHosts();
		Map<String, Integer> pracks = response.getGridProjection().getProjectionData().getRacks();
		Integer sany = response.getGridProjection().getSatisfyState().getAllocateData().getAny();
		Map<String, Integer> shosts = response.getGridProjection().getSatisfyState().getAllocateData().getHosts();
		Map<String, Integer> sracks = response.getGridProjection().getSatisfyState().getAllocateData().getRacks();

		data.add(new ClustersInfoReportData(response.getContainerClusterState().getClusterState().toString(),
				response.getGridProjection().getMembers().size(), pany, phosts, pracks, sany, shosts, sracks));
		return data;
	}

	public void createCluster(ApplicationId applicationId, String clusterId, String clusterDef, String projectionType,
			Integer projectionDataAny, Map<String, Integer> hosts, Map<String, Integer> racks,
			Map<String, Object> projectionDataProperties, Map<String, Object> extraProperties) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);

		ContainerClusterCreateRequest request = new ContainerClusterCreateRequest();
		request.setClusterId(clusterId);
		request.setClusterDef(clusterDef);
		request.setProjection(projectionType);
		request.setExtraProperties(extraProperties);

		ProjectionDataType projectionData = new ProjectionDataType();
		projectionData.setAny(projectionDataAny);
		projectionData.setHosts(hosts);
		projectionData.setRacks(racks);
		projectionData.setProperties(projectionDataProperties);

		request.setProjectionData(projectionData);
		operations.clusterCreate(request);
	}

	public void destroyCluster(ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);
		operations.clusterDestroy(clusterId);
	}

	public void startCluster(ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("start");
		operations.clusterStart(clusterId, request);
	}

	public void stopCluster(ApplicationId applicationId, String clusterId) {
		YarnContainerClusterOperations operations = buildClusterOperations(restTemplate, yarnClient, applicationId);
		ContainerClusterModifyRequest request = new ContainerClusterModifyRequest();
		request.setAction("stop");
		operations.clusterStart(clusterId, request);
	}

	private synchronized YarnContainerClusterOperations buildClusterOperations(RestTemplate restTemplate,
			YarnClient client, ApplicationId applicationId) {
		String key = applicationId.toString();
		YarnContainerClusterOperations operations = operationsCache.get(key);
		if (operations == null) {
			ApplicationReport report = client.getApplicationReport(applicationId);
			String trackingUrl = report.getOriginalTrackingUrl();
			operations = new YarnContainerClusterTemplate(trackingUrl + "/" + YarnContainerClusterEndpoint.ENDPOINT_ID,
					restTemplate);
			operationsCache.put(key, operations);
		}
		return operations;
	}

	private static String resolveApplicationdir(SpringYarnProperties springYarnProperties, String applicationName) {
		return springYarnProperties.getApplicationBaseDir().endsWith("/") ? springYarnProperties.getApplicationBaseDir()
				: (springYarnProperties.getApplicationBaseDir() + "/") + applicationName + "/";
	}

	@Configuration
	@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
			JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
			EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
	public static class Config {
	}

}
