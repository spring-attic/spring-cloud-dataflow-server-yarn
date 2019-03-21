/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.yarn.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.EndpointMBeanExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.deployer.spi.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationException;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.boot.app.AbstractClientApplication;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;

/**
 * Generic Spring Boot client application used to push Spring Yarn Boot based apps into hdfs.
 * <p>
 * Pushed application bundle is merely a collection of files inside a directory. All files
 * in this directory is considered to belong to the bundle and directory should not have any
 * other files or nested directories.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableAutoConfiguration(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class,
		JmxAutoConfiguration.class, BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		EndpointMBeanExportAutoConfiguration.class, EndpointAutoConfiguration.class })
public class YarnPushApplication extends AbstractClientApplication<String, YarnPushApplication> {

	private Map<String, Properties> configFilesContents = new HashMap<String, Properties>();
	private CloudAppType cloudAppType;

	/**
	 * Associates a new {@link CloudAppType} into this application.
	 * 
	 * @param cloudAppType the cloud app type
	 * @return the {@link YarnPushApplication} for chaining
	 */
	public YarnPushApplication cloudAppType(CloudAppType cloudAppType) {
		Assert.notNull(cloudAppType, "Cloud app type must be set");
		this.cloudAppType = cloudAppType;
		return this;
	}

	/**
	 * Associates a new {@link Properties} with a name. These properties will
	 * be serialised into a common properties format with a given config
	 * file name.
	 *
	 * @param configFileName the config file name
	 * @param configProperties the config properties
	 * @return the {@link YarnPushApplication} for chaining
	 */
	public YarnPushApplication configFile(String configFileName, Properties configProperties) {
		configFilesContents.put(configFileName, configProperties);
		return this;
	}

	/**
	 * Run a {@link SpringApplication} build by a
	 * {@link SpringApplicationBuilder} using an empty args.
	 *
	 * @see #run(String...)
	 */
	public String run() {
		return run(new String[0]);
	}

	/**
	 * Run a {@link SpringApplication} build by a {@link SpringApplicationBuilder}.
	 *
	 * @param args the Spring Application args
	 */
	public String run(String... args) {
		if (!StringUtils.hasText(applicationVersion)) {
			throw new SpringApplicationException("Error executing a spring application", new IllegalArgumentException(
					"Instance id must be set"));
		}
		if (cloudAppType == null) {
			throw new SpringApplicationException("Error executing a spring application", new IllegalArgumentException(
					"cloudAppType must be set"));
		}
		
		SpringApplicationBuilder builder = new SpringApplicationBuilder();
		builder.web(false);
		builder.sources(YarnPushApplication.class);
		SpringYarnBootUtils.addSources(builder, sources.toArray(new Object[0]));
		SpringYarnBootUtils.addProfiles(builder, profiles.toArray(new String[0]));
		SpringYarnBootUtils.addConfigFilesContents(builder, configFilesContents);
		if (StringUtils.hasText(applicationBaseDir)) {
			appProperties.setProperty("spring.yarn.applicationDir", applicationBaseDir + applicationVersion + "/");
		}
		appProperties.setProperty("spring.yarn.applicationVersion", applicationVersion);

		SpringYarnBootUtils.addApplicationListener(builder, appProperties);

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<String>() {

			@Override
			public String runWithSpringApplication(ApplicationContext context) throws Exception {
				String deployerVersion = context.getEnvironment().getRequiredProperty("spring.cloud.deployer.yarn.version");
				org.apache.hadoop.conf.Configuration configuration = context.getBean(org.apache.hadoop.conf.Configuration.class);
				DefaultYarnCloudAppService defaultYarnCloudAppService = new DefaultYarnCloudAppService(deployerVersion);
				defaultYarnCloudAppService.setConfiguration(configuration);
				defaultYarnCloudAppService.pushApplication(applicationVersion, cloudAppType);
				return null;
			}

		}, args);
	}

	@Override
	protected YarnPushApplication getThis() {
		return this;
	}

}
