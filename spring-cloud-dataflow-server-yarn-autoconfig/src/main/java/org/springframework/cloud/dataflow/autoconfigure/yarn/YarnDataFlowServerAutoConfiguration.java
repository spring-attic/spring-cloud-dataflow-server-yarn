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

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.deployer.resource.maven.MavenResourceLoader;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.hateoas.core.DefaultRelProvider;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for dataflow
 * YARN server.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnClass({ AppDeployer.class, TaskLauncher.class })
@ConditionalOnProperty(prefix = "dataflow.server.yarn", name = "enabled", havingValue = "true", matchIfMissing = true)
public class YarnDataFlowServerAutoConfiguration {

	private static final String REL_PROVIDER_BEAN_NAME = "defaultRelProvider";
	@Autowired(required = false)
	private MavenResourceLoader mavenResourceLoader;

	@Bean
	public DelegatingResourceLoader delegatingResourceLoader(org.apache.hadoop.conf.Configuration configuration) {
		DefaultResourceLoader defaultLoader = new DefaultResourceLoader();
		Map<String, ResourceLoader> loaders = new HashMap<>();
		loaders.put("hdfs", new HdfsResourceLoader(configuration));
		if (mavenResourceLoader != null) {
			loaders.put("maven", mavenResourceLoader);
		}
		loaders.put("file", defaultLoader);
		loaders.put("http", defaultLoader);
		return new DelegatingResourceLoader(loaders);
	}

	// workaround for github.com/spring-cloud/spring-cloud-dataflow/issues/575
	// remove hateos from pom after this is fix can be removed
	@Bean
	public BeanPostProcessor relProviderOverridingBeanPostProcessor() {
		return new BeanPostProcessor() {
			@Override
			public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
				// Override the RelProvider to DefaultRelProvider
				// Since DataFlow UI expects DefaultRelProvider to be used, override any other instance of
				// DefaultRelProvider (EvoInflectorRelProvider for instance) with the DefaultRelProvider.
				if (beanName != null && beanName.equals(REL_PROVIDER_BEAN_NAME)) {
					return new DefaultRelProvider();
				}
				return bean;
			}

			@Override
			public Object postProcessAfterInitialization(Object bean, String s) throws BeansException {
				return bean;
			}
		};
	}
}
