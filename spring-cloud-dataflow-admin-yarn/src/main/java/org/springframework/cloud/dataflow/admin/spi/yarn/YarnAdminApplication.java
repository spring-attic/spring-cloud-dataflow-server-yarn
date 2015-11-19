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

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.oauth2.OAuth2AutoConfiguration;
import org.springframework.cloud.dataflow.admin.AdminApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.yarn.boot.YarnAppmasterAutoConfiguration;
import org.springframework.yarn.boot.YarnContainerAutoConfiguration;

/**
 * Bootstrap class for Spring Cloud Data Flow Admin on Apache Hadoop YARN.
 *
 * @author Mark Fisher
 * @author Janne Valkealahti
 */
@ComponentScan(basePackageClasses = AdminApplication.class)
@EnableAutoConfiguration(exclude = { YarnAppmasterAutoConfiguration.class, YarnContainerAutoConfiguration.class,
		OAuth2AutoConfiguration.class })
public class YarnAdminApplication {

	public static void main(String[] args) {
		AdminApplication.main(args);
	}
}
