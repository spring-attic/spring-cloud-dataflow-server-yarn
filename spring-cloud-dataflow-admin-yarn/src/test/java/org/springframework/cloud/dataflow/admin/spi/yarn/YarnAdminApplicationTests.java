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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnStreamModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.yarn.YarnTaskModuleDeployer;
import org.springframework.context.ConfigurableApplicationContext;

public class YarnAdminApplicationTests {

	@Test
	public void testSimpleBootstrap() {
		SpringApplication app = new SpringApplication(YarnAdminApplication.class);
		ConfigurableApplicationContext context = app.run(new String[] { "--spring.cloud.bootstrap.name=admin",
				"--server.port=0", "--spring.cloud.dataflow.yarn.version=fake" });
		assertThat(context.containsBean("processModuleDeployer"), is(true));
		assertThat(context.getBean("processModuleDeployer"), instanceOf(YarnStreamModuleDeployer.class));
		assertThat(context.containsBean("taskModuleDeployer"), is(true));
		assertThat(context.getBean("taskModuleDeployer"), instanceOf(YarnTaskModuleDeployer.class));
		context.close();
	}

}
