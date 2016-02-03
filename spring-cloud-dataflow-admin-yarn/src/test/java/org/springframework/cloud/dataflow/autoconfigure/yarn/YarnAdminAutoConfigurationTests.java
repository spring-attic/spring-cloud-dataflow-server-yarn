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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Auto-config tests for {@link YarnAdminAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnAdminAutoConfigurationTests {

	private AnnotationConfigApplicationContext context;

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
		context = null;
	}

	@Test
	public void testDefaults() {
		context = new AnnotationConfigApplicationContext();
		context.register(YarnAdminAutoConfiguration.class);
		context.refresh();

		assertThat(context.containsBean("processModuleDeployer"), is(true));
		assertThat(context.containsBean("taskModuleDeployer"), is(true));
	}

	@Test
	public void testEnabledByProperties() {
		context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils
			.addEnvironment(this.context,
				"dataflow.server.yarn.enabled:true");
		context.register(YarnAdminAutoConfiguration.class);
		context.refresh();

		assertThat(context.containsBean("processModuleDeployer"), is(true));
		assertThat(context.containsBean("taskModuleDeployer"), is(true));
	}

	@Test
	public void testDisabledByProperties() {
		context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils
			.addEnvironment(this.context,
				"dataflow.server.yarn.enabled:false");
		context.register(YarnAdminAutoConfiguration.class);
		context.refresh();

		assertThat(context.containsBean("processModuleDeployer"), is(false));
		assertThat(context.containsBean("taskModuleDeployer"), is(false));
	}

}
