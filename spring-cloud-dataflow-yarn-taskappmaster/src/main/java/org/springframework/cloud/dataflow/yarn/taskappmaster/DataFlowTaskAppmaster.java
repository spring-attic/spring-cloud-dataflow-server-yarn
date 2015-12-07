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

package org.springframework.cloud.dataflow.yarn.taskappmaster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.dataflow.yarn.common.DataflowModuleYarnProperties;
import org.springframework.util.Assert;
import org.springframework.yarn.am.StaticEventingAppmaster;

public class DataFlowTaskAppmaster extends StaticEventingAppmaster {

	@Autowired
	private DataflowModuleYarnProperties dataflowModuleYarnProperties;

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		Assert.hasText(dataflowModuleYarnProperties.getCoordinates(), "Module coordinates must be set");
	}

	@Override
	public List<String> getCommands() {
		List<String> list = new ArrayList<String>(super.getCommands());
		list.add(Math.max(list.size() - 2, 0), "--dataflow.module.coordinates=" + dataflowModuleYarnProperties.getCoordinates());
		if (dataflowModuleYarnProperties.getParameters() != null) {
			for (Entry<String, String> entry : dataflowModuleYarnProperties.getParameters().entrySet()) {
				list.add(Math.max(list.size() - 2, 0), "--dataflow.module.parameters." + entry.getKey() + "=" + entry.getValue());
			}
		}
		return list;
	}

}
