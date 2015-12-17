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

package org.springframework.cloud.dataflow.yarn.streamappmaster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.listener.ContainerMonitorListener;

/**
 * Custom yarn appmaster tweaking container launch settings.
 *
 * @author Janne Valkealahti
 *
 */
public class DataFlowAppmaster extends ManagedContainerClusterAppmaster {

	private final static Log log = LogFactory.getLog(DataFlowAppmaster.class);

	@Override
	protected void onInit() throws Exception {
		super.onInit();

		// TODO: we want to have a proper support in base classes to gracefully
		//       shutdown appmaster when it has nothing to do. this trick
		//       here is solely a workaround not being able to access internal
		//       structures of base classes. this is pretty much all we can do
		//       from a subclass.
		//       potentially we want to make it configurable with a grace period, etc.
		getMonitor().addContainerMonitorStateListener(new ContainerMonitorListener() {

			@Override
			public void state(ContainerMonitorState state) {
				if (log.isDebugEnabled()) {
					log.info("Received monitor state " + state + " and container clusters size is " + getContainerClusters().size());
				}
				if (state.getRunning() == 0 && getContainerClusters().size() == 0) {
					// this state is valid at start but we know it's not gonna
					// get called until we have had at least one container running
					log.info("No running containers and no container clusters, initiate app shutdown");
					notifyCompleted();
				}
			}
		});
	}

	@Override
	protected List<String> onContainerLaunchCommands(Container container, ContainerCluster cluster,
			List<String> commands) {

		ArrayList<String> list = new ArrayList<String>(commands);
		Map<String, Object> extraProperties = cluster.getExtraProperties();

		log.info("onContainerLaunchCommands extraProperties=" + extraProperties);

		if (extraProperties != null) {
			if (extraProperties.containsKey("containerModules")) {
				String value = "containerModules=" + cluster.getExtraProperties().get("containerModules");
				list.add(Math.max(list.size() - 2, 0), value);
			}
			for (Entry<String, Object> entry : extraProperties.entrySet()) {
				if (entry.getKey().startsWith("containerArg")) {
					list.add(Math.max(list.size() - 2, 0), entry.getValue().toString());
				}
			}
		}
		return list;
	}

}
