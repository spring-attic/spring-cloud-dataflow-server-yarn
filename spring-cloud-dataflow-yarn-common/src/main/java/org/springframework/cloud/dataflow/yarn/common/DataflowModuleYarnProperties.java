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

package org.springframework.cloud.dataflow.yarn.common;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Shared boot configuration properties for "dataflow.module".
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "dataflow.module")
public class DataflowModuleYarnProperties {

	private String coordinates;
	private Map<String, String> parameters;

	/**
	 * Gets the module maven coordinates.
	 *
	 * @return the coordinates
	 */
	public String getCoordinates() {
		return coordinates;
	}

	/**
	 * Sets the module maven coordinates.
	 *
	 * @param coordinates the new coordinates
	 */
	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	/**
	 * Gets the module parameters.
	 *
	 * @return the parameters
	 */
	public Map<String, String> getParameters() {
		return parameters;
	}

	/**
	 * Sets the module parameters.
	 *
	 * @param parameters the parameters
	 */
	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	@Override
	public String toString() {
		return "DataflowModuleYarnProperties [coordinates=" + coordinates + ", parameters=" + parameters + "]";
	}

}
