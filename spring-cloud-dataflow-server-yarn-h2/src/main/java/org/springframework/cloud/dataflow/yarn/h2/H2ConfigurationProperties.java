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

package org.springframework.cloud.dataflow.yarn.h2;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for H2 database server. Database port
 * defaults to '19092'. Default username is 'sa' with no password.
 * Default database name is 'dataflow'. On default in-memory
 * database is defined which will be persisted if 'directory' is
 * defined.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(prefix = "dataflow.database.h2")
public class H2ConfigurationProperties {

	private int port = 19092;
	private String username = "sa";
	private String password;
	private String database = "dataflow";
	private String directory;

	/**
	 * Gets the port.
	 *
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the port.
	 *
	 * @param port the new port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Gets the username.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the username.
	 *
	 * @param username the new username
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password.
	 *
	 * @param password the new password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Gets the database name.
	 *
	 * @return the database name
	 */
	public String getDatabase() {
		return database;
	}

	/**
	 * Sets the database name.
	 *
	 * @param database the new database name
	 */
	public void setDatabase(String database) {
		this.database = database;
	}

	/**
	 * Gets the data directory.
	 *
	 * @return the data directory
	 */
	public String getDirectory() {
		return directory;
	}

	/**
	 * Sets the data directory
	 *
	 * @param directory the new data directory
	 */
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	@Override
	public String toString() {
		return "H2ConfigurationProperties [port=" + port + ", username=" + username + ", password="
				+ (password == null ? "" : password.replaceAll(".", "*")) + ", database=" + database + ", directory=" + directory + "]";
	}
}
