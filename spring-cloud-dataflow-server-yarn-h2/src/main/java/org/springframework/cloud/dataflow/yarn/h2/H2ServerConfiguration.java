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

/**
 * Spring configuration for H2 database server.
 *
 * @author Janne Valkealahti
 *
 */
package org.springframework.cloud.dataflow.yarn.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Configuration for H2 database server.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableConfigurationProperties(H2ConfigurationProperties.class)
public class H2ServerConfiguration {

	private static final Logger log = LoggerFactory.getLogger(H2ServerConfiguration.class);

	@Autowired
	private H2ConfigurationProperties properties;

	@Bean(destroyMethod = "stop")
	public Server dataflowH2Server() throws SQLException {
		log.info("Starting H2 Server with properties " + properties);
		ArrayList<String> args = new ArrayList<>();
		args.add("-tcp");
		args.add("-tcpAllowOthers");
		args.add("-tcpPort");
		args.add(Integer.toString(properties.getPort()));

		if (StringUtils.hasText(properties.getDirectory())) {
			args.add("-baseDir");
			args.add(properties.getDirectory());
		}
		Server server = Server.createTcpServer(args.toArray(new String[0])).start();
		initDatabaseViaConnection();
		return server;
	}

	private void initDatabaseViaConnection() throws SQLException {
		// create a connection which will kick off a db create, since this is
		// the only supported way for h2
		StringBuilder buf = new StringBuilder();
		buf.append("jdbc:h2:tcp://localhost:" + Integer.toString(properties.getPort()) + "/");
		if (!StringUtils.hasText(properties.getDirectory())) {
			buf.append("mem:");
		}
		buf.append(properties.getDatabase());
		buf.append(";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
		buf.append(";USER=" + properties.getUsername());
		if (StringUtils.hasText(properties.getPassword())) {
			buf.append(";PASSWORD=" + properties.getPassword());
		}
		Connection connection = DriverManager.getConnection(buf.toString());
		connection.close();
	}
}
