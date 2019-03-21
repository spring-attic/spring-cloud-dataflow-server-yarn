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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Application bootstrapping H2 database server.
 *
 * With a bundled startup script this application is run:
 * $ dataflow-server-yarn-h2 \
 *   --dataflow.database.h2.directory=/path/to/db \
 *   --dataflow.database.h2.username=myuser \
 *   --dataflow.database.h2.password=mypw \
 *   --dataflow.database.h2.port=19092 \
 *   --dataflow.database.h2.database=mydb
 *
 * Will work both persist files and in-memory if directory is set.
 * If password is changed for existing persist db files, database
 * will fail to start.
 *
 * @author Janne Valkealahti
 *
 */
@SpringBootApplication
public class H2ServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(H2ServerApplication.class, args);
	}
}
