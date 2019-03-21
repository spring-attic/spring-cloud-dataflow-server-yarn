/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.dataflow.server.yarn.shell.core;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.table.AbsoluteWidthSizeConstraints;
import org.springframework.shell.table.BorderSpecification;
import org.springframework.shell.table.BorderStyle;
import org.springframework.shell.table.CellMatchers;
import org.springframework.shell.table.Formatter;
import org.springframework.shell.table.Table;
import org.springframework.shell.table.TableBuilder;
import org.springframework.shell.table.TableModel;
import org.springframework.stereotype.Component;

/**
 * Shell hdfs related commands.
 *
 * @author Janne Valkealahti
 *
 */
@Component
public class HadoopCommands implements CommandMarker {

	private static final String PREFIX = "hadoop fs ";
	private static final String FALSE = "false";
	private static final String TRUE = "true";
	private static final String FROM = "from";
	private static final String TO = "to";
	private static final String IGNORECRC = "ignoreCrc";
	private static final String CRC = "crc";
	private static final String DIR = "dir";
	private static final String SKIPTRASH = "skipTrash";
	private static final String RECURSIVE = "recursive";
	private static final String RECURSION_HELP = "whether with recursion";
	private static final String SOURCE_FILE_NAMES = "source file names";
	private static final String DESTINATION_PATH_NAME = "destination path name";
	private static final String PATH = "path";

	private FsShell shell;

	@Autowired
	public void setFsShell(FsShell shell) {
		this.shell = shell;
	}

	@CliCommand(value = PREFIX + "ls", help = "List files in the directory")
	public Table ls(
			@CliOption(key = { "", DIR }, mandatory = false, unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { RECURSIVE }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = RECURSION_HELP) final boolean recursive) {
		Collection<FileStatus> files = recursive ? shell.lsr(path) : shell.ls(path);
		return applySimpleListStyle(new TableBuilder(new FileStatusTableModel(files.toArray(new FileStatus[0])))).build();
	}

	@CliCommand(value = PREFIX + "rm", help = "Remove files in the HDFS")
	public void rm(
			@CliOption(key = { "", PATH }, mandatory = false, unspecifiedDefaultValue = ".", help = "path to be deleted") final String path,
			@CliOption(key = { SKIPTRASH }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = "whether to skip trash") final boolean skipTrash,
			@CliOption(key = { RECURSIVE }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = "whether to recurse") final boolean recursive) {
		shell.rm(recursive, skipTrash, path);
	}

	@CliCommand(value = PREFIX + "cat", help = "Copy source paths to stdout")
	public String cat(
			@CliOption(key = { "", PATH }, mandatory = true, unspecifiedDefaultValue = ".", help = "file name to be shown") final String path) {
		return shell.cat(path).toString();
	}

	@CliCommand(value = PREFIX + "copyFromLocal", help = "Copy single src, or multiple srcs from local file system to the destination file system.")
	public void copyFromLocal(
			@CliOption(key = { FROM }, mandatory = true, help = SOURCE_FILE_NAMES) final String source,
			@CliOption(key = { TO }, mandatory = true, help = DESTINATION_PATH_NAME) final String dest) {
		shell.copyFromLocal(source, dest);
	}

	@CliCommand(value = PREFIX + "copyToLocal", help = "Copy files to the local file system.")
	public void copyToLocal(
			@CliOption(key = { FROM }, mandatory = true, help = SOURCE_FILE_NAMES) final String source,
			@CliOption(key = { TO }, mandatory = true, help = DESTINATION_PATH_NAME) final String dest,
			@CliOption(key = { IGNORECRC }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { CRC }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = "whether copy CRC") final boolean crc) {
		shell.copyToLocal(ignoreCrc, crc, source, dest);
	}

	@CliCommand(value = PREFIX + "expunge", help = "Empty the trash")
	public void expunge() {
		shell.expunge();
	}

	@CliCommand(value = PREFIX + "mv", help = "Move source files to destination in the HDFS")
	public void mv(
			@CliOption(key = { FROM }, mandatory = true, help = SOURCE_FILE_NAMES) final String source,
			@CliOption(key = { TO }, mandatory = true, help = DESTINATION_PATH_NAME) final String dest) {
		shell.mv(source, dest);
	}

	@CliCommand(value = PREFIX + "mkdir", help = "Create a new directory")
	public void mkdir(
			@CliOption(key = { "", DIR }, mandatory = true, help = "directory name") final String dir) {
		shell.mkdir(dir);
	}

	private static TableBuilder applySimpleListStyle(TableBuilder builder) {
		builder
			.paintBorder(BorderStyle.air, BorderSpecification.INNER_VERTICAL)
				.fromTopLeft().toBottomRight()
			.on(CellMatchers.column(4))
				.addSizer(new AbsoluteWidthSizeConstraints(19))
				.addFormatter(new TableDateTimeFormatter());
		return builder;
	}

	private static class TableDateTimeFormatter implements Formatter {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		public String[] format(Object value) {
			String formatted = "";
			if (value instanceof Long) {
				formatted = sdf.format(new Date((Long)value));
			}
			return new String[] { formatted };
		}
	}

	private static class FileStatusTableModel extends TableModel {

		private final FileStatus[] statuses;

		public FileStatusTableModel(FileStatus[] statuses) {
			this.statuses = statuses;
		}

		@Override
		public int getRowCount() {
			return statuses.length;
		}

		@Override
		public int getColumnCount() {
			return 6;
		}

		@Override
		public Object getValue(int row, int column) {
			if (column == 0) {
				return statuses[row].getPermission().toString();
			}
			else if (column == 1) {
				return statuses[row].getOwner();
			}
			else if (column == 2) {
				return statuses[row].getGroup();
			}
			else if (column == 3) {
				return statuses[row].getLen();
			}
			else if (column == 4) {
				return statuses[row].getModificationTime();
			}
			else if (column == 5) {
				return Path.getPathWithoutSchemeAndAuthority(statuses[row].getPath());
			}
			else {
				return "";
			}
		}
	}
}
