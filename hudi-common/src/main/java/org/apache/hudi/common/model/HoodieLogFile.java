/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.exception.InvalidHoodiePathException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;

import static org.apache.hudi.common.fs.FSUtils.LOG_FILE_PATTERN;

/**
 * Abstracts a single log file. Contains methods to extract metadata like the fileId, version and extension from the log
 * file path.
 * <p>
 * Also contains logic to roll-over the log file
 */
public class HoodieLogFile implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final String DELTA_EXTENSION = ".log";
  public static final String LOG_FILE_PREFIX = ".";
  public static final Integer LOGFILE_BASE_VERSION = 1;

  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR = new LogFileComparator();
  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR_REVERSED = new LogFileComparator().reversed();

  private transient FileStatus fileStatus;
  private final String pathStr;
  private long fileLen;
  private transient Path path;
  private transient String fileId;
  private transient String baseTime;
  private transient int logVersion = Integer.MIN_VALUE;
  private transient String writeToken;

  public HoodieLogFile(HoodieLogFile logFile) {
    this.fileStatus = logFile.fileStatus;
    this.pathStr = logFile.pathStr;
    this.fileLen = logFile.fileLen;
    initLogFileBaseInfo();
  }

  public HoodieLogFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
    this.pathStr = fileStatus.getPath().toString();
    this.fileLen = fileStatus.getLen();
    initLogFileBaseInfo();
  }

  public HoodieLogFile(Path logPath) {
    this.fileStatus = null;
    this.pathStr = logPath.toString();
    this.fileLen = -1;
    initLogFileBaseInfo();
  }

  public HoodieLogFile(Path logPath, Long fileLen) {
    this.fileStatus = null;
    this.pathStr = logPath.toString();
    this.fileLen = fileLen;
    initLogFileBaseInfo();
  }

  public HoodieLogFile(String logPathStr) {
    this.fileStatus = null;
    this.pathStr = logPathStr;
    this.fileLen = -1;
    initLogFileBaseInfo();
  }

  public String getFileId() {
    return fileId;
  }

  public String getBaseCommitTime() {
    return this.baseTime;
  }

  public int getLogVersion() {
    return this.logVersion;
  }

  public String getLogWriteToken() {
    return this.writeToken;
  }

  public String getFileExtension() {
    return FSUtils.getFileExtensionFromLog(getPath());
  }

  public Path getPath() {
    if (path == null) {
      this.path = new Path(pathStr);
    }
    return this.path;
  }

  public String getFileName() {
    return getPath().getName();
  }

  public void setFileLen(long fileLen) {
    this.fileLen = fileLen;
  }

  public long getFileSize() {
    return fileLen;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public void setFileStatus(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  public HoodieLogFile rollOver(FileSystem fs, String logWriteToken) throws IOException {
    String fileId = getFileId();
    String baseCommitTime = getBaseCommitTime();
    Path path = getPath();
    String extension = "." + FSUtils.getFileExtensionFromLog(path);
    int newVersion = FSUtils.computeNextLogVersion(fs, path.getParent(), fileId, extension, baseCommitTime);
    return new HoodieLogFile(new Path(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, baseCommitTime, newVersion, logWriteToken)));
  }

  public static Comparator<HoodieLogFile> getLogFileComparator() {
    return LOG_FILE_COMPARATOR;
  }

  public static Comparator<HoodieLogFile> getReverseLogFileComparator() {
    return LOG_FILE_COMPARATOR_REVERSED;
  }

  private void initLogFileBaseInfo() {
    Matcher matcher = LOG_FILE_PATTERN.matcher(getFileName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(getPath(), "LogFile");
    }
    this.fileId = matcher.group(1);
    this.baseTime = matcher.group(2);
    this.logVersion = Integer.parseInt(matcher.group(4));
    this.writeToken = matcher.group(6);
  }

  /**
   * Comparator to order log-files.
   */
  public static class LogFileComparator implements Comparator<HoodieLogFile>, Serializable {

    private static final long serialVersionUID = 1L;
    private transient Comparator<String> writeTokenComparator;

    private Comparator<String> getWriteTokenComparator() {
      if (null == writeTokenComparator) {
        // writeTokenComparator is not serializable. Hence, lazy loading
        writeTokenComparator = Comparator.nullsFirst(Comparator.naturalOrder());
      }
      return writeTokenComparator;
    }

    @Override
    public int compare(HoodieLogFile o1, HoodieLogFile o2) {
      String baseInstantTime1 = o1.getBaseCommitTime();
      String baseInstantTime2 = o2.getBaseCommitTime();

      if (baseInstantTime1.equals(baseInstantTime2)) {

        if (o1.getLogVersion() == o2.getLogVersion()) {
          // Compare by write token when base-commit and log-version is same
          return getWriteTokenComparator().compare(o1.getLogWriteToken(), o2.getLogWriteToken());
        }

        // compare by log-version when base-commit is same
        return Integer.compare(o1.getLogVersion(), o2.getLogVersion());
      }

      // compare by base-commits
      return baseInstantTime1.compareTo(baseInstantTime2);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieLogFile that = (HoodieLogFile) o;
    return Objects.equals(pathStr, that.pathStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathStr);
  }

  @Override
  public String toString() {
    return "HoodieLogFile{pathStr='" + pathStr + '\'' + ", fileLen=" + fileLen + '}';
  }
}
