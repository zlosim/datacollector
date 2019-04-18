/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Optional;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.lib.remote.FTPRemoteConnector;
import com.streamsets.pipeline.lib.remote.RemoteConnector;
import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.lib.remote.SFTPRemoteConnector;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import net.schmizz.sshj.sftp.SFTPException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileSystemException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import static com.streamsets.pipeline.stage.origin.lib.DataFormatParser.DATA_FORMAT_CONFIG_PREFIX;

public class RemoteDownloadSource extends BaseSource implements FileQueueChecker {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDownloadSource.class);
  private static final String CONF_PREFIX = "conf.";
  private static final String MINUS_ONE = "-1";

  static final String NOTHING_READ = "null";

  static final String REMOTE_URI = "remoteUri";
  static final String CONTENT_TYPE = "contentType";
  static final String CONTENT_ENCODING = "contentEncoding";


  private final RemoteDownloadConfigBean conf;
  private final File errorArchive;
  private final byte[] moveBuffer;

  private RemoteFile next = null;
  private ELEval rateLimitElEval;
  private ELVars rateLimitElVars;
  private String archiveDir;

  //By default true so, between pipeline restarts we can always trigger event.
  private boolean canTriggerNoMoreDataEvent = true;
  private long noMoreDataRecordCount = 0;
  private long noMoreDataErrorCount = 0;
  private long noMoreDataFileCount = 0;
  private long perFileRecordCount = 0;
  private long perFileErrorCount = 0;

  private final NavigableSet<RemoteFile> fileQueue = new TreeSet<>(new Comparator<RemoteFile>() {
    @Override
    public int compare(RemoteFile f1, RemoteFile f2) {
      if (f1.getLastModified() < f2.getLastModified()) {
        return -1;
      } else if (f1.getLastModified() > f2.getLastModified()) {
        return 1;
      } else {
        return f1.getFilePath().compareTo(f2.getFilePath());
      }
    }
  });

  private URI remoteURI;
  private volatile Offset currentOffset = null;
  private InputStream currentStream = null;
  private DataParser parser;
  private ErrorRecordHandler errorRecordHandler;

  private FileFilter fileFilter;
  private RemoteDownloadSourceDelegate delegate;

  public RemoteDownloadSource(RemoteDownloadConfigBean conf) {
    this.conf = conf;
    if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
      this.errorArchive = new File(conf.errorArchiveDir);
      this.moveBuffer = new byte[64 * 1024];
    } else {
      this.errorArchive = null;
      this.moveBuffer = null;
    }
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    conf.dataFormatConfig.checkForInvalidAvroSchemaLookupMode(
        conf.dataFormat,
        "conf.dataFormatConfig",
        getContext(),
        issues
    );

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.REMOTE.getLabel(),
        CONF_PREFIX + DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    this.remoteURI = RemoteConnector.getURI(conf.remoteConfig, issues, getContext(), Groups.REMOTE);

    if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
      if (conf.archiveDir == null || conf.archiveDir.isEmpty()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.POST_PROCESSING.name(),
                CONF_PREFIX + "archiveDir",
                Errors.REMOTE_DOWNLOAD_07
            )
        );
      } else {
        archiveDir = conf.archiveDir.endsWith("/") ? conf.archiveDir : conf.archiveDir + "/";
      }
    }

    validateFilePattern(issues);
    rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());
    rateLimitElVars = getContext().createELVars();

    if (issues.isEmpty()) {
      if (FTPRemoteConnector.SCHEME.equals(remoteURI.getScheme())) {
        delegate = new FTPRemoteDownloadSourceDelegate(conf);
        delegate.initAndConnect(issues, getContext(), remoteURI, archiveDir);
      } else if (SFTPRemoteConnector.SCHEME.equals(remoteURI.getScheme())) {
        delegate = new SFTPRemoteDownloadSourceDelegate(conf);
        delegate.initAndConnect(issues, getContext(), remoteURI, archiveDir);
      }
    }
    return issues;
  }

  private void validateFilePattern(List<ConfigIssue> issues) {
    if (conf.filePattern == null || conf.filePattern.trim().isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.REMOTE.getLabel(), CONF_PREFIX + "filePattern", Errors.REMOTE_DOWNLOAD_04, conf.filePattern));
    } else {
      try {
        fileFilter = new FileFilter(conf.filePatternMode, conf.filePattern);
      } catch (IllegalArgumentException ex) {
        issues.add(
            getContext().createConfigIssue(
                Groups.REMOTE.getLabel(),
                CONF_PREFIX + "filePattern",
                Errors.REMOTE_DOWNLOAD_05,
                conf.filePatternMode,
                conf.filePattern,
                ex.toString(),
                ex
            ));
      }
    }
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    final int batchSize = Math.min(maxBatchSize, conf.basic.maxBatchSize);
    // Just started up, currentOffset has not yet been set.
    // This method returns NOTHING_READ when only no events have ever been read
    if (currentOffset == null) {
      if(StringUtils.isEmpty(lastSourceOffset) || NOTHING_READ.equals(lastSourceOffset)) {
        LOG.debug("Detected invalid source offset '{}'", lastSourceOffset);

        // Use initial file
        if(!StringUtils.isEmpty(conf.initialFileToProcess)) {
          try {
            currentOffset = delegate.createOffset(conf.initialFileToProcess);
          } catch (IOException e) {
            throw new StageException(Errors.REMOTE_DOWNLOAD_06, conf.initialFileToProcess, e.toString(), e);
          }
        }

        // Otherwise start from beginning
      } else {
        // We have valid offset
        currentOffset = new Offset(lastSourceOffset);
      }
    }

    String offset = NOTHING_READ;
    try {
      Optional<RemoteFile> nextOpt = null;
      // Time to read the next file
      if (currentStream == null) {
        nextOpt = getNextFile();
        if (nextOpt.isPresent()) {
          next = nextOpt.get();
          noMoreDataFileCount++;
          // When starting up, reset to offset 0 of the file picked up for read only if:
          // -- we are starting up for the very first time, hence current offset is null
          // -- or the next file picked up for reads is not the same as the one we left off at (because we may have completed that one).
          if (currentOffset == null || !currentOffset.fileName.equals(next.getFilePath())) {
            perFileRecordCount = 0;
            perFileErrorCount = 0;

            LOG.debug("Sending New File Event. File: {}", next.getFilePath());
            RemoteDownloadSourceEvents.NEW_FILE.create(getContext()).with("filepath", next.getFilePath()).createAndSend();

            sendLineageEvent(next);

            currentOffset = delegate.createOffset(next.getFilePath());
          }
          if (conf.dataFormat == DataFormat.WHOLE_FILE) {
            Map<String, Object> metadata = new HashMap<>(7);
            long size = delegate.populateMetadata(next.getFilePath(), metadata);
            metadata.put(HeaderAttributeConstants.FILE, next.getFilePath());
            metadata.put(HeaderAttributeConstants.FILE_NAME, FilenameUtils.getName(next.getFilePath()));
            metadata.put(REMOTE_URI, remoteURI.toString());

            FileRef fileRef = new RemoteSourceFileRef.Builder()
                .bufferSize(conf.dataFormatConfig.wholeFileMaxObjectLen)
                .totalSizeInBytes(size)
                .rateLimit(FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval, rateLimitElVars, conf.dataFormatConfig.rateLimit))
                .remoteFile(next)
                .remoteUri(remoteURI)
                .createMetrics(true)
                .build();
            parser = conf.dataFormatConfig.getParserFactory().getParser(currentOffset.offsetStr, metadata, fileRef);
          } else {
            currentStream = next.createInputStream();
            LOG.info("Started reading file: {}", next.getFilePath());
            parser = conf.dataFormatConfig.getParserFactory().getParser(
                currentOffset.offsetStr, currentStream, currentOffset.getOffset());
          }
        } else {
          //Only if we saw data after last trigger/after a pipeline restart, we will trigger no more data event
          if (canTriggerNoMoreDataEvent) {
            LOG.debug(
                "Sending No More Data event. Files:{}.Records:{}, Errors:{}",
                noMoreDataFileCount,
                noMoreDataRecordCount,
                noMoreDataErrorCount
            );
            RemoteDownloadSourceEvents.NO_MORE_DATA.create(getContext())
                .with("record-count", noMoreDataRecordCount)
                .with("error-count", noMoreDataErrorCount)
                .with("file-count", noMoreDataFileCount)
                .createAndSend();
            noMoreDataErrorCount = 0;
            noMoreDataRecordCount = 0;
            noMoreDataFileCount = 0;
            canTriggerNoMoreDataEvent = false;
          }
          if (currentOffset == null) {
            return offset;
          } else {
            return currentOffset.offsetStr;
          }
        }
      }
      offset = addRecordsToBatch(batchSize, batchMaker);
    } catch (IOException | DataParserException ex) {
      // Don't retry reading this file since there can be no records produced.
      offset = MINUS_ONE;
      handleFatalException(ex, next);
    } finally {
      if (!NOTHING_READ.equals(offset) && currentOffset != null) {
        currentOffset.setOffset(offset);
      }
    }
    if (currentOffset != null) {
      return currentOffset.offsetStr;
    }
    return offset;
  }

  private String addRecordsToBatch(int maxBatchSize, BatchMaker batchMaker) throws IOException, StageException {
    String offset = NOTHING_READ;
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Record record = parser.parse();
        if (record != null) {
          record.getHeader().setAttribute(REMOTE_URI, remoteURI.toString());
          record.getHeader().setAttribute(HeaderAttributeConstants.FILE, next.getFilePath());
          record.getHeader().setAttribute(HeaderAttributeConstants.FILE_NAME,
              FilenameUtils.getName(next.getFilePath())
          );
          record.getHeader().setAttribute(
              HeaderAttributeConstants.LAST_MODIFIED_TIME,
              String.valueOf(next.getLastModified())
          );
          record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, offset == null ? "0" : offset);
          batchMaker.addRecord(record);
          perFileRecordCount++;
          noMoreDataRecordCount++;
          canTriggerNoMoreDataEvent = true;
          offset = parser.getOffset();
        } else {
          try {
            parser.close();
            if (currentStream != null) {
              currentStream.close();
            }
            LOG.debug(
                "Sending Finished File Event for {}.Records:{}, Errors:{}",
                next.getFilePath(),
                perFileRecordCount,
                perFileErrorCount
            );
            RemoteDownloadSourceEvents.FINISHED_FILE.create(getContext())
                .with("filepath", next.getFilePath())
                .with("record-count", perFileRecordCount)
                .with("error-count", perFileErrorCount)
                .createAndSend();
            handlePostProcessing(next.getFilePath());
          } finally {
            parser = null;
            currentStream = null;
            next = null;
          }
          //We will return -1 for finished files (It might happen where we are the last offset and another parse
          // returns null, in that case empty batch is emitted)
          offset = MINUS_ONE;
          break;
        }
      } catch (RecoverableDataParserException ex) {
        // Propagate partially parsed record to error stream
        Record record = ex.getUnparsedRecord();
        errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
        perFileErrorCount++;
        noMoreDataErrorCount++;
        //Even though we had an error in the data, we still saw some data
        canTriggerNoMoreDataEvent = true;
      } catch (ObjectLengthException ex) {
        errorRecordHandler.onError(Errors.REMOTE_DOWNLOAD_01, currentOffset.fileName, offset, ex);
        //Even though we couldn't process data from the file, we still saw some data
        canTriggerNoMoreDataEvent = true;
      }
    }
    return offset;
  }

  private void handlePostProcessing(String filePath) throws IOException {
    if (!getContext().isPreview()) {
      try {
        switch (conf.postProcessing) {
          case ARCHIVE:
            LOG.debug("Post Processing: Archiving file {}", filePath);
            String toPath = delegate.archive(filePath);
            LOG.info("Post Processing: Archived file {} to {}", filePath, toPath);
            break;
          case DELETE:
            LOG.debug("Post Processing: Deleting file {}", filePath);
            delegate.delete(filePath);
            LOG.info("Post Processing: Deleted file {}", filePath);
            break;
          case NONE:
            LOG.debug("Post Processing: None for file {}", filePath);
            break;
          default:
            break;
        }
      } catch (IOException ioe) {
        LOG.error("IOException during Post Processing: {}", ioe.getMessage(), ioe);
        throw ioe;
      }
    }
  }

  private void moveFileToError(RemoteFile fileToMove) {
    if (fileToMove == null) {
      LOG.warn("No file to move to error, since no file is currently in-process");
      return;
    }
    if (errorArchive != null) {
      int read;
      File errorFile = new File(errorArchive, fileToMove.getFilePath());
      if (errorFile.exists()) {
        errorFile = new File(errorArchive, fileToMove.getFilePath() + "-" + UUID.randomUUID().toString());
        LOG.info(fileToMove.getFilePath() + " is being written out as " + errorFile.getPath() +
            " as another file of the same name exists");
      }
      try (InputStream is = fileToMove.createInputStream();
           OutputStream os = new BufferedOutputStream(new FileOutputStream(errorFile))) {
        while ((read = is.read(moveBuffer)) != -1) {
          os.write(moveBuffer, 0, read);
        }
      } catch (Exception ex) {
        LOG.warn("Error while trying to write out error file to " + errorFile.getName());
      }
    }
  }

  private void handleFatalException(Exception ex, RemoteFile next) throws StageException {
    if (ex instanceof FileSystemException) {
      LOG.info("FileSystemException '{}'", ex.getMessage());
    }
    if (ex instanceof SFTPException) {
      LOG.info("SFTPException '{}'", ex.getMessage());
    }
    if (next != null) {
      LOG.error("Error while attempting to parse file: " + next.getFilePath(), ex);
    }
    if (ex instanceof FileNotFoundException) {
      LOG.warn("File: {} was found in listing, but is not downloadable", next != null ? next.getFilePath() : "(null)", ex);
    }
    if (ex instanceof ClosedByInterruptException || ex.getCause() instanceof ClosedByInterruptException) {
      //If the pipeline was stopped, we may get a ClosedByInterruptException while reading avro data.
      //This is because the thread is interrupted when the pipeline is stopped.
      //Instead of sending the file to error, publish batch and move one.
    } else {
      try {
        if (parser != null) {
          parser.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing parser", ioe);
      } finally {
        parser = null;
      }
      try {
        if (currentStream != null) {
          currentStream.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing stream", ioe);
      } finally {
        currentStream = null;
      }
      String exOffset;
      if (ex instanceof OverrunException) {
        exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
      } else {
        try {
          exOffset = (parser != null) ? parser.getOffset() : NOTHING_READ;
        } catch (IOException ex1) {
          exOffset = NOTHING_READ;
        }
      }
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          // we failed to produce a record, which leaves the input file in an unknown state.
          moveFileToError(next);
          break;
        case STOP_PIPELINE:
          if (currentOffset != null) {
            throw new StageException(Errors.REMOTE_DOWNLOAD_02, currentOffset.fileName, exOffset, ex);
          } else {
            throw new StageException(Errors.REMOTE_DOWNLOAD_03, ex);
          }
        default:
          throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
              getContext().getOnErrorRecord(), ex));
      }
    }
  }

  private Optional<RemoteFile> getNextFile() throws IOException, StageException {
    if (fileQueue.isEmpty()) {
      queueFiles();
    }
    return Optional.fromNullable(fileQueue.pollFirst());
  }

  private void queueFiles() throws IOException, StageException {
    delegate.queueFiles(this, fileQueue, fileFilter);
  }

  @Override
  public boolean shouldQueue(RemoteFile remoteFile) {
    // Case: We started up for the first time, so anything we see must be queued
    if (currentOffset == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Initial file: {}", remoteFile.getFilePath());
      }
      return true;
    }
    // We poll for new files only when fileQueue is empty, so we don't need to check if this file is in the queue.
    // The file can be in the fileQueue only if the file was already queued in this iteration -
    // which is not possible, since we are iterating through the children,
    // so this is the first time we are seeing the file.

    // Case: It is the same file as we were reading, but we have not read the whole thing, so queue it again
    // - recovering from a shutdown.
    if ((remoteFile.getFilePath().equals(currentOffset.fileName))
        && !(currentOffset.getOffset().equals(MINUS_ONE))) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Offset not complete: {}. Re-queueing.", remoteFile.getFilePath());
      }
      return true;
    }

    // Case: The file is newer than the last one we read/are reading, and its not the same last one
    if ((remoteFile.getLastModified() > currentOffset.timestamp)
        && !(remoteFile.getFilePath().equals(currentOffset.fileName))) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Updated file: {}", remoteFile.getFilePath());
      }
      return true;
    }

    // Case: The file has the same timestamp as the last one we read, but is lexicographically higher,
    // and we have not queued it before.
    if ((remoteFile.getLastModified() == currentOffset.timestamp)
        && (remoteFile.getFilePath().compareTo(currentOffset.fileName) > 0)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Same timestamp as currentOffset, lexicographically higher file: {}", remoteFile.getFilePath());
      }
      return true;
    }

    // For all other things .. we don't add.
    return false;
  }

  @Override
  public void destroy() {
    LOG.info(Utils.format("Destroying {}", getInfo().getInstanceName()));
    try {
      IOUtils.closeQuietly(currentStream);
      IOUtils.closeQuietly(parser);
      if (delegate != null) {
        delegate.close();
      }
    } catch (IOException ex) {
      LOG.warn("Error during destroy", ex);
    } finally {
      delegate = null;
      //This forces the use of same RemoteDownloadSource object
      //not to have dangling reference to old stream (which is closed)
      //Also forces to initialize the next in produce call.
      currentStream = null;
      parser = null;
      currentOffset = null;
      next = null;
      fileFilter = null;
    }
  }

  private void sendLineageEvent(RemoteFile next) {
    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, next.getFilePath());
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.FTP.name());
    event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.filePattern);
    Map<String, String> props = new HashMap<>();
    props.put("Resource URL", conf.remoteConfig.remoteAddress);
    event.setProperties(props);
    getContext().publishLineageEvent(event);
  }

}
