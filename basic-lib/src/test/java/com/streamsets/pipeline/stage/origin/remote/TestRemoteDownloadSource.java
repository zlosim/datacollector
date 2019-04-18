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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.io.fileref.FileRefTestUtil;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.remote.Authentication;
import com.streamsets.pipeline.lib.remote.FTPAndSSHDUnitTest;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.PropertyResolverUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.lang.Thread.currentThread;
import static org.awaitility.Awaitility.await;

@RunWith(Parameterized.class)
public class TestRemoteDownloadSource extends FTPAndSSHDUnitTest {

  private final Random RANDOM = new Random();

  private enum Scheme {
    sftp, ftp
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] data() {
    return Scheme.values();
  }

  private Scheme scheme;

  public TestRemoteDownloadSource(Scheme scheme) {
    this.scheme = scheme;
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Test
  public void testNoError() throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testRespectsConfiguredBatchSize() throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*",
            1
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    for (int j = 0; j < 2; j++) {
      StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
      List<Record> expected = getExpectedRecords();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(j).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testInvalidFilePatternGlob() throws Exception {
    testInvalidFilePattern(FilePatternMode.GLOB);
  }

  @Test
  public void testInvalidFilePatternRegex() throws Exception {
    testInvalidFilePattern(FilePatternMode.REGEX);
  }

  private void testInvalidFilePattern(FilePatternMode filePatternMode) throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);
    RemoteDownloadSource origin = new RemoteDownloadSource(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.JSON,
        null,
        false,
        filePatternMode,
        "", // empty pattern
        1000,
        ""
    ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin).addOutputLane("lane").build();
    runInitWithConfigException(runner, Errors.REMOTE_DOWNLOAD_04);
    destroyAndValidate(runner);

    origin = new RemoteDownloadSource(getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.JSON,
        null,
        false,
        filePatternMode,
        "[", // invalid pattern
        1000,
        ""
    ));
    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin).addOutputLane("lane").build();
    runInitWithConfigException(runner, Errors.REMOTE_DOWNLOAD_05);
    destroyAndValidate(runner);
  }

  @Test
  public void testNoErrorOrdering() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir = new File(currentThread().getContextClassLoader().getResource(path).getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords(true);
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testInitialFile() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir = new File(currentThread().getContextClassLoader().getResource(path).getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(16000000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            FilePatternMode.GLOB,
            "*",
            1000,
            "sloth.txt"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 2; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testInitialFileDoesntExists() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    setupServer(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            FilePatternMode.GLOB,
            "*",
            1000,
            "is-arvind-son-of-god.txt"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      runner.runProduce(null, 1000);
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      Assert.assertEquals(Errors.REMOTE_DOWNLOAD_06, e.getErrorCode());
      if (scheme == Scheme.sftp) {
        Assert.assertTrue(e.getCause() instanceof SFTPException);
        SFTPException cause = (SFTPException) e.getCause();
        Assert.assertEquals(Response.StatusCode.NO_SUCH_FILE, cause.getStatusCode());
        Assert.assertEquals("No such file or directory", cause.getMessage());
      } else if (scheme == Scheme.ftp){
        Assert.assertTrue(e.getCause() instanceof FileSystemException);
        FileSystemException cause = (FileSystemException) e.getCause();
        Assert.assertEquals("vfs.provider/get-last-modified-no-exist.error", cause.getCode());
      }
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testRestartFromMiddleOfFile() throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);

    // Create a new source.
    origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    op = runner.runProduce(offset, 1);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testRestartCompletedFile() throws Exception {
    path = "remote-download-source/parseSameTimestamp";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseSameTimestamp").getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords(true);
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1);
    offset = op.getNewOffset();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);

    // Create a new source.
    origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();
    // Since we don't proactively close steams, we must hit at least one null event in a batch to close the current
    // stream and open the next one, else the next batch will be empty and the data comes in the batch following that.
    op =runner.runProduce(offset, 1); // Forces /sloth.txt file parse to return -1
    op = runner.runProduce(op.getNewOffset(), 1);
    offset = op.getNewOffset();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(1).get(), actual.get(0).get());

    op =runner.runProduce(offset, 1); //Forces /panda.txt file parse to return -1
    offset = op.getNewOffset();
    op = runner.runProduce(offset, 2);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(2).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunErrorArchiveFile() throws Exception {
    path = "remote-download-source/parseOverrun";
    setupServer(path, false);
    File archiveDir = testFolder.newFolder();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            archiveDir.toString(),
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords(true);
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    Assert.assertEquals(1, archiveDir.listFiles().length);
    File expectedFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseOverrun").getPath()).listFiles()[0];
    File actualFile = archiveDir.listFiles()[0];
    Assert.assertEquals(expectedFile.getName(), actualFile.getName());
    Assert.assertTrue(FileUtils.contentEquals(expectedFile, actualFile));
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunErrorArchiveFileRecovery() throws Exception {
    path = "remote-download-source/parseRecoveryFromFailure";
    File dir =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseRecoveryFromFailure").getPath());
    File[] files = dir.listFiles();
    for (File f : files) {
      if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("longobject.txt")) {
        Assert.assertTrue(f.setLastModified(17500000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(path, false);
    File archiveDir = testFolder.newFolder();
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("longobject.txt");
      }
    };
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            archiveDir.toString(),
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords(true);
    Assert.assertEquals(0, archiveDir.listFiles().length);
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      if (i >= 1) { //longobject
        Assert.assertEquals(1, archiveDir.listFiles().length);
        continue;
      } else {
        Assert.assertEquals(0, archiveDir.listFiles().length);
      }
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    Assert.assertEquals(1, archiveDir.listFiles().length);
    File expectedFile =
        new File(currentThread().getContextClassLoader().
            getResource("remote-download-source/parseRecoveryFromFailure").getPath()).listFiles(filter)[0];

    File actualFile = archiveDir.listFiles(filter)[0];
    Assert.assertEquals(expectedFile.getName(), actualFile.getName());
    Assert.assertTrue(FileUtils.contentEquals(expectedFile, actualFile));
    destroyAndValidate(runner);
  }

  @Test
  public void testOverrunError() throws Exception {
    path = "remote-download-source/parseOverrun";
    setupServer(path, false);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    destroyAndValidate(runner);
  }

  @Test
  public void testParseError() throws Exception {
    path = "remote-download-source/parseError";
    File dir = new File(currentThread().getContextClassLoader().getResource(path).getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(2, files.length);
    for (File f : files) {
      if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(15000000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = new ArrayList<>();
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("polarbear"));
    record.set("/age", Field.create("6"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("huge"),
        Field.create("round"),
        Field.create("playful")
    )));
    expected.add(record);
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(expected.get(0).get(), actual.get(0).get());
    String offset = op.getNewOffset();
    op = runner.runProduce(offset, 1000);
    Assert.assertEquals("/sloth.txt::17000000000::-1", op.getNewOffset());
    actual = op.getRecords().get("lane");
    Assert.assertEquals(0, actual.size());

    op = runner.runProduce(offset, 1000);
    Assert.assertEquals("/sloth.txt::17000000000::-1", op.getNewOffset());
    actual = op.getRecords().get("lane");
    Assert.assertEquals(0, actual.size());

    destroyAndValidate(runner);
  }

  private List<Record> getExpectedRecords() {
    return getExpectedRecords(false);
  }

  private List<Record> getExpectedRecords(boolean withPolarBear) {
    List<Record> records = new ArrayList<>(2);
    Record record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("sloth"));
    record.set("/age", Field.create("5"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cute"),
        Field.create("slooooow"),
        Field.create("sloooooow"),
        Field.create("sloooooooow")
    )));
    records.add(record);

    record = RecordCreator.create();
    record.set(Field.create(new HashMap<String, Field>()));
    record.set("/name", Field.create("panda"));
    record.set("/age", Field.create("3"));
    record.set("/characterisitics", Field.create(Arrays.asList(
        Field.create("cool"),
        Field.create("cute"),
        Field.create("round"),
        Field.create("playful"),
        Field.create("hungry")
    )));
    records.add(record);
    if (withPolarBear) {
      record = RecordCreator.create();
      record.set(Field.create(new HashMap<String, Field>()));
      record.set("/name", Field.create("polarbear"));
      record.set("/age", Field.create("6"));
      record.set("/characterisitics", Field.create(Arrays.asList(
          Field.create("cool"),
          Field.create("cute"),
          Field.create("huge"),
          Field.create("round"),
          Field.create("playful")
      )));
      records.add(record);
    }
    return records;
  }

  @Test
  public void testPicksUpNewFiles() throws Exception {
    String originPath =
        currentThread().getContextClassLoader().getResource("remote-download-source/parseNoError").getPath();
    File originDirFile = new File(originPath).listFiles()[0];
    File tempDir = testFolder.newFolder();
    File copied = new File(tempDir, originDirFile.getName());
    Files.copy(originDirFile, copied);
    long lastModified = copied.lastModified();
    setupServer(tempDir.toString(), true);
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }

    File eventualFile = new File(tempDir, "z" + originDirFile.getName());
    Files.copy(originDirFile, eventualFile);
    eventualFile.setLastModified(lastModified);
    op = runner.runProduce(offset, 1000);
    expected = getExpectedRecords();
    actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testWholeFile() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testWholeFile";
    Path filePath = Paths.get(path + "/testWholeFile.txt");
    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        "This is sample text".getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.WHOLE_FILE,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();

      StageRunner.Output op = runner.runProduce("null", 1000);

      List<Record> actual = op.getRecords().get("lane");

      Assert.assertEquals(1, actual.size());
      Record record = actual.get(0);

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH));
      Assert.assertTrue(record.has(FileRefUtil.FILE_REF_FIELD_PATH));

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.SIZE));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.LAST_MODIFIED_TIME));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.CONTENT_TYPE));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.CONTENT_ENCODING));

      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.REMOTE_URI));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE_NAME));
      Assert.assertTrue(record.has(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE));

      Assert.assertEquals("testWholeFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("/testWholeFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE));

      Assert.assertEquals("testWholeFile.txt", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE_NAME).getValueAsString());
      Assert.assertEquals("/testWholeFile.txt", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + HeaderAttributeConstants.FILE).getValueAsString());
      Assert.assertEquals(scheme.name() + "://localhost:" + port + "/", record.get(FileRefUtil.FILE_INFO_FIELD_PATH + "/" + RemoteDownloadSource.REMOTE_URI).getValueAsString());

      InputStream is1 = new FileInputStream(filePath.toFile());

      InputStream is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      List<Record> records = runner.runProduce(op.getNewOffset(), 1000).getRecords().get("lane");
      Assert.assertEquals(0, records.size());
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testWholeFileMultipleInputStreams() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testWholeFileMultipleInputStreams";
    Assert.assertTrue(new File(path).mkdirs());
    Path filePath = Paths.get(path + "/testWholeFileMultipleInputStreams.txt");

    java.nio.file.Files.write(
        Paths.get(path + "/testWholeFileMultipleInputStreams.txt"),
        "This is sample text".getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.WHOLE_FILE,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();

      StageRunner.Output op = runner.runProduce("null", 1000);

      List<Record> actual = op.getRecords().get("lane");

      Assert.assertEquals(1, actual.size());
      Record record = actual.get(0);

      InputStream is1 = new FileInputStream(filePath.toFile());

      InputStream is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      //create the input streams again

      is1 = new FileInputStream(filePath.toFile());

      is2 = record.get(FileRefUtil.FILE_REF_FIELD_PATH).getValueAsFileRef().createInputStream(runner.getContext(), InputStream.class);

      FileRefTestUtil.checkFileContent(is1, is2);

      List<Record> records = runner.runProduce(op.getNewOffset(), 1000).getRecords().get("lane");
      Assert.assertEquals(0, records.size());
    } finally {
      destroyAndValidate(runner);
    }
  }


  private Pair<String, List<Record>> runSourceAndReturnOffsetAndRecords(
      String lastOffset,
      int batchSize
  ) throws Exception {
    RemoteDownloadConfigBean configBean = getBean(
        scheme.name() + "://localhost:" + port + "/",
        true,
        DataFormat.TEXT,
        null,
        //Process subdirectories
        false,
        "*.zip"
    );
    configBean.dataFormatConfig.compression = Compression.ARCHIVE;
    configBean.dataFormatConfig.filePatternInArchive = "testReadArchive/*.txt";
    RemoteDownloadSource origin =
        new RemoteDownloadSource(configBean);
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      StageRunner.Output op = runner.runProduce(lastOffset, batchSize);
      List<Record> records = op.getRecords().get("lane");
      return Pair.of(op.getNewOffset(), records);
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testReadArchive() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testReadArchive";
    Assert.assertTrue(new File(path).mkdirs());
    String pathPrefixInsideTheArchive = "testReadArchive";

    int numberOfFilesInArchive = 5;
    int numberOfRecordsInAFile = 10;

    List<String> internalArchiveFiles = new ArrayList<>();
    //Zip archive file
    //Writing 5 different files each with 10 records.
    Path archiveFilePath = Paths.get(path + "/testReadArchive.zip");
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archiveFilePath.toFile()))) {
      for (int i = 0; i < numberOfFilesInArchive; i++) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < numberOfRecordsInAFile; j++) {
          if (j != 0) {
            sb.append("\n");
          }
          sb.append(j);
        }
        String entry = pathPrefixInsideTheArchive + "/testReadArchive" + i + ".txt";
        internalArchiveFiles.add(entry);
        zos.putNextEntry(new ZipEntry(entry));
        zos.write(sb.toString().getBytes());
        zos.closeEntry();
      }
      zos.finish();
      zos.flush();
    }

    setupServer(path, true);

    int expectedTotalNoOfRecords = numberOfFilesInArchive * numberOfRecordsInAFile;
    int recordCount = 0;
    String lastOffset = "";

    //Run till all records are read.
    while (recordCount < expectedTotalNoOfRecords) {
      int randomBatchSize = RANDOM.nextInt(numberOfRecordsInAFile);
      //This is like pipeline stop and start, because we create a new source and runner
      //but use the old offset
      Pair<String, List<Record>> output =
          runSourceAndReturnOffsetAndRecords(lastOffset, randomBatchSize);

      List<Record> currentBatchRecords = output.getRight();

      for (Record record : currentBatchRecords) {
        //Read record and check headers to see whether the files are read properly
        int fileIdxInsideTheArchive = recordCount / numberOfRecordsInAFile;
        String currentInnerArchiveFileEntryName =
            internalArchiveFiles.get(fileIdxInsideTheArchive)
                .replace(pathPrefixInsideTheArchive + "/", "");

        Assert.assertNotNull(record.getHeader().getAttribute("fileNameInsideArchive"));
        Assert.assertEquals(
            currentInnerArchiveFileEntryName,
            record.getHeader().getAttribute("fileNameInsideArchive")
        );
        Assert.assertNotNull(record.getHeader().getAttribute("filePathInsideArchive"));
        Assert.assertEquals(
            pathPrefixInsideTheArchive,
            record.getHeader().getAttribute("filePathInsideArchive")
        );
        //Each record in a file two bytes (so multiply the result of the record idx in a file by 2)
        long currentOffset = (recordCount - (fileIdxInsideTheArchive * numberOfRecordsInAFile)) * 2;

        Assert.assertNotNull(record.getHeader().getAttribute("fileOffsetInsideArchive"));
        Assert.assertEquals(
            String.valueOf(currentOffset),
            record.getHeader().getAttribute("fileOffsetInsideArchive")
        );
        recordCount++;
      }
      lastOffset = output.getLeft();
    }
    Assert.assertEquals(expectedTotalNoOfRecords, recordCount);

    Pair<String, List<Record>> output =
        runSourceAndReturnOffsetAndRecords(lastOffset, 5);
    Assert.assertEquals(0, output.getRight().size());
  }


  @Test
  public void testHeaderAttributes() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testHeaderAttributes";
    List<String> foldersToCreate = ImmutableList.of("/folder1/folder11", "/folder2/folder22", "/folder3/folder33", "/folder4/folder44");
    List<String> filePaths = new ArrayList<>();

    long currrentMillis = System.currentTimeMillis() - (foldersToCreate.size() * 1000);
    for (String folder : foldersToCreate) {

      String fullFolderPath = path + folder ;
      Assert.assertTrue(new File(fullFolderPath).mkdirs());

      String fileFullPath = fullFolderPath + "/" + folder.substring(1).replaceAll("/", "_") + "testHeaderAttributes.txt";

      //creating file name as _ instead of / in folder
      //for ex: folder1/folder11 will have the file name folder1_folder11_testHeaderAttributes.txt
      java.nio.file.Files.write(
          Paths.get(fileFullPath),
          "This is sample text".getBytes(),
          StandardOpenOption.CREATE_NEW
      );

      Assert.assertTrue(Paths.get(fileFullPath).toFile().setLastModified(currrentMillis));
      currrentMillis = currrentMillis + 1000;
      //Strip local file path
      filePaths.add(fileFullPath.substring(path.length()));
    }

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.TEXT,
            null,
            //Process subdirectories
            true,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();
      String offset = "";
      for (int i = 0; i < filePaths.size(); i++) {
        StageRunner.Output op = runner.runProduce(offset, 1000);
        List<Record> records = op.getRecords().get("lane");
        Assert.assertEquals(1, records.size());
        Record.Header header = records.get(0).getHeader();
        String fileFullPath = filePaths.get(i);
        Assert.assertEquals(fileFullPath, header.getAttribute(HeaderAttributeConstants.FILE));
        String fileName = fileFullPath.substring(fileFullPath.lastIndexOf("/") + 1);
        Assert.assertEquals(fileName, header.getAttribute(HeaderAttributeConstants.FILE_NAME));
        Assert.assertNotNull(header.getAttribute(RemoteDownloadSource.REMOTE_URI));
        offset = op.getNewOffset();
      }
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testReadFileInMultipleRuns() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testReadFileInMultipleRuns";
    Path filePath =  Paths.get(path + "/testReadFileInMultipleRuns.txt");
    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        ("1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n" + "9\n" + "10\n").getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    int totalRecordsRead = 0, runTimes = 0, expectedRecordCount = 10, totalRunTimes = expectedRecordCount * 2;
    String lastOffset = RemoteDownloadSource.NOTHING_READ;
    while (runTimes < totalRunTimes) {
      SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
          .addOutputLane("lane")
          .build();
      runner.runInit();
      try {
        //no more files to process, this will fail.
        StageRunner.Output op = runner.runProduce(lastOffset, 5);
        lastOffset = op.getNewOffset();
        List<Record> actual = op.getRecords().get("lane");
        totalRecordsRead += actual.size();
      } finally {
        destroyAndValidate(runner);
      }
      runTimes++;
    }
    Assert.assertEquals(expectedRecordCount, totalRecordsRead);
  }

  @Test
  public void testEventsUserDirIsRoot() throws Exception {
    testEvents(true);
  }

  @Test
  public void testEvents() throws Exception {
    testEvents(false);
  }

  private void testEvents(boolean userDirIsRoot) throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testEvents";
    Path dir = Paths.get(path + "/dir");
    Assert.assertTrue(dir.toFile().mkdirs());
    Path filePath1 = Paths.get(dir + "/testEvents1.txt");
    Path filePath2 = Paths.get(dir + "/testEvents2.txt");

    byte[] sampleText = "This is sample text".getBytes();
    java.nio.file.Files.write(filePath1, sampleText, StandardOpenOption.CREATE_NEW);
    java.nio.file.Files.write(filePath2, sampleText, StandardOpenOption.CREATE_NEW);

    setupServer(path, true);

    String pathInUri = userDirIsRoot ? "" : path;
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/" + pathInUri,
            userDirIsRoot,
            DataFormat.TEXT,
            null,
            true,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce("", 10);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      List<EventRecord> eventRecords = runner.getEventRecords();
      Record newFileEvent = eventRecords.get(0);
      Record finishedFileEvent = eventRecords.get(1);

      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(
          "/" + dir.getFileName() + "/" + filePath1.getFileName().toString(),
          newFileEvent.get("/filepath").getValueAsString()
      );
      Assert.assertEquals(
          "/" + dir.getFileName() + "/" + filePath1.getFileName().toString(),
          finishedFileEvent.get("/filepath").getValueAsString()
      );
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      eventRecords = runner.getEventRecords();
      newFileEvent = eventRecords.get(0);
      finishedFileEvent = eventRecords.get(1);

      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(
          "/" + dir.getFileName() + "/" + filePath2.getFileName().toString(),
          newFileEvent.get("/filepath").getValueAsString()
      );
      Assert.assertEquals(
          "/" + dir.getFileName() + "/" + filePath2.getFileName().toString(),
          finishedFileEvent.get("/filepath").getValueAsString()
      );
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      Record noMoreDataEventRecord = runner.getEventRecords().get(0);
      Assert.assertEquals("no-more-data", noMoreDataEventRecord.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(2, noMoreDataEventRecord.get("/record-count").getValueAsLong());
      Assert.assertEquals(2, noMoreDataEventRecord.get("/file-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(0, runner.getEventRecords().size());

      String events3Name = "/testEvents3.txt";
      Path filePath3 = Paths.get(path + events3Name);
      java.nio.file.Files.write(filePath3, sampleText, StandardOpenOption.CREATE_NEW);

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(2, runner.getEventRecords().size());
      eventRecords = runner.getEventRecords();
      newFileEvent = eventRecords.get(0);
      finishedFileEvent = eventRecords.get(1);
      Assert.assertEquals("new-file", newFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals("finished-file", finishedFileEvent.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(events3Name, newFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals(events3Name, finishedFileEvent.get("/filepath").getValueAsString());
      Assert.assertEquals(1, finishedFileEvent.get("/record-count").getValueAsLong());

      runner.getEventRecords().clear();
      output = runner.runProduce(output.getNewOffset(), 10);
      records = output.getRecords().get("lane");
      Assert.assertEquals(0, records.size());
      Assert.assertEquals(1, runner.getEventRecords().size());
      //Counters are reset, so we should have seen one file after the last no more data event
      noMoreDataEventRecord = runner.getEventRecords().get(0);
      Assert.assertEquals("no-more-data", noMoreDataEventRecord.getHeader().getAttribute("sdc.event.type"));
      Assert.assertEquals(1, noMoreDataEventRecord.get("/record-count").getValueAsLong());
      Assert.assertEquals(1, noMoreDataEventRecord.get("/file-count").getValueAsLong());
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testMockReset() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testMockReset";
    Path filePath =  Paths.get(path + "/testMockReset.txt");
    Assert.assertTrue(new File(path).mkdirs());

    java.nio.file.Files.write(
        filePath,
        ("This is sample text").getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      //no more files to process, this will fail.
      StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 5);
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
    } finally {
      destroyAndValidate(runner);
    }

    runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      //no more files to process, this will fail.
      StageRunner.Output op = runner.runProduce(null, 5);
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
    } finally {
      destroyAndValidate(runner);
    }

  }

  @Test
  public void testMultiBatchSameFile() throws Exception {
    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/testMultiBatchSameFile";
    Path filePath =  Paths.get(path + "/testMultiBatchSameFile.txt");
    Assert.assertTrue(new File(path).mkdirs());

    StringBuilder sb = new StringBuilder();
    for (int i = 0 ; i < 100; i++) {
      sb.append("This is sample text" + i);
      sb.append("\n");
    }

    java.nio.file.Files.write(
        filePath,
        sb.toString().getBytes(),
        StandardOpenOption.CREATE_NEW
    );

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.TEXT,
            null,
            false,
            "*"
        ));

    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    List<Record> actual;
    String lastOffset = RemoteDownloadSource.NOTHING_READ;
    try {
      do {
        //Batch size 25, records 100. (4 batches)
        StageRunner.Output op = runner.runProduce(lastOffset, 25);
        lastOffset = op.getNewOffset();
        actual = op.getRecords().get("lane");
      } while (actual.size() > 0);
    } finally {
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testFileSelection() throws Exception {
    String [] fileNames = {
        "aaa",
        "aab",
        "bbb",
        "bba",
        "aaaa"
    };

    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source/fileNameSelection";
    Assert.assertTrue(new File(path).mkdirs());

    for(String f : fileNames) {
      File dest = new File(path, f);
      Files.write(someSampleData(), dest);
    }

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "??a"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }

    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      System.out.println(actual.get(i));
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }

    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertNotEquals(expected.size(), actual.size());
    destroyAndValidate(runner);
  }

  @Test
  public void testFileSelectionRecursive() throws Exception {
    String [] directories = {
        "fileNameSelection",
        "fileNameSelection/dir1",
        "fileNameSelection/dir2",
        "fileNameSelection/dir3"
    };

    String [] fileNames = {
        "aaa",
        "aab",
        "bbb",
        "bba",
        "aaaa"
    };

    path = testFolder.getRoot().getAbsolutePath() + "/remote-download-source";
    for(String dir : directories) {
      String myPath = path + '/' + dir;
      Assert.assertTrue(new File(myPath).mkdirs());

      for(String f : fileNames) {
        File dest = new File(myPath, f);
        Files.write(someSampleData(), dest);
      }
    }

    setupServer(path, true);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            true,
            "??a"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op;
    List<Record> expected = getExpectedRecords();
    List<Record> actual = null;

    // 2 files per directory.  root directory and 3 child directories.
    final int MATCHING_FILES = 8;

    for(int j = 0 ; j < MATCHING_FILES; ++j) {
      op = runner.runProduce(offset, 1000);
      actual = op.getRecords().get("lane");
      Assert.assertEquals(expected.size(), actual.size());
      for (int i = 0; i < 2; i++) {
        Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
      }
    }

    // try again, but there are no more matching files.
    op = runner.runProduce(offset, 1000);
    actual = op.getRecords().get("lane");
    Assert.assertNotEquals(expected.size(), actual.size());
    destroyAndValidate(runner);
  }

  @Test
  public void testLineage() throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);
    final String filePattern = "*";
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            filePattern
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);

    List<LineageEvent> events = runner.getLineageEvents();
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(LineageEventType.ENTITY_READ, events.get(0).getEventType());
    Assert.assertEquals(filePattern, events.get(0).getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));
    Assert.assertEquals(EndPointType.FTP.name(), events.get(0).getSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE));

    // run the second batch should not fire the lineage event since no file has been read from the origin
    runner.runProduce(op.getNewOffset(), 1000);
    events = runner.getLineageEvents();
    Assert.assertEquals(1, events.size());

    destroyAndValidate(runner);
  }

  @Test
  public void testConnectionRetry() throws Exception {
    path = "remote-download-source/parseNoError";
    setupServer(path, false);

    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    // No connections at first
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(0, opened.get());
      Assert.assertEquals(0, closed.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    runner.runInit();
    // Now we've made one connection
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(1, opened.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(1, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    // Set timeout after being idle to be really quick (1ms)
    if (scheme == Scheme.sftp) {
      PropertyResolverUtils.updateProperty(sshd, FactoryManager.IDLE_TIMEOUT, 1);
    } else if (scheme == Scheme.ftp) {
      ftpServer.getServerContext().getListeners().get("default").getActiveSessions().iterator().next().setMaxIdleTime(1);
    }
    // Wait until that one connection has been closed
    if (scheme == Scheme.sftp) {
      await().atMost(10, TimeUnit.SECONDS).until(() -> Assert.assertEquals(1, closed.get()));
      Assert.assertEquals(1, closed.get());
    } else if (scheme == Scheme.ftp) {
      await().atMost(10, TimeUnit.SECONDS).until(
          () -> Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber()));
      Assert.assertEquals(0, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
    }
    if (scheme == Scheme.sftp) {
      // Unset the timeout config for SFTP because it's global
      PropertyResolverUtils.updateProperty(sshd, FactoryManager.IDLE_TIMEOUT, FactoryManager.DEFAULT_IDLE_TIMEOUT);
    }
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    // Now we've opened a new connection
    if (scheme == Scheme.sftp) {
      Assert.assertEquals(2, opened.get());
    } else if (scheme == Scheme.ftp) {
      Assert.assertEquals(1, ftpServer.getServerContext().getFtpStatistics().getCurrentConnectionNumber());
      Assert.assertEquals(2, ftpServer.getServerContext().getFtpStatistics().getTotalConnectionNumber());
    }
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testMDTM() throws Exception {
    Assume.assumeTrue(scheme == Scheme.ftp);
    path = "remote-download-source/parseSameTimestamp";
    File dir = new File(currentThread().getContextClassLoader().getResource(path).getPath());
    File[] files = dir.listFiles();
    Assert.assertEquals(3, files.length);
    Date now = DateUtils.truncate(new Date(), Calendar.SECOND);
    for (File f : files) {
      if (f.getName().equals("panda.txt")) {
        Assert.assertTrue(f.setLastModified(now.getTime()));
      } else if (f.getName().equals("polarbear.txt")) {
        Assert.assertTrue(f.setLastModified(18000000000L));
      } else if (f.getName().equals("sloth.txt")) {
        Assert.assertTrue(f.setLastModified(17000000000L));
      }
    }
    setupServer(dir.getAbsolutePath(), true);

    testMDTMHelper(17000000000L, 18000000000L, now.getTime());

    // Now, we'll configure the FTP Server to pretend like it doesn't support MDTM, which should lower the accuracy and
    // correctness of the timestamps
    supportMDTM = false;

    testMDTMHelper(
        // Older dates are truncated to month
        DateUtils.truncate(new Date(17000000000L), Calendar.DAY_OF_MONTH).getTime(),
        DateUtils.truncate(new Date(18000000000L), Calendar.DAY_OF_MONTH).getTime(),
        // Recent dates are truncated to minute
        DateUtils.truncate(now, Calendar.MINUTE).getTime());
  }

  public void testMDTMHelper(long slothTime, long polarbearTime, long pandaTime) throws Exception {
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/",
            true,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    String offset = RemoteDownloadSource.NOTHING_READ;
    StageRunner.Output op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    Assert.assertEquals("/sloth.txt::" + slothTime + "::-1", offset);
    op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    Assert.assertEquals("/polarbear.txt::" + polarbearTime + "::-1", offset);
    op = runner.runProduce(offset, 1000);
    offset = op.getNewOffset();
    Assert.assertEquals("/panda.txt::" + pandaTime + "::-1", offset);
    destroyAndValidate(runner);
  }

  @Test
  public void testPathInUriUserDirIsRoot() throws Exception {
    testPathInUri(true);
  }

  @Test
  public void testPathInUri() throws Exception {
    testPathInUri(false);
  }

  private void testPathInUri(boolean userDirisRoot) throws Exception {
    String originPath =
        currentThread().getContextClassLoader().getResource("remote-download-source/parseNoError").getPath();
    File originDirFile = new File(originPath).listFiles()[0];
    File tempDir = testFolder.newFolder();
    File copied = new File(tempDir, originDirFile.getName());
    Files.copy(originDirFile, copied);
    setupServer(testFolder.getRoot().getAbsolutePath(), true);
    String pathInUri = userDirisRoot ? tempDir.getName() : tempDir.getAbsolutePath();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/" + pathInUri,
            userDirisRoot,
            DataFormat.JSON,
            null,
            false,
            "*"
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    StageRunner.Output op = runner.runProduce(RemoteDownloadSource.NOTHING_READ, 1000);
    List<Record> expected = getExpectedRecords();
    List<Record> actual = op.getRecords().get("lane");
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(expected.get(i).get(), actual.get(i).get());
    }
    destroyAndValidate(runner);
  }

  @Test
  public void testProcessingArchiveInvalidDir() throws Exception {
    for (String archiveDir : new String[]{null, ""}) {
      RemoteDownloadSource origin = new RemoteDownloadSource(getBean(
          scheme.name() + "://localhost:" + port + "/",
          true,
          DataFormat.JSON,
          null,
          true,
          FilePatternMode.GLOB,
          "*",
          1000,
          null,
          PostProcessingOptions.ARCHIVE,
          archiveDir,
          true
      ));
      SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
          .addOutputLane("lane")
          .build();
      runInitWithConfigException(runner, Errors.REMOTE_DOWNLOAD_07);
      destroyAndValidate(runner);
    }
  }

  @Test
  public void testPostProcessingDelete() throws Exception {
    testPostProcessing(false, false, false, true);
  }

  @Test
  public void testPostProcessingDeleteUserDirIsRoot() throws Exception {
    testPostProcessing(false, false, true, true);
  }

  @Test
  public void testPostProcessingDeleteInPreview() throws Exception {
    testPostProcessing(false, true, true, true);
  }

  @Test
  public void testPostProcessingArchive() throws Exception {
    testPostProcessing(true, false, false, false);
  }

  @Test
  public void testPostProcessingArchiveUserDirIsRoot() throws Exception {
    testPostProcessing(true, false, true, false);
  }

  @Test
  public void testPostProcessingArchiveArchiveUserDirIsRoot() throws Exception {
    testPostProcessing(true, false, false, true);
  }

  @Test
  public void testPostProcessingArchiveUserDirIsRootArchiveUserDirIsRoot() throws Exception {
    testPostProcessing(true, false, true, true);
  }

  @Test
  public void testPostProcessingArchiveInPreview() throws Exception {
    testPostProcessing(true, true, true, true);
  }

  private void testPostProcessing(
      boolean archive, boolean isPreview, boolean userDirIsRoot, boolean archiveDirUserDirIsRoot
  ) throws Exception {
    String originPath =
        currentThread().getContextClassLoader().getResource("remote-download-source/parseSameTimestamp").getPath();
    File userDir = testFolder.newFolder("user-dir");
    File dataDir = new File(userDir, "data-dir");
    dataDir.mkdir();
    List<File> files = new ArrayList<>();
    for (File originDirFile : new File(originPath).listFiles()) {
      File copied = new File(dataDir, originDirFile.getName());
      Files.copy(originDirFile, copied);
      if (originDirFile.getName().equals("panda.txt")) {
        Assert.assertTrue(copied.setLastModified(18000000000L));
      } else if (originDirFile.getName().equals("polarbear.txt")) {
        Assert.assertTrue(copied.setLastModified(19000000000L));
      } else if (originDirFile.getName().equals("sloth.txt")) {
        Assert.assertTrue(copied.setLastModified(17000000000L));
      }
      files.add(copied);
    }
    File archiveDir = new File(userDir, "archive-dir");
    setupServer(userDir.getAbsolutePath(), true);
    for (File file : files) {
      Assert.assertTrue(file.exists());
    }
    String pathInUri = userDirIsRoot ? "" : userDir.getAbsolutePath();
    RemoteDownloadSource origin =
        new RemoteDownloadSource(getBean(
            scheme.name() + "://localhost:" + port + "/" + pathInUri,
            userDirIsRoot,
            DataFormat.JSON,
            null,
            true,
            FilePatternMode.GLOB,
            "*",
            1000,
            null,
            archive ? PostProcessingOptions.ARCHIVE : PostProcessingOptions.DELETE,
            archiveDirUserDirIsRoot ? archiveDir.getName() : archiveDir.getAbsolutePath(),
            archiveDirUserDirIsRoot
        ));
    SourceRunner runner = new SourceRunner.Builder(RemoteDownloadDSource.class, origin)
        .addOutputLane("lane")
        .setPreview(isPreview)
        .build();
    runner.runInit();
    List<Record> expected = getExpectedRecords(true);
    String offset = RemoteDownloadSource.NOTHING_READ;
    for (int i = 0; i < 3; i++) {
      StageRunner.Output op = runner.runProduce(offset, 1000);
      offset = op.getNewOffset();
      List<Record> actual = op.getRecords().get("lane");
      Assert.assertEquals(1, actual.size());
      Assert.assertEquals(expected.get(i).get(), actual.get(0).get());
    }
    // Check files were deleted (this is still the case for archiving because they were moved)
    // On the other hand, if it's a preview, then they should still exist
    for (File file : files) {
      Assert.assertEquals(isPreview, file.exists());
    }
    if (archive) {
      // Check archive files were created
      // On the other hand, if it's a preview, then they should not have been created
      File archiveDataDir = new File(archiveDir, dataDir.getName());
      Assert.assertEquals(!isPreview, archiveDir.exists());
      Assert.assertEquals(!isPreview, archiveDataDir.exists());
      for (File file : files) {
        Assert.assertEquals(!isPreview, new File(archiveDataDir, file.getName()).exists());
      }
    }
    destroyAndValidate(runner);
  }

  private void destroyAndValidate(SourceRunner runner) throws Exception {
    runner.runDestroy();
    if (scheme == Scheme.sftp) {
      await().atMost(10, TimeUnit.SECONDS).untilTrue(closedAll);
    }
  }

  private byte [] someSampleData() {
    String data =
        "{\"name\": \"sloth\",\"age\": \"5\",\"characterisitics\": [\"cute\", \"slooooow\", \"sloooooow\", \"sloooooooow\"]}\n"
            + "{\"name\": \"panda\",\"age\": \"3\",\"characterisitics\": [\"cool\", \"cute\", \"round\", \"playful\", \"hungry\"]}";
    return data.getBytes();
  }

  private RemoteDownloadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      DataFormat dataFormat,
      String errorArchive,
      boolean processSubDirectories,
      String filePattern
  ) {
    return getBean(
        remoteHost,
        userDirIsRoot,
        dataFormat,
        errorArchive,
        processSubDirectories,
        FilePatternMode.GLOB,
        filePattern,
        1000,
        ""
    );
  }

  private RemoteDownloadConfigBean getBean(
    String remoteHost,
    boolean userDirIsRoot,
    DataFormat dataFormat,
    String errorArchive,
    boolean processSubDirectories,
    String filePattern,
    int batchSize
  ) {
    return getBean(
      remoteHost,
      userDirIsRoot,
      dataFormat,
      errorArchive,
      processSubDirectories,
      FilePatternMode.GLOB,
      filePattern,
      batchSize,
      ""
    );
  }

  private RemoteDownloadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      DataFormat dataFormat,
      String errorArchive,
      boolean processSubDirectories,
      FilePatternMode filePatternMode,
      String filePattern,
      int batchSize,
      String initialFile
  ) {
    return getBean(
        remoteHost,
        userDirIsRoot,
        dataFormat,
        errorArchive,
        processSubDirectories,
        filePatternMode,
        filePattern,
        batchSize,
        initialFile,
        PostProcessingOptions.NONE,
        null,
        true
    );
  }

  private RemoteDownloadConfigBean getBean(
      String remoteHost,
      boolean userDirIsRoot,
      DataFormat dataFormat,
      String errorArchive,
      boolean processSubDirectories,
      FilePatternMode filePatternMode,
      String filePattern,
      int batchSize,
      String initialFile,
      PostProcessingOptions postProcessing,
      String archiveDir,
      boolean archiveDirUserDirIsRoot
  ) {
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.remoteConfig.remoteAddress = remoteHost;
    configBean.remoteConfig.userDirIsRoot = userDirIsRoot;
    configBean.remoteConfig.username = () -> TESTUSER;
    configBean.remoteConfig.auth = Authentication.PASSWORD;
    configBean.remoteConfig.password = () -> TESTPASS;
    configBean.remoteConfig.strictHostChecking = false;
    configBean.dataFormat = dataFormat;
    configBean.errorArchiveDir = errorArchive;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    configBean.processSubDirectories = processSubDirectories;
    configBean.filePatternMode = filePatternMode;
    configBean.filePattern = filePattern;
    configBean.basic.maxBatchSize = batchSize;
    configBean.initialFileToProcess = initialFile;
    configBean.postProcessing = postProcessing;
    configBean.archiveDir = archiveDir;
    configBean.archiveDirUserDirIsRoot = archiveDirUserDirIsRoot;
    return configBean;
  }

  private StageException runInitWithConfigException(SourceRunner runner, ErrorCode... expected) {
    try {
      runner.runInit();
      Assert.fail("Expected a StageException");
    } catch (StageException e) {
      assertExceptionMessageContainsOnlyRemoteError(e, expected);
      return e;
    }
    return null;
  }

  private void assertExceptionMessageContainsOnlyRemoteError(Exception e, ErrorCode... expected) {
    String msg = e.getMessage();
    for (ErrorCode errror : expected) {
      Assert.assertTrue("Expected exception to contain " + errror.getCode() + " but did not: " + msg,
          msg.contains(errror.getCode()));
    }
    List<String> foundErrors = new ArrayList<>();
    for (Errors error : Errors.values()) {
      if (!ArrayUtils.contains(expected, error) && msg.contains(error.getCode())) {
        foundErrors.add(error.getCode());
      }
    }
    if (!foundErrors.isEmpty()) {
      Assert.fail("Expected exception NOT to contain " + Arrays.toString(foundErrors.toArray()) + " but it did: "
          + msg);
    }
  }

  private void setupServer(String homeDir, boolean absolutePath) throws Exception {
    if (!absolutePath) {
      URL url = currentThread().getContextClassLoader().getResource(homeDir);
      homeDir = url.getPath();
    }
    if (scheme == Scheme.sftp) {
      setupSSHD(homeDir);
    } else if (scheme == Scheme.ftp) {
      setupFTPServer(homeDir);
    }
  }
}
