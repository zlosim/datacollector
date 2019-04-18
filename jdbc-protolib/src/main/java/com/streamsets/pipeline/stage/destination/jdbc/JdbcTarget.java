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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.jdbc.DuplicateKeyAction;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationType;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordReaderWriterFactory;
import com.streamsets.pipeline.lib.jdbc.JdbcRecordWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.SchemaAndTable;
import com.streamsets.pipeline.lib.jdbc.SchemaTableClassifier;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JDBC Destination for StreamSets Data Collector
 */
public class JdbcTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTarget.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private final boolean rollbackOnError;
  private final boolean useMultiRowOp;
  private final int maxPrepStmtParameters;

  private final String schemaNameTemplate;
  private final String tableNameTemplate;
  private SchemaTableClassifier schemaTableClassifier = null;
  private final List<JdbcFieldColumnParamMapping> customMappings;
  private final boolean caseSensitive;
  private final boolean dynamicSchemaName;
  private final boolean dynamicTableName;
  private final List<String> customDataSqlStateCodes;

  private final ChangeLogFormat changeLogFormat;
  private final HikariPoolConfigBean hikariConfigBean;
  private final CacheCleaner cacheCleaner;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;

  private final int defaultOpCode;
  private final UnsupportedOperationAction unsupportedAction;
  private final DuplicateKeyAction duplicateKeyAction;

  private final JdbcUtil jdbcUtil;

  class RecordWriterLoader extends CacheLoader<SchemaAndTable, JdbcRecordWriter> {
    @Override
    public JdbcRecordWriter load(SchemaAndTable key) throws Exception {
      return JdbcRecordReaderWriterFactory.createJdbcRecordWriter(
          hikariConfigBean.getConnectionString(),
          dataSource,
          key.getSchemaName(),
          key.getTableName(),
          customMappings,
          rollbackOnError,
          useMultiRowOp,
          maxPrepStmtParameters,
          defaultOpCode,
          unsupportedAction,
          duplicateKeyAction,
          JdbcRecordReaderWriterFactory.createRecordReader(changeLogFormat),
          caseSensitive,
          customDataSqlStateCodes
      );
    }
  }

  private final LoadingCache<SchemaAndTable, JdbcRecordWriter> recordWriters;

  public JdbcTarget(
      final String schemaNameTemplate,
      final String tableNameTemplate,
      final List<JdbcFieldColumnParamMapping> customMappings,
      final boolean caseSensitive,
      final boolean rollbackOnError,
      final boolean useMultiRowOp,
      int maxPrepStmtParameters,
      final ChangeLogFormat changeLogFormat,
      final JDBCOperationType defaultOperation,
      final UnsupportedOperationAction unsupportedAction,
      final HikariPoolConfigBean hikariConfigBean,
      final List<String> customDataSqlStateCodes
  ) {
    this(
        schemaNameTemplate,
        tableNameTemplate,
        customMappings,
        caseSensitive,
        rollbackOnError,
        useMultiRowOp,
        maxPrepStmtParameters,
        changeLogFormat,
        defaultOperation.getCode(),
        unsupportedAction,
        null, // no support for duplicate-key errors
        hikariConfigBean,
        customDataSqlStateCodes
    );
  }

  public JdbcTarget(
      final String schemaNameTemplate,
      final String tableNameTemplate,
      final List<JdbcFieldColumnParamMapping> customMappings,
      final boolean caseSensitive,
      final boolean rollbackOnError,
      final boolean useMultiRowOp,
      int maxPrepStmtParameters,
      final ChangeLogFormat changeLogFormat,
      final int defaultOpCode,
      UnsupportedOperationAction unsupportedAction,
      DuplicateKeyAction duplicateKeyAction,
      HikariPoolConfigBean hikariConfigBean,
      final List<String> customDataSqlStateCodes
  ) {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.schemaNameTemplate = schemaNameTemplate;
    this.tableNameTemplate = tableNameTemplate;
    this.customMappings = customMappings;
    this.caseSensitive = caseSensitive;
    this.rollbackOnError = rollbackOnError;
    this.useMultiRowOp = useMultiRowOp;
    this.maxPrepStmtParameters = maxPrepStmtParameters;
    this.changeLogFormat = changeLogFormat;
    this.defaultOpCode = defaultOpCode;
    this.unsupportedAction = unsupportedAction;
    this.duplicateKeyAction = duplicateKeyAction;
    this.hikariConfigBean = hikariConfigBean;
    this.dynamicTableName = jdbcUtil.isElString(tableNameTemplate);
    this.dynamicSchemaName = jdbcUtil.isElString(schemaNameTemplate);
    this.customDataSqlStateCodes = customDataSqlStateCodes;

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(500)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .removalListener((RemovalListener<SchemaAndTable, JdbcRecordWriter>) removal -> {
          removal.getValue().deinit();
        });

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    this.recordWriters = cacheBuilder.build(new RecordWriterLoader());

    cacheCleaner = new CacheCleaner(this.recordWriters, "JdbcTarget", 10 * 60 * 1000);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    Target.Context context = getContext();
    issues = hikariConfigBean.validateConfigs(context, issues);
    errorRecordHandler = new DefaultErrorRecordHandler(context);

    if (dynamicSchemaName || dynamicTableName) {
      schemaTableClassifier = new SchemaTableClassifier(schemaNameTemplate, tableNameTemplate, context);
    }

    ELUtils.validateExpression(
        schemaNameTemplate,
        context,
        Groups.JDBC.getLabel(),
        JdbcUtil.SCHEMA_NAME,
        JdbcErrors.JDBC_103,
        issues
    );

    ELUtils.validateExpression(
        tableNameTemplate,
        context,
        Groups.JDBC.getLabel(),
        JdbcUtil.TABLE_NAME,
        JdbcErrors.JDBC_26,
        issues
    );

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = jdbcUtil.createDataSourceForWrite(
            hikariConfigBean,
            schemaNameTemplate,
            tableNameTemplate,
            caseSensitive,
            issues,
            customMappings,
            context
        );
      } catch (RuntimeException | SQLException | StageException e) {
        LOG.debug("Could not connect to data source", e);
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    return issues;
  }

  @Override
  public void destroy() {
    recordWriters.invalidateAll();
    if (null != dataSource) {
      dataSource.close();
    }
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    // jdbc target always commit batch execution
    final boolean perRecord = false;

    if (dynamicSchemaName || dynamicTableName)  {
      jdbcUtil.write(
          batch,
          schemaTableClassifier,
          recordWriters,
          errorRecordHandler,
          perRecord
      );
    } else {
      SchemaAndTable key = new SchemaAndTable(schemaNameTemplate, tableNameTemplate);
      jdbcUtil.write(batch.getRecords(), key, recordWriters, errorRecordHandler, perRecord);
    }
  }
}
