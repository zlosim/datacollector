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

package com.streamsets.pipeline.lib.jdbc.multithread.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class MSQueryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MSQueryUtil.class);
  static final String CT_TABLE_NAME = "CT";
  static final String TABLE_NAME = "P";

  public static final String SYS_CHANGE_VERSION = "SYS_CHANGE_VERSION";
  public static final int SYS_CHANGE_VERSION_TYPE = -5; // bigint

  public static final String CDC_START_LSN = "__$start_lsn";
  public static final String CDC_END_LSN = "__$end_lsn";
  public static final String CDC_SEQVAL = "__$seqval";
  public static final String CDC_OPERATION = "__$operation";
  public static final String CDC_UPDATE_MASK = "__$update_mask";
  public static final String CDC_COMMAND_ID = "__$command_id";
  public static final String CDC_TXN_WINDOW = "__$sdc.txn_window";
  public static final String CDC_SOURCE_SCHEMA_NAME = "schema_name";
  public static final String CDC_SOURCE_TABLE_NAME = "table_name";
  public static final String CDC_CAPTURE_INSTANCE_NAME = "capture_instance_name";

  private static final String IF_EXISTENCE_CDC_TABLE_QUERY = "IF EXISTS (SELECT OBJECT_ID FROM cdc.change_tables WHERE capture_instance='%s')";
  private static final String BEGIN_QUERY = "BEGIN";
  private static final String END_QUERY = "END";

  private static final String CHANGE_TRACKING_TABLE_QUERY = "SELECT min_valid_version \n" +
      "FROM sys.change_tracking_tables t\n" +
      "WHERE t.object_id = OBJECT_ID('%s.%s')";

  private static final String CHANGE_TRACKING_CURRENT_VERSION_QUERY = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

  private static final String INIT_CHANGE_TRACKING_QUERY = "DECLARE @synchronization_version BIGINT = %1$s;\n" +
      "\n" +
      "SELECT * \n" +
      "FROM %2$s AS P\n" +
      "RIGHT OUTER JOIN CHANGETABLE(CHANGES %2$s, @synchronization_version) AS " + CT_TABLE_NAME + "\n" +
      "%3$s\n" +
      "%4$s";

  private static final String CHANGE_TRACKING_QUERY = "SELECT * \n" +
          "FROM %1$s AS " + TABLE_NAME + "\n" +
          "RIGHT OUTER JOIN CHANGETABLE(CHANGES %1$s, %2$s) AS " + CT_TABLE_NAME + "\n" +
          "%3$s\n" +
          "%4$s\n" +
          "%5$s\n";

  private static final String SELECT_CT_CLAUSE = "SELECT * FROM CHANGETABLE(CHANGES %s, %s) AS CT %s %s";
  private static final String SELECT_CLAUSE = "SELECT * " +
      "FROM cdc.fn_cdc_get_all_changes_%s (@start_lsn, @to_lsn, N'all update old') ";
  private static final String SELECT_TABLE_CLAUSE = "SELECT * FROM cdc.%s_CT ";

  private static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ");
  private static final Joiner AND_JOINER = Joiner.on(" AND ");

  private static final String COLUMN_GREATER_THAN_VALUE = "%s > '%s' ";
  private static final String BINARY_COLUMN_GREATER_THAN_CLAUSE = "%s > CAST(0x%s AS BINARY(10)) ";
  private static final String COLUMN_EQUALS_VALUE = "%s = %s ";
  private static final String BINARY_COLUMN_EQUALS_CLAUSE = "%s = CAST(0x%s AS BINARY(10)) ";
  private static final String ON_CLAUSE = " ON %s";
  private static final String WHERE_CLAUSE = "WHERE %s ";
  private static final String ORDER_BY_CLAUSE = " ORDER BY %s ";
  private static final String OR_CLAUSE = "(%s) OR (%s) ";
  private static final String AND_CLAUSE = "(%s) AND (%s) ";
  private static final String AND_CLAUSE2 = "(%s) AND (%s) AND (%s)";

  private MSQueryUtil() {}

  public static String getCurrentVersion() {
    return CHANGE_TRACKING_CURRENT_VERSION_QUERY;
  }

  public static String getMinVersion(String schema, String table) {
    return String.format(CHANGE_TRACKING_TABLE_QUERY, schema, table);
  }

  public static String buildQuery(
      Map<String, String> offsetMap,
      int maxBatchSize, String tableName,
      Collection<String> offsetColumns,
      Map<String, String> startOffset,
      boolean includeJoin,
      long offset
  ) {
    boolean isInitial = true;

    List<String> greaterCondition = new ArrayList<>();
    List<String> equalCondition = new ArrayList<>();
    List<String> orderCondition = new ArrayList<>();
    String greater = "";

    orderCondition.add(SYS_CHANGE_VERSION);

    for (String primaryKey: offsetColumns) {
      if (!primaryKey.equals(SYS_CHANGE_VERSION)) {
        equalCondition.add(String.format(COLUMN_EQUALS_VALUE, CT_TABLE_NAME + "." + primaryKey, TABLE_NAME + "." + primaryKey));
        orderCondition.add(CT_TABLE_NAME + "." + primaryKey);

        if (!Strings.isNullOrEmpty(offsetMap.get(primaryKey))) {
          greaterCondition.add(String.format(COLUMN_GREATER_THAN_VALUE, CT_TABLE_NAME + "." + primaryKey, offsetMap.get(primaryKey)));
          isInitial = false;
        }
      }
    }

    String equal = String.format(ON_CLAUSE, AND_JOINER.join(equalCondition));
    String orderby = String.format(ORDER_BY_CLAUSE, COMMA_SPACE_JOINER.join(orderCondition));

    long lastSysChangeVersion = offsetMap.get(SYS_CHANGE_VERSION) == null ? 0 : Long.parseLong(offsetMap.get(SYS_CHANGE_VERSION));

    if (!isInitial) {
      greaterCondition.add(String.format(COLUMN_EQUALS_VALUE, CT_TABLE_NAME + "." + SYS_CHANGE_VERSION, offsetMap.get(SYS_CHANGE_VERSION)));
      String condition1 = AND_JOINER.join(greaterCondition);
      String condition2 = String.format(COLUMN_GREATER_THAN_VALUE, CT_TABLE_NAME + "." + SYS_CHANGE_VERSION, offsetMap.get(SYS_CHANGE_VERSION));
      greater = String.format(WHERE_CLAUSE, String.format(OR_CLAUSE, condition1, condition2));

      if (includeJoin) {
        return String.format(
            CHANGE_TRACKING_QUERY,
            tableName,
            offset,
            equal,
            greater,
            orderby
        );

      } else {
        return String.format(
            SELECT_CT_CLAUSE,
            tableName,
            offset,
            greater,
            orderby
        );
      }
    }

    if (includeJoin) {
      return String.format(
          INIT_CHANGE_TRACKING_QUERY,
          offset,
          tableName,
          equal,
          orderby
      );
    } else {
      return String.format(
          SELECT_CT_CLAUSE,
          tableName,
          offset,
          greater,
          orderby
      );
    }
  }

  public static String buildCDCQuery(
      Map<String, String> offsetMap,
      String tableName,
      Map<String, String> startOffset,
      boolean allowLateTable,
      boolean enableSchemaChanges,
      int fetchSize,
      boolean useTable,
      int txnWindow
  ) {
    String captureInstanceName = tableName.substring("cdc.".length(), tableName.length() - "_CT".length());
    StringBuilder query = new StringBuilder();
    String declare_from_lsn;
    String declare_to_lsn;
    String declare_to_lsn2 = "";
    String where_clause;

    // check the existing of CDC table
    if (allowLateTable) {
      query.append(String.format(IF_EXISTENCE_CDC_TABLE_QUERY, captureInstanceName));
      query.append("\n");
      query.append(BEGIN_QUERY);
      query.append("\n");
    }

    // initial offset
    if (offsetMap.get(CDC_START_LSN) == null) {
      String condition = "";
      if (startOffset.get(CDC_START_LSN) == null) {
        declare_from_lsn = String.format("DECLARE @start_lsn binary(10) = sys.fn_cdc_get_min_lsn (N'%s')",
            captureInstanceName
        );
        condition = "__$start_lsn > @start_lsn and __$start_lsn <= @to_lsn";
      } else {

        if (startOffset.get(CDC_START_LSN).equals("-1")) {
          declare_from_lsn = String.format("DECLARE @start_lsn binary(10) = sys.fn_cdc_get_max_lsn(); ");

          condition = "__$start_lsn >= @start_lsn and __$start_lsn <= @to_lsn";
        } else {
          declare_from_lsn = String.format(
              "DECLARE @start_lsn binary(10) " + "= 0x%s; ",
              startOffset.get(CDC_START_LSN)
          );
          condition = "__$start_lsn >= @start_lsn and __$start_lsn <= @to_lsn";
        }
      }

      where_clause = String.format(WHERE_CLAUSE, condition);

    } else {
      declare_from_lsn = String.format("DECLARE @start_lsn binary(10) = 0x%s; ",
          offsetMap.get(CDC_START_LSN));

      String condition1 = String.format(
          AND_CLAUSE2,
          String.format(COLUMN_EQUALS_VALUE, CDC_START_LSN, "@start_lsn"),
          String.format(BINARY_COLUMN_EQUALS_CLAUSE, CDC_SEQVAL, offsetMap.get(CDC_SEQVAL)),
          String.format(COLUMN_GREATER_THAN_VALUE, CDC_OPERATION, offsetMap.get(CDC_OPERATION))
      );

      condition1 = String.format(
          OR_CLAUSE,
          condition1,
          String.format(
              AND_CLAUSE,
              String.format(COLUMN_EQUALS_VALUE, CDC_START_LSN,  "@start_lsn"),
              String.format(BINARY_COLUMN_GREATER_THAN_CLAUSE, CDC_SEQVAL,  offsetMap.get(CDC_SEQVAL))
              )
      );

      String condition2 = "__$start_lsn > @start_lsn and __$start_lsn <= @to_lsn";

      where_clause = String.format(WHERE_CLAUSE, String.format(OR_CLAUSE, condition1, condition2));
    }

    if (txnWindow > 0) {
      declare_to_lsn = String.format(
          "DECLARE @to_lsn binary(10) = " + "sys.fn_cdc_map_time_to_lsn('largest less than or equal', " +
              "DATEADD(second, %s, sys.fn_cdc_map_lsn_to_time(@start_lsn))); ",
          txnWindow
      );
      declare_to_lsn2 = String.format("IF @start_lsn = @to_lsn " +
          "SET @to_lsn = sys.fn_cdc_get_max_lsn(); ");
    } else {
      declare_to_lsn = String.format("DECLARE @to_lsn binary(10) = sys.fn_cdc_get_max_lsn(); ");
    }


    query.append(declare_from_lsn);
    query.append(declare_to_lsn);
    query.append(declare_to_lsn2);

    if (useTable) {
      query.append(String.format(SELECT_TABLE_CLAUSE, captureInstanceName));
    } else {
      query.append(String.format(SELECT_CLAUSE, captureInstanceName));
    }


    query.append(where_clause);

    query.append(String.format(ORDER_BY_CLAUSE, COMMA_SPACE_JOINER.join(ImmutableList.of(CDC_START_LSN, CDC_SEQVAL, CDC_OPERATION))));

    if (allowLateTable) {
      query.append(END_QUERY);
    }

    // if schema change detection is enabled, get first row of the source table
    if (enableSchemaChanges) {
      query.append("\n");
      query.append("DECLARE @schema_name VARCHAR(MAX);");
      query.append("\n");
      query.append("DECLARE @table_name VARCHAR(MAX);");
      query.append("\n");
      query.append("DECLARE @capture_instance_name VARCHAR(MAX);");
      query.append("\n");

      // get the source table info
      query.append(
          "SELECT @schema_name=OBJECT_SCHEMA_NAME(source_object_id), @table_name=OBJECT_NAME(source_object_id), @capture_instance_name=capture_instance");
      query.append(" FROM cdc.change_tables");
      query.append(String.format(
          " WHERE capture_instance = '%s';",
          captureInstanceName
      ));

      query.append("\n");
      // get the first row of source table in additional to source table info (schema & table name) and capture instance name
      query.append(
          "EXEC(" +
            "'SELECT TOP 1 * " +
            " FROM (" +
                "SELECT ''' + @schema_name + ''' AS " + CDC_SOURCE_SCHEMA_NAME + ", " +
                "''' + @table_name + ''' AS " + CDC_SOURCE_TABLE_NAME + ", " +
                "''' + @capture_instance_name + ''' AS " + CDC_CAPTURE_INSTANCE_NAME +
            ") TMP" +
            " LEFT OUTER JOIN ' + @schema_name + '.' + @table_name " +
            "+ ' ON TMP." + CDC_SOURCE_SCHEMA_NAME + "<> NULL'" +
          ");"
      );
    }

    return query.toString();
  }
}