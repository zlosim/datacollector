package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

public class AerospikeTargetConfig {
  public static final String AEROSPIKE_TARGET_CONFIG_PREFIX = "AerospikeTargetConfig.";


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:3000",
      label = "Aerospike nodes",
      description = "Comma-separated list of Aerospike nodes. Use format <HOST>:<PORT>",
      displayPosition = 10,
      group = "AEROSPIKE"
  )
  public String connectionString;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Retry Attempts",
      defaultValue = "1",
      required = true,
      min = 1,
      displayPosition = 20,
      group = "AEROSPIKE"
  )
  public int maxRetries = 1;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "namespace",
      description = "Expression to get namespace",
      displayPosition = 30,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String namespaceExpr;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Set",
      description = "Expression to get set",
      displayPosition = 40,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String setExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Key",
      description = "Expression to get key",
      displayPosition = 50,
      group = "AEROSPIKE",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String keyExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Bins",
      description = "Bin names and their values",
      displayPosition = 10,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "MAPPING"
  )
  @ListBeanModel
  public List<BinMappingConfig> binMappingConfigs;

}
