package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class BinMappingConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bin",
      description = "Expression to get bin name",
      displayPosition = 20
  )
  public String binExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Value",
      description = "Expression to get value",
      defaultValue="${record:attribute('/val')}",
      displayPosition = 30
  )
  public String valueExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="STRING",
      label = "Partition Value Type",
      description="Partition column's value type",
      displayPosition = 20
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType valueType = DataType.STRING;

  public BinMappingConfig() {
  }
}
