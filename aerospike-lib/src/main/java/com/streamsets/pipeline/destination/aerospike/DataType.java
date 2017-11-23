package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum DataType implements Label {
  STRING("String")
  ;

  private String label;

  DataType(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}

