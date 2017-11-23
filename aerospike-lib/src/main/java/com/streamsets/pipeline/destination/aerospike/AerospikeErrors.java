package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum AerospikeErrors implements ErrorCode {
  AEROSPIKE_01("Connection string cannot be empty"),
  AEROSPIKE_02("Invalid URI '{}'"),
  AEROSPIKE_03("Unable to connect to Aerospike on '{}'"),
  AEROSPIKE_04("Put operation was not successful '{}'"),
  AEROSPIKE_05("There has to be at least one bin to store"),
  ;

  private final String msg;
  AerospikeErrors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
