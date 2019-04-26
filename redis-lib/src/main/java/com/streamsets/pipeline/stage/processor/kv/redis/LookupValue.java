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
package com.streamsets.pipeline.stage.processor.kv.redis;

import com.streamsets.pipeline.lib.redis.DataType;

import java.util.Objects;

public class LookupValue{
  private DataType type;
  private Object value;

  public LookupValue(Object value, DataType type) {
    this.value = value;
    this.type = type;
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    boolean result = false;
    if(other instanceof LookupValue) {
      LookupValue that = (LookupValue)other;
      result = (this.type.equals(that.getType()) && ((this.value == null && that.getValue() == null) || this.value.equals(that.getValue())));
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }
}
