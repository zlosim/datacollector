/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum DataType implements Label {
  STRING("String", String.class),
  LONG("Long", Long.class),
  DOUBLE("Double", Double.class);

  private String label;
  private Class className;

  DataType(String label, Class className) {
    this.label = label;
    this.className = className;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Class getClassName() {
    return className;
  }
}

