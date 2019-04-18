/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.couchbase;

import com.streamsets.pipeline.api.ConfigDef;

public class N1QLMappingConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Property Name",
      displayPosition = 10,
      description = "The document property name",
      group = "SUBDOC"
  )
  public String property;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "SDC Field",
      description = "The field in the record to receive the value",
      displayPosition = 20,
      group = "SUBDOC"
  )
  public String sdcField;
}
