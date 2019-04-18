/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.processor.crypto;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version = 1,
    label = "Encrypt and Decrypt Fields",
    description = "Encrypts or decrypts field values",
    icon = "crypto.png",
    onlineHelpRefUrl = "index.html?contextID=ask_fyd_vcj_x2b"
)
@ConfigGroups(ProcessorEncryptGroups.class)
@GenerateResourceBundle
public class FieldEncryptDProcessor extends DProcessor {

  @ConfigDefBean
  public ProcessorFieldEncryptConfig conf = new ProcessorFieldEncryptConfig();

  @Override
  protected Processor createProcessor() {
    return new FieldEncryptProcessor(conf);
  }
}
