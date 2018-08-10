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
package com.streamsets.pipeline.stage.destination.kinesis;


import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    // We're reusing upgrader for both ToErrorKinesisDTarget & KinesisDTarget, make sure that you
    // upgrade both versions at the same time when changing.
    version = 6,
    label = "Write to Kinesis",
    description = "Writes records to Kinesis as SDC Records",
    onlineHelpRefUrl ="index.html?contextID=concept_kgc_l4y_5r",
    upgrader = KinesisTargetUpgrader.class
)
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {
        "kinesisConfig.dataFormat"
    }
)
@ErrorStage
@HideStage(HideStage.Type.ERROR_STAGE)
@GenerateResourceBundle
public class ToErrorKinesisDTarget extends KinesisDTarget {

  @Override
  protected Target createTarget() {
    kinesisConfig.dataFormat = DataFormat.SDC_JSON;
    return new KinesisTarget(kinesisConfig);
  }

}
