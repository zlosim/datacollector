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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@StageDef(
    // We're reusing upgrader for both ToErrorKinesisDTarget & KinesisDTarget, make sure that you
    // upgrade both versions at the same time when changing.
    version = 6,
    label = "Kinesis Producer",
    description = "Writes data to Amazon Kinesis",
    icon = "kinesis.png",
    upgrader = KinesisTargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_q2j_ml4_yr"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class KinesisDTarget extends DTarget {

  @ConfigDefBean(groups = {"KINESIS", "DATA_FORMAT"})
  public KinesisProducerConfigBean kinesisConfig;

  @Override
  protected Target createTarget() {
    return new KinesisTarget(kinesisConfig);
  }
}
