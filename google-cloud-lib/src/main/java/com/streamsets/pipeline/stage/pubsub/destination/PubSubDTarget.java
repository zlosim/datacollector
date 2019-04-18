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

package com.streamsets.pipeline.stage.pubsub.destination;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;

@StageDef(
    version = 2,
    label = "Google Pub Sub Publisher",
    description = "Publishes messages to Google Pub/Sub",
    icon = "pubsub.png",
    upgrader = PubSubTargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_n1k_sk1_v1b"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class PubSubDTarget extends DTarget {
  @ConfigDefBean
  public PubSubTargetConfig conf = new PubSubTargetConfig();

  @Override
  protected Target createTarget() {
    conf.dataFormatConfig.isDelimited = conf.isDelimited;
    return new PubSubTarget(conf);
  }
}
