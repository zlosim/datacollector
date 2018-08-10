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
package com.streamsets.pipeline.stage.destination.maprstreams;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;

@StageDef(
    version = 3,
    label = "Write to MapR Streams",
    description = "Writes Pipeline Statistic records to MapR Streams",
    icon = "mapr_es.png",
    upgrader = MapRStreamsTargetUpgrader.class,
    onlineHelpRefUrl = ""
)
@StatsAggregatorStage
@HideStage(HideStage.Type.STATS_AGGREGATOR_STAGE)
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {
        "maprStreamsTargetConfigBean.dataFormat",
        "maprStreamsTargetConfigBean.mapRStreamsTargetConfig.singleMessagePerBatch"
    }
)
@GenerateResourceBundle
public class StatsMapRStreamsDTarget extends MapRStreamsDTarget {

  @Override
  protected Target createTarget() {
    KafkaTargetConfig kafkaTargetConfig = convertToKafkaConfigBean(maprStreamsTargetConfigBean);
    kafkaTargetConfig.dataFormat = DataFormat.SDC_JSON;
    return new KafkaTarget(kafkaTargetConfig);
  }

}
