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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;

@StageDef(
  version = 4,
  label = "SDC RPC to Kafka",
  execution = ExecutionMode.STANDALONE,
  description = "Receives records via SDC RPC from a Data Collector pipeline that uses an SDC RPC destination and " +
    "writes them to Kafka",
  icon="sdcipctokafka.png",
  onlineHelpRefUrl ="index.html?contextID=task_il5_gtl_pw",
  upgrader = SdcIpcToKafkaUpgrader.class
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(
    value = {
        "conf.dataGeneratorFormatConfig.csvFileFormat",
        "conf.dataGeneratorFormatConfig.csvHeader",
        "conf.dataGeneratorFormatConfig.csvReplaceNewLines",
        "conf.dataGeneratorFormatConfig.csvReplaceNewLinesString",
        "conf.dataGeneratorFormatConfig.csvCustomDelimiter",
        "conf.dataGeneratorFormatConfig.csvCustomEscape",
        "conf.dataGeneratorFormatConfig.csvCustomQuote",
        "conf.dataGeneratorFormatConfig.jsonMode",
        "conf.dataGeneratorFormatConfig.textFieldPath",
        "conf.dataGeneratorFormatConfig.textEmptyLineIfNull",
        "conf.dataGeneratorFormatConfig.textRecordSeparator",
        "conf.dataGeneratorFormatConfig.avroSchemaSource",
        "conf.dataGeneratorFormatConfig.avroSchema",
        "conf.dataGeneratorFormatConfig.schemaRegistryUrls",
        "conf.dataGeneratorFormatConfig.schemaLookupMode",
        "conf.dataGeneratorFormatConfig.subject",
        "conf.dataGeneratorFormatConfig.subjectToRegister",
        "conf.dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration",
        "conf.dataGeneratorFormatConfig.registerSchema",
        "conf.dataGeneratorFormatConfig.schemaId",
        "conf.dataGeneratorFormatConfig.includeSchema",
        "conf.dataGeneratorFormatConfig.avroCompression",
        "conf.dataGeneratorFormatConfig.binaryFieldPath",
        "conf.dataGeneratorFormatConfig.protoDescriptorFile",
        "conf.dataGeneratorFormatConfig.messageType",
        "conf.dataGeneratorFormatConfig.fileNameEL",
        "conf.dataGeneratorFormatConfig.wholeFileExistsAction",
        "conf.dataGeneratorFormatConfig.includeChecksumInTheEvents",
        "conf.dataGeneratorFormatConfig.checksumAlgorithm",
        "conf.dataFormat",
        "conf.runtimeTopicResolution",
        "conf.partitionStrategy",
        "conf.partition",
        "conf.singleMessagePerBatch",
        "conf.topicExpression",
        "conf.topicWhiteList",
        "configs.tlsConfigBean.trustStoreFilePath",
        "configs.tlsConfigBean.trustStoreType",
        "configs.tlsConfigBean.trustStorePassword",
        "configs.tlsConfigBean.trustStoreAlgorithm"
    }
)
@Deprecated
public class SdcIpcToKafkaDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public SdcIpcConfigs configs;

  @ConfigDefBean()
  public KafkaTargetConfig conf;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "900",
    label = "Max Message Size (KB)",
    description = "Maximum size of the message written to Kafka. Configure in relationship to Max Batch Request Size and the maximum message size configured in Kafka. ",
    displayPosition = 30,
    group = "KAFKA",
    min = 1,
    max = 10000
  )
  public int kafkaMaxMessageSize;

  // The "topic" config from KafkaConfigBean depends on "runtimeTopicResolution" config which is hidden because
  // it does not make sense in this origin. As a result "topic" config will also not be shown to the user.
  // The only caveat is that the UI will not be able to pin point the topic configuration if it fails validation.
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 25,
    group = "KAFKA"
  )
  public String topic;

  @Override
  protected Source createSource() {
    conf.topic = topic;
    return new SdcIpcToKafkaSource(configs, conf, kafkaMaxMessageSize);
  }
}
