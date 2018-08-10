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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

public class MapRStreamsTargetConfigBean {

  public static final String MAPR_STREAMS_CONFIG_BEAN_PREFIX = "maprStreamsTargetConfigBean.";
  @ConfigDefBean(groups = {"MAPR_STREAMS"})
  public MapRStreamsTargetConfig mapRStreamsTargetConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SDC_JSON",
    label = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        KafkaDestinationGroups.KAFKA.name(),
        MAPR_STREAMS_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig",
        issues
    );
    mapRStreamsTargetConfig.init(context, dataFormat, issues);
  }


  public void destroy() {
    mapRStreamsTargetConfig.destroy();
  }
}
