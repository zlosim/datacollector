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
package com.streamsets.pipeline.stage.processor.httprouter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.ArrayList;
import java.util.List;

@StageDef(
    version = 1,
    label = "HTTP Router",
    description = "Passes records to streams based on HTTP Method and URL Path",
    icon="laneSelector.png",
    outputStreams = StageDef.VariableOutputStreams.class,
    outputStreamsDrivenByConfig = "routerLaneConfigs",
    onlineHelpRefUrl ="index.html?contextID=task_ny1_rrk_x2b",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EMR_BATCH
    }
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HttpRouterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "",
      displayPosition = 10,
      group = "ROUTER"
  )
  @ListBeanModel
  public List<HttpRouterLaneConfig> routerLaneConfigs = new ArrayList<>();

  @Override
  protected Processor createProcessor() {
    return new HttpRouterProcessor(routerLaneConfigs);
  }

}
