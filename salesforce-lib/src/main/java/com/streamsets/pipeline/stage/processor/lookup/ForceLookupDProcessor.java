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
package com.streamsets.pipeline.stage.processor.lookup;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.salesforce.ForceLookupConfigBean;

@StageDef(
    version = 2,
    label = "Salesforce Lookup",
    description = "Lookup records in Salesforce to enrich records",
    icon = "salesforce.png",
    upgrader = ForceLookupProcessorUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_fhn_yrk_yx"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs({
  "forceConfig.useCompression",
  "forceConfig.showTrace",
  "forceConfig.cacheConfig.retryOnCacheMiss"
})
public class ForceLookupDProcessor extends DProcessor {
  @ConfigDefBean
  public ForceLookupConfigBean forceConfig;

  @Override
  protected Processor createProcessor() {
    return new ForceLookupProcessor(forceConfig);
  }
}
