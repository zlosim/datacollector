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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version = 3,
    label = "Kudu Lookup",
    description = "Performs KV lookups to enrich records",
    icon = "kudu.png",
    privateClassLoader = true,
    upgrader = KuduProcessorUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_b5b_dyl_p1b"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(
  "conf.cache.retryOnCacheMiss"
)
public class KuduLookupDProcessor extends DProcessor {
  @ConfigDefBean(groups = {"LOOKUP", "KUDU"})
  public KuduLookupConfig conf;

  @Override
  protected Processor createProcessor() {
    return new KuduLookupProcessor(conf);
  }
}
