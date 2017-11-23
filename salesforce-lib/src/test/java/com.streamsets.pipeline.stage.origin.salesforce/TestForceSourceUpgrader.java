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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.salesforce.SubscriptionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestForceSourceUpgrader {
  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();

    ForceSourceUpgrader forceSourceUpgrader = new ForceSourceUpgrader();
    forceSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals("forceConfig.subscriptionType", config.getName());
    Assert.assertEquals(SubscriptionType.PUSH_TOPIC, config.getValue());
  }
}
