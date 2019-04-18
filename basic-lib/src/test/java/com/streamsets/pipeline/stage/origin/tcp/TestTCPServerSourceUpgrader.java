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
package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestTCPServerSourceUpgrader {

  @Test
  public void testV1ToV2() throws Exception {
    List<Config> configs = new LinkedList<>();
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 1, 2, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.lengthFieldCharset");
  }

  @Test
  public void testV2ToV3() throws Exception {
    List<Config> configs = new LinkedList<>();
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");

    configs.clear();
    upgrader.upgrade("lib", "stage", "stageInst", 1, 3, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.lengthFieldCharset");
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new LinkedList<>();
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();

    // test empty configs since version 2
    upgrader.upgrade("lib", "stage", "stageInst", 2, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 300);

    // test good value near max permitted value
    configs.clear();
    configs.add(new Config("conf.readTimeout", 3599));
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 3599);

    // test good value being min permitted value
    configs.clear();
    configs.add(new Config("conf.readTimeout", 1));
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 1);

    // test incorrect value 0
    configs.clear();
    configs.add(new Config("conf.readTimeout", 0));
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 3600);

    // test incorrect negative value
    configs.clear();
    configs.add(new Config("conf.readTimeout", -1));
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 3600);

    // test incorrect value bigger than max value
    configs.clear();
    configs.add(new Config("conf.readTimeout", 3800));
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(configs.size(), 1);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
    UpgraderTestUtils.assertExists(configs, "conf.readTimeout", 3600);
  }

}
