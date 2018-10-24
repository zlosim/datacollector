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

import com.streamsets.datacollector.config.AmazonEMRConfig;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisBaseUpgrader;

import java.util.List;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;

public class FirehoseTargetUpgrader extends KinesisBaseUpgrader {

  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        // fall through
      case 2:
        upgradeV2toV3(configs);
        // fall through
      case 3:
        upgradeV3toV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV3toV4(List<Config> configs) {
    String regionProperty = KINESIS_CONFIG_BEAN + ".region";
    for (int i = 0; i < configs.size(); i++) {
      if (configs.get(i).getName().equals(regionProperty)) {
        if ("GovCloud".equals(configs.get(i).getValue())) {
          configs.set(i, new Config(regionProperty, AwsRegion.US_GOV_WEST_1.name()));
        }
      }
    }
  }

  private static void upgradeV2toV3(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV1toV2(List<Config> configs) {
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".endpoint", ""));
  }
}
