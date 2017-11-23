package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.configurablestage.DTarget;


@StageDef(
    version = 2,
    label = "Aerospike",
    description = "Writes data to Aerospike",
    icon = "aerospike.png",
    upgrader = AerospikeTargetUpgrader.class,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class AerospikeDTarget extends DTarget {

  @ConfigDefBean(groups = {"AEROSPIKE"})
  public AerospikeTargetConfig conf;

  @Override
  protected Target createTarget() {
    return new AerospikeTarget(this.conf);
  }
}
