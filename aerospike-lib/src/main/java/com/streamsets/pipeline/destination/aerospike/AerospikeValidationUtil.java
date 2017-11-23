package com.streamsets.pipeline.destination.aerospike;

import com.aerospike.client.Host;
import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Stage;

import java.util.ArrayList;
import java.util.List;

public class AerospikeValidationUtil {
  public List<Host> validateConnectionString(
      List<Stage.ConfigIssue> issues,
      String connectionString,
      String configGroupName,
      String configName,
      Stage.Context context
  ) {
    List<Host> clusterNodesList = new ArrayList<>();
    if (connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
          AerospikeErrors.AEROSPIKE_01, configName));
    } else {
      String[] nodes = connectionString.split(",");
      for (String node : nodes) {
        try {
          HostAndPort hostAndPort = HostAndPort.fromString(node);
          if(!hostAndPort.hasPort() || hostAndPort.getPort() < 0) {
            issues.add(context.createConfigIssue(configGroupName, configName, AerospikeErrors.AEROSPIKE_02, connectionString));
          } else {
            clusterNodesList.add(new Host(hostAndPort.getHostText(), hostAndPort.getPort()));
          }
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, AerospikeErrors.AEROSPIKE_02, connectionString));
        }
      }
    }
    return clusterNodesList;
  }
}
