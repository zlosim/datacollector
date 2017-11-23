package com.streamsets.pipeline.destination.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class AerospikeTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(AerospikeTarget.class);
  private final AerospikeTargetConfig conf;
  private int retries = 1;
  private AerospikeClient client;
  private AerospikeValidationUtil validationUtil;


  public AerospikeTarget(AerospikeTargetConfig conf) {
    this.conf = conf;
    this.validationUtil = new AerospikeValidationUtil();
  }


  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    List<Host> hosts = validationUtil.validateConnectionString(issues, conf.connectionString, Groups.AEROSPIKE.getLabel(), "conf.connectionString", getContext());
    client = new AerospikeClient(new ClientPolicy(), (Host[]) hosts.toArray());
    while (!client.isConnected() && retries <= conf.maxRetries) {
      retries++;
    }
    if (retries > conf.maxRetries) {
      issues.add(getContext().createConfigIssue(Groups.AEROSPIKE.getLabel(), "conf.connectionString", AerospikeErrors.AEROSPIKE_03, conf.connectionString));
    }
    if (conf.binMappingConfigs.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.MAPPING.getLabel(), "conf.binMappingConfigs", AerospikeErrors.AEROSPIKE_05));
    } else {
      for (BinMappingConfig binMapping : conf.binMappingConfigs) {

        // Validate bin name
        if (binMapping.binExpr.isEmpty()) {
          issues.add(getContext().createConfigIssue())
        }

      }

    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();
    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        putToAerospike(record);

      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, AerospikeErrors.AEROSPIKE_04, e.toString());
            break;
          case STOP_PIPELINE:
            throw new StageException(AerospikeErrors.AEROSPIKE_04, e.toString());
          default:
            throw new IllegalStateException(
                Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    }

  }

  @Override
  public void destroy() {
    client.close();
    super.destroy();

  }

  private void putToAerospike(Record record) {
    conf.binMappingConfigs
    Key key = new Key(conf.namespaceExpr.)
  }


}
