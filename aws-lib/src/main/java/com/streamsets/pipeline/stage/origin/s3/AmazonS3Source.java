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

package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;

import java.util.Map;

public interface AmazonS3Source {

  /**
   * Concrete classes must implement this method and handle the offset depending on the running thread.
   *
   * @param lastSourceOffset offset object received
   * @param context stage context
   * @return s3Object as String
   */
  Map<Integer, S3Offset> handleOffset(
      Map<String, String> lastSourceOffset, PushSource.Context context
  ) throws StageException;

  /**
   * Update one entry from offsets map
   *
   * @param runnerId to be updated
   */
  void updateOffset(Integer runnerId, S3Offset s3Offset);

  /**
   * Get offset associated to a given key
   *
   * @param runnerId for which we are looking for
   * @return offset associated
   */
  S3Offset getOffset(Integer runnerId);

  /**
   * Get latest offset saved
   *
   * @return get latest offset saved
   */
  S3Offset getLatestOffset();

  /**
   * Increments the counter for records
   */
  long incrementNoMoreDataRecordCount();

  /**
   * Increments the counter for error records
   */
  long incrementNoMoreDataErrorCount();

  /**
   * Increments the counter for files
   */
  long incrementNoMoreDataFileCount();

  /**
   * Send the NO_MORE_DATA_EVENT if all the threads have finished processing the ongoing event
   *
   * @param batchContext batch context of the runner to create the event
   * @return true if the event has been sent
   */
  boolean sendNoMoreDataEvent(BatchContext batchContext);

  /**
   * When a refill occurs the no-more-data event needs can be sent again
   */
  void restartNoMoreDataEvent();
}
