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
package com.streamsets.datacollector.cli.sch;

import io.airlift.airline.Command;

@Command(name = "unregister", description = "Unregister this Data Collector from Control Hub.")
public class UnregisterCommand extends AbstractCommand {
  @Override
  protected void executeAction() throws Exception {
    SchAdmin.disableDPM(
      getUserID(),
      getUserPassword(),
      getOrganization(),
      new SchAdmin.Context(getRuntimeInfo(), getConfiguration(), isSkipConfigUpdate(), getTokenFilePath())
    );
  }
}
