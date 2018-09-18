/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.oss.driver.internal.core.channel;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.net.InetSocketAddress;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ProtocolInitHandlerIT {

  @Test(expected = AllNodesFailedException.class)
  public void should_generate_error_message_indicating_possible_wrong_contact_point()
      throws Exception {
    SessionBuilder builder =
        SessionUtils.baseBuilder()
            .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1", 0));
    try {
      builder.build();
    } catch (AllNodesFailedException ane) {
      String message = ane.getMessage();
      assertThat(message).contains("Connection refused, possible wrong contact points");
      throw ane;
    }
  }
}
