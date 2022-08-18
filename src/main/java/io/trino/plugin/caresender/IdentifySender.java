/*
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
package io.trino.plugin.caresender;

import com.google.common.collect.ImmutableMap;
import com.segment.analytics.messages.IdentifyMessage;

import static io.trino.plugin.caresender.TrinoSegmentClient.analytics;

public class IdentifySender
        implements Sender
{
    public void send(String userid, String eventName, ImmutableMap<String, Object> traits)
    {
        analytics.enqueue(IdentifyMessage.builder()
                .userId(userid)
                .traits(traits));
    }
}
