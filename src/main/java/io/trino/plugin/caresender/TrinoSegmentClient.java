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

import com.segment.analytics.Analytics;
import com.segment.analytics.Log;

public class TrinoSegmentClient
{
    private TrinoSegmentClient()
    {
    }

    private static Log stdout = new Log()
    {
        @Override
        public void print(Level level, String format, Object... args)
        {
            System.out.println(level + ":\t" + String.format(format, args));
        }

        @Override
        public void print(Level level, Throwable error, String format, Object... args)
        {
            System.out.println(level + ":\t" + String.format(format, args));
            System.out.println(error);
        }
    };
    public static final Analytics analytics = Analytics.builder(System.getenv("SEGMENT_WRITEKEY")).log(stdout).build();
}
