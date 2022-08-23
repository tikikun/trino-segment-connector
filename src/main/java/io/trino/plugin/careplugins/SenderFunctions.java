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
package io.trino.plugin.careplugins;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.caresender.IdentifySender;
import io.trino.plugin.caresender.TrackSender;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;

import static io.trino.plugin.utils.RowUtils.rowToImmutableMap;

public class SenderFunctions
{
    private SenderFunctions()
    {
    }

    @ScalarFunction("care_sendTrack")
    @TypeParameter("V")
    @Description("This will send track event to Segment")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice care_sendTrack(@TypeParameter("V") RowType rowType,
                                       @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice userId,
                                       @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice eventName,
                                       @SqlNullable @SqlType("V") Block propertiesRowData)
    {
        TrackSender trackSender = new TrackSender();
        trackSender.send(userId.toStringUtf8(),
                eventName.toStringUtf8(),
                rowToImmutableMap(rowType, propertiesRowData));
        return Slices.utf8Slice("Sent the data");
    }

    @ScalarFunction("care_sendIdentify")
    @TypeParameter("V")
    @Description("This will send identify event to Segment")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice care_sendIdentify(@TypeParameter("V") RowType rowType,
                                          @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice userId,
                                          @SqlNullable @SqlType("V") Block traitsRowData)
    {
        IdentifySender identifySender = new IdentifySender();
        identifySender.send(userId.toStringUtf8(),
                rowToImmutableMap(rowType, traitsRowData));
        return Slices.utf8Slice("Sent the data");
    }
}
