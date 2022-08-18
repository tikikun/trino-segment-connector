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
package io.trino.plugin.utils;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class RowUtils
{
    private RowUtils()
    {
    }

    public static ImmutableMap<String, Object> rowToImmutableMap(RowType rowType, Block rowData)
    {
        List<Type> typesList = rowType.getTypeParameters();
        List<RowType.Field> fieldsList = rowType.getFields();
        int rowSize = rowData.getPositionCount();
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        // Check not support case
        if (typesList.size() != fieldsList.size()) {
            throw new TrinoException(NOT_SUPPORTED, "You need to specify all key values pair using cast row");
        }
        //for (Type eachType : typesList) {
        //    if (!eachType.equals(VarcharType.VARCHAR)) {
        //        throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Currently only support VARCHAR, please convert all to VARCHAR");
        //    }
        //}
        // Run mapbuilder
        for (int position = 0; position < rowSize; position++) {
            Type type = typesList.get(position);
            String key = fieldsList.get(position).getName().get();
            if (type.equals(VARCHAR)) {
                mapBuilder.put(key,
                        type.getSlice(rowData, position).toStringUtf8());
            }
            else if (type.equals(BOOLEAN)) {
                mapBuilder.put(key,
                        typesList.get(position).getBoolean(rowData, position));
            }
            else if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
                mapBuilder.put(key,
                        typesList.get(position).getLong(rowData, position));
            }
            else if (type.equals(REAL) || type.equals(DOUBLE)) {
                mapBuilder.put(key,
                        typesList.get(position).getDouble(rowData, position));
            }
            else if (type.equals(DATE)) {
                mapBuilder.put(key,
                        LocalDate.ofEpochDay(typesList.get(position).getLong(rowData, position)).toString());
            }
            else if (type.equals(TIMESTAMP_MILLIS)) {
                mapBuilder.put(key,
                        Timestamp.from(Instant.ofEpochMilli(typesList.get(position).getLong(rowData, position) / 1000)).toString());
            }
            else if (isShortDecimal(type)) {
                int scale = ((DecimalType) type).getScale();
                mapBuilder.put(key,
                        BigDecimal.valueOf(type.getLong(rowData, position), scale));
            }
            else if (isLongDecimal(type)) {
                int scale = ((DecimalType) type).getScale();
                mapBuilder.put(key,
                        new BigDecimal(((Int128) type.getObject(rowData, position)).toBigInteger(), scale));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, "Argument of type " + type.getDisplayName() + " is not supported to send in Segment");
            }
        }
        return mapBuilder.buildOrThrow();
    }
}
