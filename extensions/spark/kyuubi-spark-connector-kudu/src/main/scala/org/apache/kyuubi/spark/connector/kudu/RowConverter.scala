/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.spark.connector.kudu

import org.apache.kudu.{Schema, Type}
import org.apache.kudu.client.{PartialRow, RowResult}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.types.DataTypes._

class RowConverter(kuduSchema: Schema, sparkSchema: StructType, ignoreNull: Boolean) {

  private val typeConverter = CatalystTypeConverters.createToScalaConverter(sparkSchema)

  private val indices: Array[(Int, Int)] = sparkSchema.fields.zipWithIndex.flatMap {
    case (field, sparkIdx) =>
      // Support Spark schemas that have more columns than the Kudu table by
      // ignoring missing Kudu columns.
      if (kuduSchema.hasColumn(field.name)) {
        Some(sparkIdx -> kuduSchema.getColumnIndex(field.name))
      } else None
  }

  /**
   * Converts a Spark internalRow to a Spark Row.
   */
  def toRow(internalRow: InternalRow): Row = {
    typeConverter(internalRow).asInstanceOf[Row]
  }

  /**
   * Converts a Spark row to a Kudu PartialRow.
   */
  def toPartialRow(row: Row): PartialRow = {
    val partialRow = kuduSchema.newPartialRow()
    for ((sparkIdx, kuduIdx) <- indices) {
      if (row.isNullAt(sparkIdx)) {
        if (kuduSchema.getColumnByIndex(kuduIdx).isKey) {
          val keyName = kuduSchema.getColumnByIndex(kuduIdx).getName
          throw new IllegalArgumentException(s"Can't set primary key column '$keyName' to null")
        }
        if (!ignoreNull) partialRow.setNull(kuduIdx)
      } else {
        sparkSchema.fields(sparkIdx).dataType match {
          case StringType =>
            kuduSchema.getColumnByIndex(kuduIdx).getType match {
              case Type.STRING =>
                partialRow.addString(kuduIdx, row.getString(sparkIdx))
              case Type.VARCHAR =>
                partialRow.addVarchar(kuduIdx, row.getString(sparkIdx))
              case t =>
                throw new IllegalArgumentException(s"Invalid Kudu column type $t")
            }
          case BinaryType =>
            partialRow.addBinary(kuduIdx, row.getAs[Array[Byte]](sparkIdx))
          case BooleanType =>
            partialRow.addBoolean(kuduIdx, row.getBoolean(sparkIdx))
          case ByteType =>
            partialRow.addByte(kuduIdx, row.getByte(sparkIdx))
          case ShortType =>
            partialRow.addShort(kuduIdx, row.getShort(sparkIdx))
          case IntegerType =>
            partialRow.addInt(kuduIdx, row.getInt(sparkIdx))
          case LongType =>
            partialRow.addLong(kuduIdx, row.getLong(sparkIdx))
          case FloatType =>
            partialRow.addFloat(kuduIdx, row.getFloat(sparkIdx))
          case DoubleType =>
            partialRow.addDouble(kuduIdx, row.getDouble(sparkIdx))
          case TimestampType =>
            partialRow.addTimestamp(kuduIdx, row.getTimestamp(sparkIdx))
          case DateType =>
            partialRow.addDate(kuduIdx, row.getDate(sparkIdx))
          case DecimalType() =>
            partialRow.addDecimal(kuduIdx, row.getDecimal(sparkIdx))
          case t =>
            throw new IllegalArgumentException(s"No support for Spark SQL type $t")
        }
      }
    }
    partialRow
  }

  /**
   * Converts a Kudu RowResult to a Spark row.
   */
  def toRow(rowResult: RowResult): Row = {
    val columnCount = rowResult.getColumnProjection.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = rowResult.getObject(i)
    }
    new GenericRowWithSchema(columns, sparkSchema)
  }

  /**
   * Converts a Kudu PartialRow to a Spark row.
   */
  def toRow(partialRow: PartialRow): Row = {
    val columnCount = partialRow.getSchema.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = partialRow.getObject(i)
    }
    new GenericRowWithSchema(columns, sparkSchema)
  }
}
