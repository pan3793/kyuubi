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

import java.util

import scala.collection.JavaConverters._

import org.apache.kudu.{ColumnSchema, ColumnTypeAttributes, Schema, Type}
import org.apache.kudu.ColumnTypeAttributes.ColumnTypeAttributesBuilder
import org.apache.kudu.Type._
import org.apache.spark.sql.types._

object KuduSchemaUtils {

  /**
   * Converts a Kudu [[Type]] to a Spark SQL [[DataType]].
   *
   * @param kuduType the Kudu type
   * @param attr the Kudu type attributes
   * @return the corresponding Spark SQL type
   */
  def toSpark(kuduType: Type, attr: ColumnTypeAttributes): DataType = kuduType match {
    case BOOL => BooleanType
    case INT8 => ByteType
    case INT16 => ShortType
    case INT32 => IntegerType
    case INT64 => LongType
    case UNIXTIME_MICROS => TimestampType
    case DATE => DateType
    case FLOAT => FloatType
    case DOUBLE => DoubleType
    case VARCHAR => VarcharType(attr.getLength)
    case STRING => StringType
    case BINARY => BinaryType
    case DECIMAL => DecimalType(attr.getPrecision, attr.getScale)
  }

  /**
   * Converts a Spark SQL [[DataType]] to a Kudu [[Type]].
   *
   * @param sparkType the Spark SQL type
   * @return
   */
  def toKudu(sparkType: DataType): Type = sparkType match {
    case BinaryType => BINARY
    case BooleanType => BOOL
    case StringType => STRING
    case VarcharType(_) => VARCHAR
    case TimestampType => UNIXTIME_MICROS
    case DateType => DATE
    case ByteType => INT8
    case ShortType => INT16
    case IntegerType => INT32
    case LongType => INT64
    case FloatType => FLOAT
    case DoubleType => DOUBLE
    case DecimalType() => DECIMAL
    case _ => throw new IllegalArgumentException(s"No support for Spark SQL type $sparkType")
  }

  /**
   * Generates a SparkSQL schema from a Kudu schema.
   *
   * @param kuduSchema the Kudu schema
   * @param fields an optional column projection
   * @return the SparkSQL schema
   */
  def toSpark(kuduSchema: Schema, fields: Option[Seq[String]] = None): StructType = {
    val kuduColumns = fields match {
      case Some(fieldNames) => fieldNames.map(kuduSchema.getColumn)
      case None => kuduSchema.getColumns.asScala
    }
    val sparkColumns = kuduColumns.map { col =>
      val sparkType = toSpark(col.getType, col.getTypeAttributes)
      StructField(col.getName, sparkType, col.isNullable)
    }
    StructType(sparkColumns)
  }

  /**
   * Generates a Kudu schema from a SparkSQL schema.
   *
   * @param sparkSchema the SparkSQL schema
   * @param keys the ordered names of key columns
   * @return the Kudu schema
   */
  def toKudu(sparkSchema: StructType, keys: Seq[String]): Schema = {
    val kuduCols = new util.ArrayList[ColumnSchema]()
    // add the key columns first, in the order specified
    for (key <- keys) {
      val field = sparkSchema.fields(sparkSchema.fieldIndex(key))
      val col = createColumnSchema(field, isKey = true)
      kuduCols.add(col)
    }
    // now add the non-key columns
    for (field <- sparkSchema.fields.filter(field => !keys.contains(field.name))) {
      val col = createColumnSchema(field, isKey = false)
      kuduCols.add(col)
    }
    new Schema(kuduCols)
  }

  /**
   * Generates a Kudu column schema from a SparkSQL field.
   *
   * @param field the SparkSQL field
   * @param isKey true if the column is a key
   * @return the Kudu column schema
   */
  private def createColumnSchema(field: StructField, isKey: Boolean): ColumnSchema = {
    val kuduType = toKudu(field.dataType)
    val col = new ColumnSchema.ColumnSchemaBuilder(field.name, kuduType)
      .key(isKey)
      .nullable(field.nullable)
    // Add ColumnTypeAttributesBuilder to DECIMAL columns
    if (kuduType == DECIMAL) {
      val dt = field.dataType.asInstanceOf[DecimalType]
      col.typeAttributes(
        new ColumnTypeAttributesBuilder()
          .precision(dt.precision)
          .scale(dt.scale)
          .build())
    }
    col.build()
  }

}
