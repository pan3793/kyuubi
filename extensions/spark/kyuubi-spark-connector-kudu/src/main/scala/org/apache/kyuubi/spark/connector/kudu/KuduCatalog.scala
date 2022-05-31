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

import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KuduCatalog extends TableCatalog {

  var options: CaseInsensitiveStringMap = _

  var kuduClient: KuduClient = _

  private var _name: String = _

  override def name: String = _name

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this._name = name
    this.options = options

    val builder = new KuduClient.KuduClientBuilder(options.get("masterAddresses"))
    builder.defaultAdminOperationTimeoutMs(options.getLong("defaultAdminOperationTimeout", 30000L))
    builder.defaultOperationTimeoutMs(options.getLong("defaultOperationTimeout", 30000L))
    if (options.getBoolean("isDisableStatistics", false)) builder.disableStatistics
    this.kuduClient = builder.build()
  }

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array() =>
      kuduClient.getTablesList.getTablesList.asScala.map(Identifier.of(Array(), _)).toArray
    case _ => throw new NoSuchNamespaceException(namespace)
  }

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident)
    }
    val kuduTable = kuduClient.openTable(ident.name)
    new KuduSparkTable(kuduTable)
  }

  override def tableExists(ident: Identifier): Boolean = ident.namespace match {
    case Array() => kuduClient.tableExists(ident.name)
    case _ => false
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException
}
