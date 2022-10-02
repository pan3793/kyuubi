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

package org.apache.kyuubi.spark.connector.kudu.write

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite

import org.apache.kyuubi.spark.connector.kudu.KuduSchemaUtils

class KuduWriteBuilder(
  writeJob: KuduWriteJobContext
) extends WriteBuilder with Write {

  override def toBatch: BatchWrite = new KuduBatchWriterFactory(writeJob)

  override def toStreaming: StreamingWrite = super.toStreaming

  override def build(): Write = this
}

class KuduBatchWriterFactory(
  writeJob: KuduWriteJobContext
) extends BatchWrite with DataWriterFactory {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = this

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new KuduWriter(writeJob)
}

class KuduWriter(
    writeJob: KuduWriteJobContext) extends DataWriter[InternalRow] {

  private lazy val kuduSchema = KuduSchemaUtils.toKudu(writeJob.sparkSchema)

  override def write(record: InternalRow): Unit = {
    kuduSchema.newPartialRow()
  }

  override def commit(): WriterCommitMessage = KuduWriteMessage()

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
