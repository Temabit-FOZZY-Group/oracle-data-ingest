/*
 * Copyright (2021) The Delta Flow Project Authors.
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

package mirroring.config

case class WriterContext(
    mode: String,
    path: String,
    partitionCols: Array[String],
    lastPartitionCol: String,
    mergeKeys: Array[String],
    primaryKey: Array[String],
    whereClause: String
)

object WriterContext {

  implicit class EnrichedWriterContext(writerContext: WriterContext) {

    private var _ctCurrentVersion: String = ""

    def ctCurrentVersion: String = _ctCurrentVersion

    def ctCurrentVersion_=(version: BigInt): Unit = _ctCurrentVersion = version.toString

  }
}
