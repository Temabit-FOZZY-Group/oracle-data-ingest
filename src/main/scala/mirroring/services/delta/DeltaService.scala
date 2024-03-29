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

package mirroring.services.delta

import mirroring.config.WriterContext
import mirroring.services.builders.FilterBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.LogSupport

class DeltaService(context: WriterContext) extends LogSupport {

  def write(data: DataFrame): Unit = {
    logger.info(s"Saving data to ${context.path}")
    dfWriter(data).save(context.path)
    logger.info(s"Saved data to ${context.path}")
  }

  def dfWriter(dataFrame: DataFrame): DataFrameWriter[Row] = {

    var writer = dataFrame.write
      .mode(context.mode)
      .format("delta")
      .option("mergeSchema", "true")
      .option("userMetadata", context.ctCurrentVersion)

    val whereClause =
      if (StringUtils.isBlank(context.whereClause)) None else Some(context.whereClause)
    val replaceWhere = FilterBuilder.buildReplaceWherePredicate(
      dataFrame,
      context.lastPartitionCol,
      whereClause
    )

    if (context.mode == "overwrite") {
      logger.info(s"Data matching next condition will be replaced: $replaceWhere")
      replaceWhere
        .foreach { where =>
          writer = writer.option("replaceWhere", where)
        }
    }

    if (context.partitionCols.nonEmpty) {
      logger.info("Saving data with partition")
      writer = writer
        .partitionBy(context.partitionCols: _*)
    }
    writer
  }
}
