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

import io.delta.tables.DeltaTable
import mirroring.config.{ApplicationConfig, WriterContext}
import mirroring.services.SparkService.spark
import mirroring.services.builders.FilterBuilder
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.LogSupport

class MergeDeltaService(context: WriterContext) extends DeltaService(context) with LogSupport {

  override def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, context.path)) {
      logger.info("Target table already exists. Merging data...")

      DeltaTable
        .forPath(spark, context.path)
        .as(ApplicationConfig.TargetAlias)
        .merge(
          data.as(ApplicationConfig.SourceAlias),
          FilterBuilder.buildMergeCondition(
            context.mergeKeys,
            data,
            context.lastPartitionCol
          )
        )
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    } else {
      logger.info("Target table doesn't exist yet. Initializing table...")
      super.write(data)
    }
  }

}
