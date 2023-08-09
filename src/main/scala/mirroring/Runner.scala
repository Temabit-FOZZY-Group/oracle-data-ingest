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

package mirroring

import org.apache.spark.sql.DataFrame
import mirroring.builders.{ConfigBuilder, DataframeBuilder, FilterBuilder}
import mirroring.services.databases.{JdbcService}
import mirroring.services.writer.{MergeService, DeltaService, WriterContext}
import mirroring.services.{SparkService, SqlService, DeltaTableService}
import wvlet.log.LogSupport

object Runner extends LogSupport {

  def initConfig(args: Array[String]): Config = {

    for (x <- args) {
      logger.info(s"Parameter: ${x}")
    }

    //logger.info(s"Parameters parsed: ${args}")
    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    config
  }

  def setSparkContext(config: Config): Unit = {
    val spark = SparkService.spark
    logger.info(
      s"""Creating spark session with configurations: ${spark.conf.getAll
        .mkString(", ")}"""
    )
    spark.sparkContext.setLogLevel(config.logSparkLvl)
    spark.conf.set("spark.sql.session.timeZone", config.timezone)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting mirroring-lib...")
    val config: Config = initConfig(args)
    setSparkContext(config)
    val jdbcContext                  = config.getJdbcContext
    val writerContext: WriterContext = config.getWriterContext
    val query: String                = config.query

    val jdbcService: JdbcService = new JdbcService(jdbcContext)

    val jdbcDF: DataFrame = jdbcService.loadData(query)
    val ds                = DataframeBuilder.buildDataFrame(jdbcDF, config.getDataframeBuilderContext)
    jdbcDF.unpersist()

    val writerService: DeltaService = new DeltaService(writerContext)
    writerService.write(data = ds)
    deltaPostProcessing(config, ds)
  }

  private def deltaPostProcessing(config: Config, ds: DataFrame): Unit = {
    if (config.zorderby_col.nonEmpty) {
      val replaceWhere =
        FilterBuilder.buildReplaceWherePredicate(
          ds,
          config.lastPartitionCol
        ).getOrElse("1=1")

      DeltaTableService.executeZOrdering(
        config.pathToSave,
        config.zorderby_col,
        replaceWhere
      )
    }
    DeltaTableService.runVacuum(config.pathToSave)
    if (config.hiveDb.nonEmpty) {
      SqlService.run(config)
    }
  }
}
