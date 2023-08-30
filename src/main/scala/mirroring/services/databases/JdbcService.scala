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

package mirroring.services.databases

import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoders}
import mirroring.DatatypeMapping
import mirroring.config.JdbcContext
import mirroring.services.SparkService.spark
import wvlet.log.LogSupport

import scala.collection.mutable

class JdbcService(jdbcContext: JdbcContext) extends LogSupport {

  private val db_user: String     = sys.env.getOrElse("DB_USER", "")
  private val db_password: String = sys.env.getOrElse("DB_PASSWORD", "")

  def loadData(query: String): DataFrame = {
    logger.info(s"Reading data with url: ${jdbcContext.url}")
    logger.info(s"Reading data with query: $query")

    val jdbcDF = getSparkReader
      .option("dbtable", query)
      .option("fetchsize", jdbcContext.fetchSize)
      .load()
      .repartition(jdbcContext.partitionsNumber)
      .cache()

    jdbcDF
  }

  private def getSparkReader: DataFrameReader = {
    spark.read
      .format("jdbc")
      .option("url", jdbcContext.url)
      .option("user", sys.env.getOrElse("DB_USER", ""))
      .option("password", sys.env.getOrElse("DB_PASSWORD", ""))
      .option("driver", "oracle.jdbc.driver.OracleDriver")
    //.option("customSchema", getCustomSchema())
  }

  private def getCustomSchema(): String = {
    val sql =
      s"(select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where " +
        s"TABLE_NAME = '${jdbcContext.table}' and TABLE_SCHEMA = '${jdbcContext.schema}') subq"

    val sourceSchema = spark.read
      .format("jdbc")
      .option("url", getUrl)
      .option("dbtable", sql)
      .load()

    // create custom schema to avoid transferring DATE as STRING
    // viz https://jtds.sourceforge.net/typemap.html
    try {
      val customSchema = sourceSchema
        .map(row => {
          if (DatatypeMapping.contains(row.getString(1))) {
            f"${row.getString(0)} ${DatatypeMapping.getDatatype(row.getString(1))}"
          } else {
            ""
          }
        })(Encoders.STRING)
        .filter(row => row.nonEmpty)
        .reduce(_ + ", " + _)

      logger.info(s"Reading data with customSchema: $customSchema")
      customSchema
    } catch {
      case e: java.lang.UnsupportedOperationException
          if e.getMessage.contains("empty collection") =>
        logger.info(s"No custom schema will be used")
        ""
    }
  }

  private def getUrl: String = {
    // If user/password are passed through environment variables, extract them and append to the url
    val sb = new mutable.StringBuilder(jdbcContext.url)
    if (
      !jdbcContext.url.contains("user") && !jdbcContext.url.contains(
        "password"
      ) && db_user.nonEmpty && db_password.nonEmpty
    ) {
      if (!jdbcContext.url.endsWith(";")) {
        sb.append(";")
      }
      sb.append(s"user=$db_user;password=$db_password")
    }

    require(
      sb.toString.contains("password="),
      "Parameters user and password are required for jdbc connection."
    )

    sb.toString
  }

}
