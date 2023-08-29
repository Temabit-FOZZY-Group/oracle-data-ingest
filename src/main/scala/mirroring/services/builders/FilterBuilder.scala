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

package mirroring.services.builders

import mirroring.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import wvlet.log.LogSupport

object FilterBuilder extends LogSupport {

  def buildJoinCondition(
      keys: Array[String],
      source: String,
      target: String
  ): String = {
    keys
      .map(x => s"$source.${x.trim} = $target.${x.trim}")
      .reduce(_ + " and " + _)
  }

  def buildMergeCondition(
      primaryKey: Array[String],
      ds: DataFrame,
      partitionCol: String = "",
      sourceColPrefix: String = "",
      targetColPrefix: String = ""
  ): String = {

    val maybePartitionColumn = if (partitionCol.isEmpty) None else Some(partitionCol)

    val condition = List(
      getFilterCondition(ds, maybePartitionColumn),
      Some(getPrimaryKeyCondition(primaryKey, sourceColPrefix, targetColPrefix))
    ).flatten
      .mkString(" and ")

    logger.info(s"Data will be merged using next condition: $condition")
    condition
  }

  private def getPrimaryKeyCondition(
      primaryKey: Array[String],
      sourceColPrefix: String,
      targetColPrefix: String
  ): String = {
    primaryKey
      .map(colName =>
        s"${ApplicationConfig.SourceAlias}.$sourceColPrefix${colName.trim} = ${ApplicationConfig.TargetAlias}.$targetColPrefix${colName.trim}"
      )
      .mkString(" and ")
  }

  private def getFilterCondition(
      ds: DataFrame,
      maybePartitionColumn: Option[String]
  ): Option[String] = {
    maybePartitionColumn.flatMap { partitionCol =>
      buildReplaceWherePredicate(ds, partitionCol)
        .map(_.replace(partitionCol, s"${ApplicationConfig.TargetAlias}.$partitionCol"))
    }
  }

  def buildStrWithoutSpecChars(
      in: String,
      replacement: String = "_"
  ): String = {
    in.replaceAll("\\W", replacement)
  }

  def buildReplaceWherePredicate(
      ds: DataFrame,
      partitionCol: String,
      whereClause: Option[String] = None
  ): Option[String] = {
    if (ds != null && partitionCol.nonEmpty) {
      val replaceWhere: StringBuilder = getWhereStatementBuilder(whereClause)

      val partitionColumnDS = ds
        .select(partitionCol)
        .distinct
        .na
        .drop()
        .as[String](Encoders.STRING)

      if (!partitionColumnDS.isEmpty) {
        replaceWhere.append(s"$partitionCol in (")
        replaceWhere.append(getWhereClauseValues(partitionColumnDS))
        replaceWhere.append(")")
      }
      Option(replaceWhere.toString)
    } else {
      None
    }
  }

  private def getWhereClauseValues(values: Dataset[String]): String = {

    val ds = values
      .map((partition: String) =>
        if (
          !partition
            .forall(_.isDigit) && partition != "true" && partition != "false"
        ) {
          s"'$partition'"
        } else {
          partition
        }
      )(Encoders.STRING)

    ds.collect().mkString(", ")
  }

  private def getWhereStatementBuilder(whereClause: Option[String]) = {
    whereClause
      .map(clause => new StringBuilder(s"$clause AND "))
      .getOrElse(new StringBuilder())
  }
}
