/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptPlanner, RelOptCost, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.plan.nodes.FlinkCalc
import org.apache.flink.api.table.typeutils.TypeConverter
import TypeConverter._
import org.apache.flink.api.table.BatchTableEnvironment
import org.apache.calcite.rex._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink RelNode which matches along with LogicalCalc.
  *
  */
class DataSetCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    private[flink] val calcProgram: RexProgram, // for tests
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with FlinkCalc
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetCalc(
      cluster,
      traitSet,
      inputs.get(0),
      getRowType,
      calcProgram,
      ruleDescription)
  }

  override def toString: String = calcToString(calcProgram, getExpressionString)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("select", selectionToString(calcProgram, getExpressionString))
      .itemIf("where",
        conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)

    // compute number of expressions that do not access a field or literal, i.e. computations,
    //   conditions, etc. We only want to account for computations, not for simple projections.
    val compCnt = calcProgram.getExprList.asScala.toList.count {
      case i: RexInputRef => false
      case l: RexLiteral => false
      case _ => true
    }

    planner.getCostFactory.makeCost(rowCnt, rowCnt * compCnt, 0)
  }

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)

    if (calcProgram.getCondition != null) {
      // we reduce the result card to push filters down
      (rowCnt * 0.75).min(1.0)
    } else {
      rowCnt
    }
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val generator = new CodeGenerator(config, false, inputDS.getType)

    val body = functionBody(
      generator,
      inputDS.getType,
      getRowType,
      calcProgram,
      config,
      expectedType)

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)

    import scala.collection.JavaConversions._

    def chooseForwardedFields(): String = {
      import scala.collection.JavaConversions._
      val modified = calcProgram.
        getExprList
        .filter(_.isInstanceOf[RexCall])
        .flatMap(_.asInstanceOf[RexCall].operands)
        .map(_.asInstanceOf[RexLocalRef].getIndex)
        .toSet


      //TODO check if it is needed
      def wrapIndex(v: Int) = {
        if(inputDS.getType.getTypeClass.getSimpleName == "Row") {
          if(expectedType.isEmpty || inputDS.getType.getTypeClass == expectedType.get.getTypeClass) {
            s"f$v"
          } else {
            s"f$v->_${v + 1}"
          }
        } else {
          if(expectedType.isEmpty || inputDS.getType.getTypeClass == expectedType.get.getTypeClass) {
            s"_$v"
          } else {
            s"_${v + 1}->f$v"
          }
        }
      }

      def wrapIndices(v1: Int, v2: Int) = {
        if (inputDS.getType.getTypeClass.getSimpleName == "Row") {
          if (expectedType.get.getTypeClass.getSimpleName == "Row") {
            s"f$v1->f$v2"
          } else {
            s"f$v1->_${v2 + 1}"
          }
        } else {
          if (expectedType.get.getTypeClass.getSimpleName == "Row") {
            s"_${v1 + 1}->f$v2"
          } else {
            s"_${v1 + 1}->_${v2 + 1}"
          }
        }
      }


      val template = (v: Int) => if(inputDS.getType.getTypeClass.getSimpleName == "Row") {
        s"f$v"
      } else {
        s"_${v + 1}"
      }


      calcProgram.getProjectList
        .map(e => (e.getName, e.getIndex))
        .zipWithIndex
        .map { case ((name, idx), pidx) => (name, idx, pidx) }
        .filterNot(a => modified.contains(a._2))
        .map {a =>
          if (a._2 == a._3) {
            println(wrapIndex(a._2))
            wrapIndex(a._2)
          } else {
            println(wrapIndices(a._2, a._3))
            wrapIndices(a._2, a._3)
          }
        }.mkString(";")
      ""
    }


    val inputTypes = this.calcProgram.getInputRowType
    val inputCount = this.calcProgram.getExprCount
    val inputFields: mutable.Buffer[RexInputRef] = this.calcProgram.getExprList.filter(_.isInstanceOf[RexInputRef]).map(_.asInstanceOf[RexInputRef])
    val rexCalls = this.calcProgram.getExprList.filter(_.isInstanceOf[RexCall]).map(_.asInstanceOf[RexCall])

    calcProgram.getProjectList

    calcProgram.getExprList.get(0).asInstanceOf[RexInputRef]

    println(
      s"""
         |Total input fields: $inputCount
         |Input types: $inputTypes
         |Input fields: ${inputFields.mkString(", ")}
         |Input Map: ${inputFields.map(e => (e.getName, e.getIndex))}
         |Rex calls: ${rexCalls.mkString(", ")}
         |Rex operands: ${rexCalls.map(_.operands).mkString(", ")}
         |Output types: ${calcProgram.getOutputRowType}
         |Project list: ${calcProgram.getProjectList}
         |Project Map: ${calcProgram.getProjectList.map(e => (e.getName, e.getIndex))}
       """.stripMargin)
    println(s"efficient: ${tableEnv.config.getEfficientTypeUsage}")
    val mapFunc = calcMapFunction(genFunction)
    val fields: String = chooseForwardedFields()
    println(fields)
    if(fields != "") {
      inputDS.flatMap(mapFunc).withForwardedFields(fields).name(calcOpName(calcProgram, getExpressionString))
    } else {
      inputDS.flatMap(mapFunc).name(calcOpName(calcProgram, getExpressionString))
    }
  }


}
