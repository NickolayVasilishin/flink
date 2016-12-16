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

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.BatchTableEnvironment
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.plan.nodes.FlinkCalc
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter._

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

      val compositeTypeField = (fields: Seq[String]) => (v: Int) => fields(v)

      implicit def string2ForwardFields(left: String) = new AnyRef {
        def ->(right: String):String = left + "->" + right
        def simplify(): String = if (left.split("->").head == left.split("->").last) left.split("->").head else left
      }


      def chooseWrapper(typeInformation: Any): (Int) => String = {
        typeInformation match {
          case composite: CompositeType[_] => {
            //POJOs' fields are sorted, so we can not access them by their positional index. So we collect field names from
            //outputRowType. For all other types we get field names from inputDS.
            if (composite.getFieldNames.toSet == calcProgram.getOutputRowType.getFieldNames.toSet) {
              compositeTypeField(calcProgram.getOutputRowType.getFieldNames)
            } else {
              compositeTypeField(composite.getFieldNames)
            }
          }
          case basic: BasicTypeInfo[_] => (v: Int) => s"*"
        }
      }


      val wrapInput = chooseWrapper(inputDS.getType)
      val wrapOutput = chooseWrapper(returnType)

      //choose format of string depending on input/output types
      def wrapIndex(index: Int): String = {
        if (inputDS.getType.getClass == returnType.getClass) {
          wrapInput(index)
        } else {
          wrapInput(index) -> wrapOutput(index)
        }
      }

      //choose format of string depending on input/output types
      def wrapIndices(inputIndex: Int, outputIndex: Int): String = {
        wrapInput(inputIndex) -> wrapOutput(outputIndex) simplify()
      }

      //get indices of all modified operands
      val modifiedOperands = calcProgram.
        getExprList
        .filter(_.isInstanceOf[RexCall])
        .flatMap(_.asInstanceOf[RexCall].operands)
        .map(_.asInstanceOf[RexLocalRef].getIndex)
        .toSet

      // get input/output indices of operands, filter modified operands and specify forwarding
      val tuples = calcProgram.getProjectList
        .map(ref => (ref.getName, ref.getIndex))
        .zipWithIndex
        .map { case ((name, inputIndex), projectIndex) => (name, inputIndex, projectIndex) }
        //consider only input fields
        .filter(_._2 < calcProgram.getExprList.filter(_.isInstanceOf[RexInputRef]).map(_.asInstanceOf[RexInputRef]).size)
        .filterNot(ref => modifiedOperands.contains(ref._2))

        tuples.map {ref =>
          if (ref._2 == ref._3) {
            wrapIndex(ref._2)
          } else {
            wrapIndices(ref._2, ref._3)
          }
        }.mkString(";")
    }
//
//    def printInfo: Unit = {
//      val inputTypes = this.calcProgram.getInputRowType
//      val inputCount = this.calcProgram.getExprCount
//      val inputFields: mutable.Buffer[RexInputRef] = this.calcProgram.getExprList.filter(_.isInstanceOf[RexInputRef]).map(_.asInstanceOf[RexInputRef])
//      val rexCalls = this.calcProgram.getExprList.filter(_.isInstanceOf[RexCall]).map(_.asInstanceOf[RexCall])
//
//      println(
//        s"""
//           |Total fields: $inputCount
//
//           |Input types: $inputTypes
//           |Input fields: ${inputFields.mkString(", ")}
//           |Input Map: ${inputFields.map(e => (e.getName, e.getIndex))}
//           |Rex calls: ${rexCalls.mkString(", ")}
//           |Rex operands: ${rexCalls.map(_.operands).mkString(", ")}
//           |Output types: ${calcProgram.getOutputRowType}
//           |Project list: ${calcProgram.getProjectList}
//           |Project Map: ${calcProgram.getProjectList.map(e => (e.getName, e.getIndex))}
//           |Efficient: ${tableEnv.config.getEfficientTypeUsage}
//        """.stripMargin)
//    }
//    printInfo

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
