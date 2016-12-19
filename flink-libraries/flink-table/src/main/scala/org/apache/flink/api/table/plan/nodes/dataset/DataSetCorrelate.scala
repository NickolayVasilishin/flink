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
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLocalRef, RexNode}
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.BatchTableEnvironment
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.functions.utils.TableSqlFunction
import org.apache.flink.api.table.plan.nodes.FlinkCorrelate
import org.apache.flink.api.table.typeutils.TypeConverter._

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class DataSetCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    scan: LogicalTableFunctionScan,
    condition: Option[RexNode],
    relRowType: RelDataType,
    joinRowType: RelDataType,
    joinType: SemiJoinType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkCorrelate
  with DataSetRel {

  override def deriveRowType() = relRowType

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(getInput) * 1.5
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * 0.5)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetCorrelate(
      cluster,
      traitSet,
      inputs.get(0),
      scan,
      condition,
      relRowType,
      joinRowType,
      joinType,
      ruleDescription)
  }

  override def toString: String = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    correlateToString(rexCall, sqlFunction)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("function", sqlFunction.getTableFunction.getClass.getCanonicalName)
      .item("rowType", relRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]])
    : DataSet[Any] = {

    val config = tableEnv.getConfig
    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    // we do not need to specify input type
    val inputDS = inputNode.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val funcRel = scan.asInstanceOf[LogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    val pojoFieldMapping = sqlFunction.getPojoFieldMapping
    val udtfTypeInfo = sqlFunction.getRowTypeInfo.asInstanceOf[TypeInformation[Any]]

    val generator = new CodeGenerator(
      config,
      false,
      inputDS.getType,
      Some(udtfTypeInfo),
      None,
      Some(pojoFieldMapping))

    val body = functionBody(
      generator,
      udtfTypeInfo,
      getRowType,
      rexCall,
      condition,
      config,
      joinType,
      expectedType)

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)

    val mapFunc = correlateMapFunction(genFunction)


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
            if (composite.getFieldNames.toSet == rowType.getFieldNames.toSet) {
              compositeTypeField(rowType.getFieldNames)
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


      //TODO get all modifiedOperands, map them to fields
      //get indices of all modified operands
      val modifiedOperandsInRel = funcRel.getCall.asInstanceOf[RexCall].operands
        .map {
          _.asInstanceOf[RexInputRef].getIndex
        }
        .toSet
      //TODO do we need it?
      val joinCondition = if (condition.isDefined) {
        condition.get.asInstanceOf[RexCall].operands
          .map(_.asInstanceOf[RexInputRef].getIndex)
          .toSet
      } else {
        Set()
      }
      val modifiedOperands = modifiedOperandsInRel ++ joinCondition

      // get input/output indices of operands, filter modified operands and specify forwarding

      val tuples = inputDS.getType.asInstanceOf[CompositeType[_]].getFieldNames
        .zipWithIndex
        .map(_._2)
        .filterNot(modifiedOperands.contains)

      tuples.map(wrapIndex).mkString(";")
    }


//    val fields: String = "f0;f1"
    val fields: String = chooseForwardedFields()
    if (fields == "") {
      inputDS.flatMap(mapFunc).name(correlateOpName(rexCall, sqlFunction, relRowType))
    } else {
      inputDS.flatMap(mapFunc).withForwardedFields(fields).name(correlateOpName(rexCall, sqlFunction, relRowType))
    }
  }
}
