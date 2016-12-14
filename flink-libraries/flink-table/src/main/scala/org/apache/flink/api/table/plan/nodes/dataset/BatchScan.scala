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

import org.apache.calcite.plan._
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLocalRef}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.{Row, TableConfig}
import org.apache.flink.api.table.plan.schema.FlinkTable
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter.determineReturnType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class BatchScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with DataSetRel {

  override def toString: String = {
    s"Source(from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")}))"
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  protected def convertToExpectedType(
      input: DataSet[Any],
      flinkTable: FlinkTable[_],
      expectedType: Option[TypeInformation[Any]],
      config: TableConfig): DataSet[Any] = {

    val inputType = input.getType



//    import scala.collection.JavaConversions._
//    def chooseForwardedFields(): String = {
//
//      val f = (v: Int) => s"f$v"
//      val `_` = (v: Int) => s"_${v + 1}"
//      implicit def string2ForwardFields(left: String) = new AnyRef {
//        def ->(right: String):String = left + "->" + right
//      }
//
//      //choose format of string depending on input/output types
//      def wrapIndex(index: Int): String = {
//        if(inputDS.getType.getTypeClass == classOf[Row]) {
//          if(inputDS.getType.getTypeClass == returnType.getTypeClass) {
//            f(index)
//          } else {
//            f(index) -> `_`(index)
//          }
//        } else {
//          if(inputDS.getType.getTypeClass == returnType.getTypeClass) {
//            `_`(index)
//          } else {
//            `_`(index) -> f(index)
//          }
//        }
//      }
//
//      //choose format of string depending on input/output types
//      def wrapIndices(inputIndex: Int, outputIndex: Int): String = {
//        if (inputDS.getType.getTypeClass == classOf[Row]) {
//          if (returnType.getTypeClass == classOf[Row]) {
//            f(inputIndex) -> f(outputIndex)
//          } else {
//            f(inputIndex) -> `_`(outputIndex)
//          }
//        } else {
//          if (returnType.getTypeClass == classOf[Row]) {
//            `_`(inputIndex) -> f(outputIndex)
//          } else {
//            `_`(inputIndex) -> `_`(outputIndex)
//          }
//        }
//      }
//
//      //get indices of all modified operands
//      val modified = calcProgram.
//        getExprList
//        .filter(_.isInstanceOf[RexCall])
//        .flatMap(_.asInstanceOf[RexCall].operands)
//        .map(_.asInstanceOf[RexLocalRef].getIndex)
//        .toSet
//
//      // get input/output indices of operands, filter modified operands and specify forwarding
//      calcProgram.getProjectList
//        .map(ref => (ref.getName, ref.getIndex))
//        .zipWithIndex
//        .map { case ((name, inputIndex), projectIndex) => (name, inputIndex, projectIndex) }
//        .filterNot(ref => modified.contains(ref._2))
//        .map {ref =>
//          if (ref._2 == ref._3) {
//            println(wrapIndex(ref._2))
//            wrapIndex(ref._2)
//          } else {
//            println(wrapIndices(ref._2, ref._3))
//            wrapIndices(ref._2, ref._3)
//          }
//        }.mkString(";")
//    }



    expectedType match {

      // special case:
      // if efficient type usage is enabled and no expected type is set
      // we can simply forward the DataSet to the next operator.
      // however, we cannot forward PojoTypes as their fields don't have an order
      case None if config.getEfficientTypeUsage && !inputType.isInstanceOf[PojoTypeInfo[_]] =>
        input

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {

          val mapFunc = getConversionMapper(
            config,
            nullableInput = false,
            inputType,
            determinedType,
            "DataSetSourceConversion",
            getRowType.getFieldNames,
            Some(flinkTable.fieldIndexes))

          val opName = s"from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

          val f = (v: Int) => s"f$v"
          val `_` = (v: Int) => s"_${v + 1}"
          implicit def string2ForwardFields(left: String) = new AnyRef {
            def ->(right: String):String = left + "->" + right
          }

          val rowTypeField = (v: Int) => s"f$v"
          //      val `_` = (v: Int) => s"_${v + 1}"
          val compositeTypeField = (fields: Seq[String]) => (v: Int) => fields(v)
          //      val caseClassField = (v: Int) => ""

          def chooseWrapper(typeInformation: TypeInformation[Any]): (Int) => String = {
            typeInformation match {
              case row if row.isInstanceOf[RowTypeInfo] => rowTypeField
              case pojo: PojoTypeInfo[_] => compositeTypeField(pojo.getFieldNames.toSeq)
              case caseClass: CaseClassTypeInfo[_] => compositeTypeField(caseClass.getFieldNames.toSeq)
              //TODO why
              case basic: BasicTypeInfo[_] => (v: Int) => s"*"
            }
          }

          val wrapInput = chooseWrapper(inputType)
          val wrapOutput = chooseWrapper(determinedType)

          def wrapIndex(index: Int): String = {
            if (inputType.getClass == determinedType.getClass) {
              wrapInput(index)
            } else {
              wrapInput(index) -> wrapOutput(index)
            }
          }

//          "_1->f0;_2->f1;_3->f2"
          input.map(mapFunc).withForwardedFields(flinkTable.fieldIndexes.map(wrapIndex).mkString(";")).name(opName)
        }
        // no conversion necessary, forward
        else {
          input
        }
    }
  }
}
