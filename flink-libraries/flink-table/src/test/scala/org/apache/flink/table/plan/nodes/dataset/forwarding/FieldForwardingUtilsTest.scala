package org.apache.flink.table.plan.nodes.dataset.forwarding

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfoTest, RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.junit.Test
import org.apache.flink.table.plan.nodes.dataset.forwarding.FieldForwardingUtils._

class FieldForwardingUtilsTest {

  @Test
  def test() = {
    val typeInfo1: RowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val info1: TypeInformation[PojoTypeInfoTest.TestPojo] = TypeExtractor.getForClass(classOf[PojoTypeInfoTest.TestPojo])

    println(getForwardedInput(typeInfo1, info1, Seq(0, 1)))
  }

  final class TestPojo {
    var someInt: Int = 0
    private var aString: String = null
    var doubleArray: Array[Double] = null

    def setaString(aString: String) {
      this.aString = aString
    }

    def getaString: String = aString
  }
}
