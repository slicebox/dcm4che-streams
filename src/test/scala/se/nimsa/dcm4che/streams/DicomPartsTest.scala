package se.nimsa.dcm4che.streams

import org.scalatest.{FlatSpecLike, Matchers}
import DicomData._
import akka.util.ByteString
import se.nimsa.dcm4che.streams.DicomParts.{DicomAttribute, DicomHeader, DicomValueChunk}

class DicomPartsTest extends FlatSpecLike with Matchers {

  "DicomHeader" should "should return a new header with modified length for explicitVR, LE" in {
    val (tag, vr, headerLength, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, false).get
    val header = DicomHeader(tag, vr, length, false, false, true, patientNameJohnDoe.take(8))
    val updatedHeader = header.updateLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(6) shouldEqual header.bytes.take(6)
    updatedHeader.bytes(6) shouldEqual 5
    updatedHeader.bytes(7) shouldEqual 0
  }

  it should "should return a new header with modified length for explicitVR, BE" in {
    val (tag, vr, headerLength, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoeBE, true).get
    val header = DicomHeader(tag, vr, length, false, true, true, patientNameJohnDoeBE.take(8))
    val updatedHeader = header.updateLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(6) shouldEqual header.bytes.take(6)
    updatedHeader.bytes(6) shouldEqual 0
    updatedHeader.bytes(7) shouldEqual 5
  }

  it should "should return a new header with modified length for implicitVR, LE" in {
    val (tag, vr, headerLength, length) = DicomParsing.readHeaderImplicitVR(patientNameJohnDoeImplicit).get
    val header = DicomHeader(tag, vr, length, false, false, false, patientNameJohnDoeImplicit.take(8))
    val updatedHeader = header.updateLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(4) shouldEqual header.bytes.take(4)
    updatedHeader.bytes(4) shouldEqual 5
    updatedHeader.bytes(5) shouldEqual 0
  }


  "DicomAttribute" should "should return a new attribute with updated header and updated value" in {
    val (tag, vr, headerLength, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, false).get
    val header = DicomHeader(tag, vr, length, false, false, true, patientNameJohnDoe.take(8))
    val value = DicomValueChunk(false, patientNameJohnDoe.drop(8) ,true)
    val attribute = DicomAttribute(header, Seq(value))
    val updatedAttribute = attribute.updateStringValue("Jimmyboy^Doe")
    updatedAttribute.bytes.size shouldEqual 12
    updatedAttribute.header.length shouldEqual 12
  }

  it should "should return a new attribute with updated header and updated value with padding" in {
    val (tag, vr, headerLength, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, false).get
    val header = DicomHeader(tag, vr, length, false, false, true, patientNameJohnDoe.take(8))
    val value = DicomValueChunk(false, patientNameJohnDoe.drop(8) ,true)
    val attribute = DicomAttribute(header, Seq(value))
    val updatedAttribute = attribute.updateStringValue("Jimmy^Doe")

    updatedAttribute.bytes.size shouldEqual 10
    updatedAttribute.header.length shouldEqual 10
    updatedAttribute.bytes.drop(9) shouldEqual ByteString(32)
  }
}
