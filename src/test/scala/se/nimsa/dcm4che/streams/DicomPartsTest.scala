package se.nimsa.dcm4che.streams

import org.scalatest.{FlatSpecLike, Matchers}
import DicomData._
import akka.util.ByteString
import se.nimsa.dcm4che.streams.DicomParts.{DicomAttribute, DicomHeader, DicomValueChunk}

class DicomPartsTest extends FlatSpecLike with Matchers {

  "DicomHeader" should "should return a new header with modified length for explicitVR, LE" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val header = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = true, patientNameJohnDoe.take(8))
    val updatedHeader = header.withUpdatedLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(6) shouldEqual header.bytes.take(6)
    updatedHeader.bytes(6) shouldEqual 5
    updatedHeader.bytes(7) shouldEqual 0
  }

  it should "should return a new header with modified length for explicitVR, BE" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoeBE, assumeBigEndian = true).get
    val header = DicomHeader(tag, vr, length, isFmi = false, bigEndian = true, explicitVR = true, patientNameJohnDoeBE.take(8))
    val updatedHeader = header.withUpdatedLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(6) shouldEqual header.bytes.take(6)
    updatedHeader.bytes(6) shouldEqual 0
    updatedHeader.bytes(7) shouldEqual 5
  }

  it should "should return a new header with modified length for implicitVR, LE" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderImplicitVR(patientNameJohnDoeImplicit).get
    val header = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = false, patientNameJohnDoeImplicit.take(8))
    val updatedHeader = header.withUpdatedLength(5)

    updatedHeader.length shouldEqual 5
    updatedHeader.bytes.take(4) shouldEqual header.bytes.take(4)
    updatedHeader.bytes(4) shouldEqual 5
    updatedHeader.bytes(5) shouldEqual 0
  }

  it should "create a valid explicit VR little endian byte sequence representation when constructed without explicit bytes" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val bytes = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = true).bytes
    bytes shouldBe patientNameJohnDoe.take(8)
  }

  it should "create a valid implicit VR little endian byte sequence representation when constructed without explicit bytes" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val bytes = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = false).bytes
    bytes shouldBe patientNameJohnDoeImplicit.take(8)
  }

  it should "create a valid explicit VR big endian byte sequence representation when constructed without explicit bytes" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val bytes = DicomHeader(tag, vr, length, isFmi = false, bigEndian = true, explicitVR = true).bytes
    bytes shouldBe patientNameJohnDoeBE.take(8)
  }

  "DicomAttribute" should "should return a new attribute with updated header and updated value" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val header = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = true, patientNameJohnDoe.take(8))
    val value = DicomValueChunk(bigEndian = false, patientNameJohnDoe.drop(8) ,last = true)
    val attribute = DicomAttribute(header, Seq(value))
    val updatedAttribute = attribute.withUpdatedValue("Jimmyboy^Doe")
    updatedAttribute.valueBytes.size shouldEqual 12
    updatedAttribute.header.length shouldEqual 12
    updatedAttribute.bytes.size shouldEqual 20
  }

  it should "should return a new attribute with updated header and updated value with padding" in {
    val (tag, vr, _, length) = DicomParsing.readHeaderExplicitVR(patientNameJohnDoe, assumeBigEndian = false).get
    val header = DicomHeader(tag, vr, length, isFmi = false, bigEndian = false, explicitVR = true, patientNameJohnDoe.take(8))
    val value = DicomValueChunk(bigEndian = false, patientNameJohnDoe.drop(8) ,last = true)
    val attribute = DicomAttribute(header, Seq(value))
    val updatedAttribute = attribute.withUpdatedValue("Jimmy^Doe")

    updatedAttribute.valueBytes.size shouldEqual 10
    updatedAttribute.header.length shouldEqual 10
    updatedAttribute.valueBytes.drop(9) shouldEqual ByteString(32)
  }
}
