package se.nimsa.dcm4che.streams

import org.scalatest.{FlatSpecLike, Matchers}
import DicomData._
import akka.util.ByteString
import org.dcm4che3.data.Tag._
import org.dcm4che3.data.VR
import org.dcm4che3.data.UID._

class DicomParsingTest extends FlatSpecLike with Matchers {

  "DicomParsing" should "parse headers for explicit VR little endian" in {
    val bytes = patientNameJohnDoe
    val maybeHeader = DicomParsing.readHeaderExplicitVR(bytes, false)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "parse headers for explicit VR big endian" in {
    val bytes = patientNameJohnDoeBE
    val maybeHeader = DicomParsing.readHeaderExplicitVR(bytes, true)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "parse headers for implicit VR little endian" in {
    val bytes = patientNameJohnDoeImplicit
    val maybeHeader = DicomParsing.readHeaderImplicitVR(bytes)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "return None if buffer too short" in {
    val bytes = ByteString(16, 0, 16, 0, 80, 78)
    val maybeHeader = DicomParsing.readHeaderExplicitVR(bytes, false)
    maybeHeader.isEmpty shouldBe true
  }

  it should "parse a UID data element for explicit VR little endian" in {
    val bytes = mediaStorageSOPInstanceUID
    val msuid = DicomParsing.parseUIDAttribute(bytes, true, false)
    msuid.tag shouldBe MediaStorageSOPInstanceUID
    msuid.vr shouldBe VR.UI
    msuid.value.utf8String shouldEqual "1.2.276.0.7230010.3.1.4.1536491920.17152.1480884676.735"
  }

  it should "parse a UID data element for implicit VR little endian" in {
    val bytes = mediaStorageSOPClassUIDImplicitLE
    val msuid = DicomParsing.parseUIDAttribute(bytes, false, false)
    msuid.tag shouldBe MediaStorageSOPClassUID
    msuid.vr shouldBe VR.UI
    msuid.value.utf8String shouldEqual CTImageStorage
  }
}
