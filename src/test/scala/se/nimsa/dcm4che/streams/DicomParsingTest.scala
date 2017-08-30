package se.nimsa.dcm4che.streams

import java.nio.ByteBuffer

import akka.util.ByteString
import org.dcm4che3.data.Tag._
import org.dcm4che3.data.UID._
import org.dcm4che3.data.VR
import org.scalatest.{FlatSpecLike, Matchers}

class DicomParsingTest extends FlatSpecLike with Matchers {

  import DicomParsing._
  import TestData._
  
  "DicomParsing" should "parse headers for explicit VR little endian" in {
    val bytes = patientNameJohnDoe
    val maybeHeader = readHeaderExplicitVR(bytes, assumeBigEndian = false)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "parse headers for explicit VR big endian" in {
    val bytes = patientNameJohnDoeBE
    val maybeHeader = readHeaderExplicitVR(bytes, assumeBigEndian = true)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "parse headers for implicit VR little endian" in {
    val bytes = patientNameJohnDoeImplicit
    val maybeHeader = readHeaderImplicitVR(bytes)
    maybeHeader.isDefined shouldBe true
    val (tag, vr, headerLength, length) = maybeHeader.get
    tag shouldEqual PatientName
    vr shouldEqual VR.PN
    headerLength shouldEqual 8
    length shouldEqual 8
  }

  it should "return None if buffer too short" in {
    val bytes = ByteString(16, 0, 16, 0, 80, 78)
    val maybeHeader = readHeaderExplicitVR(bytes, assumeBigEndian = false)
    maybeHeader.isEmpty shouldBe true
  }

  it should "parse a UID data element for explicit VR little endian" in {
    val bytes = mediaStorageSOPInstanceUID
    val msuid = parseUIDAttribute(bytes, explicitVR = true, assumeBigEndian = false)
    msuid.tag shouldBe MediaStorageSOPInstanceUID
    msuid.vr shouldBe VR.UI
    msuid.value.utf8String shouldEqual "1.2.276.0.7230010.3.1.4.1536491920.17152.1480884676.735"
  }

  it should "parse a UID data element for implicit VR little endian" in {
    val bytes = mediaStorageSOPClassUIDImplicitLE
    val msuid = parseUIDAttribute(bytes, explicitVR = false, assumeBigEndian = false)
    msuid.tag shouldBe MediaStorageSOPClassUID
    msuid.vr shouldBe VR.UI
    msuid.value.utf8String shouldEqual CTImageStorage
  }

  it should "transform a ByteString of size 2 to the correct short representation" in {
    val s1 = bytesToShort(ByteString(0xA, 0x5), bigEndian = true)
    s1 shouldBe a[java.lang.Short] // runtime type is java native short
    s1 shouldBe 0xA05
    val s2 = bytesToShort(ByteString(0xA, 0x5), bigEndian = false)
    s2 shouldBe a[java.lang.Short]
    s2 shouldBe 0x50A
  }

  it should "transform a ByteString of size 4 to the correct integer representation" in {
    val s1 = bytesToInt(ByteString(0xA, 0xB, 0xC, 0xD), bigEndian = true)
    s1 shouldBe a[java.lang.Integer] // runtime type is java native short
    s1 shouldBe 0x0A0B0C0D
    val s2 = bytesToInt(ByteString(0xA, 0xB, 0xC, 0xD), bigEndian = false)
    s2 shouldBe a[java.lang.Integer]
    s2 shouldBe 0x0D0C0B0A
  }

  it should "transform a ByteString of size 8 to the correct long representation" in {
    val s1 = bytesToLong(ByteString(1, 2, 3, 4, 5, 6, 7, 8), bigEndian = true)
    s1 shouldBe a[java.lang.Long]
    s1 shouldBe 0x0102030405060708L
    val s2 = bytesToLong(ByteString(1, 2, 3, 4, 5, 6, 7, 8), bigEndian = false)
    s2 shouldBe a[java.lang.Long]
    s2 shouldBe 0x0807060504030201L
  }

  it should "transform a ByteString of size 8 to the correct double representation" in {
    val beBytes = ByteString(ByteBuffer.allocate(8).putLong(java.lang.Double.doubleToLongBits(math.Pi)).array()) // java is big-endian
    val s1 = bytesToDouble(beBytes, bigEndian = true)
    s1 shouldBe a[java.lang.Double]
    s1 shouldBe math.Pi
    val s2 = bytesToDouble(beBytes.reverse, bigEndian = false)
    s2 shouldBe a[java.lang.Double]
    s2 shouldBe math.Pi
  }

  it should "transform a ByteString of size 4 to the correct float representation" in {
    val beBytes = ByteString(ByteBuffer.allocate(4).putInt(java.lang.Float.floatToIntBits(math.Pi.toFloat)).array()) // java is big-endian
    val s1 = bytesToFloat(beBytes, bigEndian = true)
    s1 shouldBe a[java.lang.Float]
    s1 shouldBe math.Pi.toFloat
    val s2 = bytesToFloat(beBytes.reverse, bigEndian = false)
    s2 shouldBe a[java.lang.Float]
    s2 shouldBe math.Pi.toFloat
  }

  it should "transform a ByteString of size 2 to the correct unsigned short representation (as an Int)" in {
    val s1 = bytesToUShort(ByteString(0xFF, 0xFE), bigEndian = true)
    s1 shouldBe a[java.lang.Integer]
    s1 shouldBe 0xFFFE
    val s2 = bytesToUShort(ByteString(0xFF, 0xFE), bigEndian = false)
    s2 shouldBe a[java.lang.Integer]
    s2 shouldBe 0xFEFF
  }

  it should "parse a tag number given as two consecutive short numbers (big or little endian)" in {
    val s1 = ByteString(0xA, 0xB)
    val s2 = ByteString(1, 2)
    val t1 = bytesToTag(s1.concat(s2), bigEndian = true)
    t1 shouldBe a[java.lang.Integer]
    t1 shouldBe 0x0A0B0102
    val t2 = bytesToTag(s1.concat(s2), bigEndian = false)
    t2 shouldBe a[java.lang.Integer]
    t2 shouldBe 0x0B0A0201
  }

  it should "parse two bytes in big-endian order as a VR code" in {
    val vr = ByteString(0xAB, 0xCD)
    bytesToVR(vr) shouldBe 0xABCD
  }

  it should "decode a short into bytes" in {
    shortToBytes(0xABCD.toShort, bigEndian = true) shouldBe ByteString(0xAB, 0xCD)
    shortToBytes(0xABCD.toShort, bigEndian = false) shouldBe ByteString(0xCD, 0xAB)
  }

  it should "decode an integer into bytes" in {
    intToBytes(0x01020304, bigEndian = true) shouldBe ByteString(1, 2, 3, 4)
    intToBytes(0x01020304, bigEndian = false) shouldBe ByteString(4, 3, 2, 1)
  }

  it should "not read explicit VR attribute headers with insufficient length" in {
    readHeaderExplicitVR(ByteString.empty, assumeBigEndian = true) match {
      case Some(_) => fail
      case None =>
    }
    val tagBytes = ByteString(0x00, 0x08, 0x00, 0x08)
    val vrBytes = ByteString(0x4F, 0x42)
    val lengthBytes = ByteString(0, 1) // should be length 4 for this VR (OB, header length 12)
    readHeaderExplicitVR(tagBytes ++ vrBytes ++ lengthBytes, assumeBigEndian = true) match {
      case Some(_) => fail
      case None =>
    }
  }

  it should "read explicit VR attribute headers with sufficient length" in {
    val tagBytes = ByteString(0x00, 0x08, 0x00, 0x08)
    val vrBytes = ByteString(0x43, 0x53)
    val lengthBytes = ByteString(0, 1)
    readHeaderExplicitVR(tagBytes ++ vrBytes ++ lengthBytes, assumeBigEndian = true) match {
      case Some((tag, vr, headerLength, valueLength)) =>
        tag shouldBe 0x00080008
        vr shouldBe VR.CS
        headerLength shouldBe 8
        valueLength shouldBe 1
      case None =>
        fail
    }
  }

  it should "read explicit VR sequence attribute headers with valid length" in {
    val tagBytes = ByteString(0xFF, 0xFE, 0xE0, 0x00)
    val lengthBytes = ByteString(0, 0, 0, 1)
    readHeaderExplicitVR(tagBytes ++ lengthBytes, assumeBigEndian = true) match {
      case Some((tag, vr, headerLength, valueLength)) =>
        tag shouldBe 0xFFFEE000
        vr shouldBe null
        headerLength shouldBe 8
        valueLength shouldBe 1
      case None =>
        fail
    }
  }

  it should "not read explicit VR sequence attribute headers with undefined length" in {
    val tagBytes = ByteString(0xFF, 0xFE, 0xE0, 0x00)
    val vrBytes = ByteString(0xFF, 0xFF, 0xFF, 0xFF)
    readHeaderExplicitVR(tagBytes ++ vrBytes, assumeBigEndian = true) match {
      case Some(_) =>
        fail
      case None =>
    }
  }

  it should "not read implicit VR attribute headers with insufficient length" in {
    readHeaderImplicitVR(ByteString.empty) match {
      case Some(_) => fail
      case None =>
    }
  }

  it should "read implicit VR attribute headers with valid length" in {
    val tagBytes = ByteString(0x08, 0x00, 0x08, 0x00)
    val lengthBytes = ByteString(1, 0, 0, 0)
    readHeaderImplicitVR(tagBytes ++ lengthBytes) match {
      case Some((tag, vr, headerLength, valueLength)) =>
        tag shouldBe 0x00080008
        vr shouldBe VR.CS
        headerLength shouldBe 8
        valueLength shouldBe 1
      case None =>
        fail
    }
  }

  it should "read implicit VR sequence attribute headers with valid length" in {
    val tagBytes = ByteString(0xFE, 0xFF, 0x00, 0xE0)
    val lengthBytes = ByteString(1, 0, 0, 0)
     readHeaderImplicitVR(tagBytes ++ lengthBytes) match {
      case Some((tag, vr, headerLength, valueLength)) =>
        tag shouldBe 0xFFFEE000
        vr shouldBe null
        headerLength shouldBe 8
        valueLength shouldBe 1
      case None =>
        fail
    }
  }

  it should "not read implicit VR sequence attribute headers with undefined length" in {
    val tagBytes = ByteString(0xFE, 0xFF, 0x00, 0xE0)
    val lengthBytes = ByteString(0xFF, 0xFF, 0xFF, 0xFF)
    readHeaderImplicitVR(tagBytes ++ lengthBytes) match {
      case Some(_) =>
        fail
      case None =>
    }
  }
}
