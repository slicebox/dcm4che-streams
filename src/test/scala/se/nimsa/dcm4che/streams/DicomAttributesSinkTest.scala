package se.nimsa.dcm4che.streams

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data._
import org.dcm4che3.io.{DicomInputStream, DicomOutputStream, DicomStreamException}
import org.scalatest.{AsyncFlatSpecLike, Matchers}

import scala.concurrent.Future

class DicomAttributesSinkTest extends TestKit(ActorSystem("DicomAttributesSinkSpec")) with AsyncFlatSpecLike with Matchers {

  import DicomData._
  import se.nimsa.dcm4che.streams.DicomAttributesSink._
  import se.nimsa.dcm4che.streams.DicomFlows._

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  def toAttributes(bytes: ByteString): (Option[Attributes], Option[Attributes]) = {
    val dis = new DicomInputStream(new ByteArrayInputStream(bytes.toArray))
    val fmi = Option(dis.readFileMetaInformation())
    val dataset = Option(dis.readDataset(-1, -1))
    (fmi, dataset)
  }

  def toBytes(attributes: Attributes, tsuid: String): ByteString = {
    val baos = new ByteArrayOutputStream()
    val dos = new DicomOutputStream(baos, tsuid)
    dos.writeDataset(null, attributes)
    dos.close()
    ByteString(baos.toByteArray)
  }

  def toFlowAttributes(bytes: ByteString): Future[(Option[Attributes], Option[Attributes])] =
    Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)
      .runWith(attributesSink)

  def assertEquivalentToDcm4che(bytes: ByteString) = {
    val attributes = toAttributes(bytes)
    val futureFlowAttributes = toFlowAttributes(bytes)

    futureFlowAttributes.map { flowAttributes =>
      flowAttributes shouldEqual attributes
    }
  }

  "The DICOM attributes sink" should "be equivalent to dcm4che for a file with preamble, FMI and a dataset" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe
    assertEquivalentToDcm4che(bytes)
  }

  it should "be equivalent to dcm4che for a file with neither FMI nor preamble" in {
    val bytes = patientNameJohnDoe
    assertEquivalentToDcm4che(bytes)
  }

  it should "be equivalent to dcm4che for a file with big endian encoding" in {
    val bytes = patientNameJohnDoeBE
    assertEquivalentToDcm4che(bytes)
  }

  it should "accept but not produce FMI nor dataset attributes for a file consisting of a preamble only" in {
    val bytes = preamble

    val futureFlowAttributes = toFlowAttributes(bytes)

    futureFlowAttributes.map {
      case (maybeFmi, maybeDataset) =>
        maybeFmi shouldBe empty
        maybeDataset shouldBe empty
    }
  }

  it should "be equivalent to dcm4che for deflated DICOM files" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate)
    assertEquivalentToDcm4che(bytes)
  }

  it should "skip deflated data chunks" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate)

    val futureFlowAttributes = Source.single(bytes)
      .via(new DicomPartFlow(inflate = false))
      .via(attributeFlow)
      .runWith(attributesSink)

    futureFlowAttributes.map {
      case (maybeFmi, maybeDataset) =>
        maybeFmi shouldBe defined
        maybeDataset shouldBe empty
    }
  }

  it should "fail when upstream fails" in {
    val bytes = ByteString(1, 2, 3, 4)

    recoverToSucceededIf[DicomStreamException] {
      toFlowAttributes(bytes)
    }
  }

  it should "be equivalent to dcm4che for DICOM files with fragments" in {
    val bytes = pixeDataFragments ++ itemStart(4) ++ ByteString(1, 2, 3, 4) ++ itemStart(4) ++ ByteString(5, 6, 7, 8) ++ seqEnd

    val (maybeFmi, maybeDataset) = toAttributes(bytes)
    val futureFlowAttributes = toFlowAttributes(bytes)

    futureFlowAttributes.map {
      case (maybeFlowFmi, maybeFlowDataset) =>
        // no equals method for fragments so...
        maybeFlowFmi shouldBe empty
        maybeFlowFmi shouldEqual maybeFmi
        maybeFlowDataset shouldBe defined
        maybeDataset shouldBe defined
        maybeFlowDataset.flatMap { flowDataset =>
          maybeDataset.map { dataset =>
            flowDataset.size shouldEqual dataset.size
            flowDataset.tags shouldEqual dataset.tags
            val v1 = flowDataset.getValue(Tag.PixelData)
            val v2 = dataset.getValue(Tag.PixelData)
            v1 shouldBe a [Fragments]
            v2 shouldBe a [Fragments]
            val f1 = v1.asInstanceOf[Fragments]
            val f2 = v2.asInstanceOf[Fragments]
            f1.size shouldBe 2
            f2.size shouldBe 2
            util.Arrays.equals(f1.get(0).asInstanceOf[Array[Byte]], f2.get(0).asInstanceOf[Array[Byte]]) shouldBe true
            util.Arrays.equals(f1.get(1).asInstanceOf[Array[Byte]], f2.get(1).asInstanceOf[Array[Byte]]) shouldBe true
          }
        }.getOrElse(fail)
    }
  }

  it should "be equivalent to dcm4che for DICOM files with sequences" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd
    assertEquivalentToDcm4che(bytes)
  }

  it should "be equivalent to dcm4che for DICOM files with sequences in sequences" in {
    val bytes = seqStart ++ itemNoLength ++ seqStart ++ itemNoLength ++ patientNameJohnDoe ++ itemEnd ++ seqEnd ++ studyDate ++ itemEnd ++ seqEnd
    assertEquivalentToDcm4che(bytes)
  }

  it should "not handle non-standard encodings when specific character set is not specified" in {
    val attr = new Attributes()
    attr.setString(Tag.PatientName, VR.PN, "Ö₯")
    val bytes = toBytes(attr, UID.ExplicitVRLittleEndian)
    val attr2 = toAttributes(bytes)._2.get
    attr2.getString(Tag.PatientName) should not be "Ö₯"
  }

  it should "handle non-standard encodings" in {
    val attr = new Attributes()
    attr.setSpecificCharacterSet("ISO 2022 IR 100", "ISO 2022 IR 126")
    attr.setString(Tag.PatientName, VR.PN, "Ö₯")
    val bytes = toBytes(attr, UID.ExplicitVRLittleEndian)
    val attr2 = toAttributes(bytes)._2.get
    attr2.getString(Tag.PatientName) shouldBe "Ö₯"
  }

}
