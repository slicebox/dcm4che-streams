package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.{Tag, VR}
import org.scalatest.{FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomPartFlow.DicomPart

class DicomFlowsTest extends TestKit(ActorSystem("DicomAttributesSinkSpec")) with FlatSpecLike with Matchers {

  import DicomData._
  import se.nimsa.dcm4che.streams.DicomFlows._

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  "A DICOM attributes flow" should "combine headers and value chunks into attributes" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectAttribute(Tag.PatientName)
      .expectAttribute(Tag.StudyDate)
      .expectDicomComplete()
  }

  it should "combine items in fragments into fragment elements" in {
    val bytes = pixeDataFragments ++ itemStart(4) ++ ByteString(1, 2, 3, 4) ++ itemStart(4) ++ ByteString(5, 6, 7, 8) ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment()
      .expectFragment()
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  "A print flow" should "not change incoming elements" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(printFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The DICOM parts filter" should "block all attributes not on the white list" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(partFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "also apply to FMI is instructed" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(partFilter(Seq(Tag.StudyDate), applyToFmi = true))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "also apply to attributes in sequences" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(partFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "also work on fragments" in {
    val bytes = pixeDataFragments ++ itemStart(4) ++ ByteString(1, 2, 3, 4) ++ itemStart(4) ++ ByteString(5, 6, 7, 8) ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(partFilter(Seq.empty))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  "The DICOM group length discard filter" should "discard group length elements except 0002,0000" in {
    val groupLength = ByteString(8, 0, 0, 0, 85, 76, 4, 0) ++ DicomData.intToBytesLE(studyDate.size)
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ groupLength ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(groupLengthDiscardFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }


  "The DICOM blacklist filter" should "filter elements matching the blacklist condition" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ fmiVersion ++ tsuidExplicitLE ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(blacklistFilter((tag: Int) => DicomParsing.isFileMetaInformation(tag), applyToFmi = true))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }


  "The DICOM whitelist filter" should "filter elements not matching the whitelist condition" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ fmiVersion ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter((tag: Int) => DicomParsing.groupNumber(tag) >= 8, applyToFmi = true))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The transform flow" should "transform the value of the specified attributes" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributesTransformFlow((Tag.PatientName, _ => mikeBytes), (Tag.StudyDate, _ => ByteString.empty)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName, VR.PN, mikeBytes.length)
      .expectValueChunk(mikeBytes)
      .expectHeader(Tag.StudyDate, VR.DA, 0)
      .expectValueChunk(ByteString.empty)
      .expectDicomComplete()
  }

  it should "transform attributes in sequences" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributesTransformFlow((Tag.PatientName, _ => mikeBytes)))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem()
      .expectHeader(Tag.PatientName, VR.PN, mikeBytes.length)
      .expectValueChunk(mikeBytes)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  "The deflate flow" should "recreate the dicom parts of a dataset which has been deflated and inflated again" in {
    val bytes = fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(deflateDatasetFlow())
      .via(attributesTransformFlow(
        (Tag.FileMetaInformationGroupLength, _ => fmiGroupLength(tsuidDeflatedExplicitLE)),
        (Tag.TransferSyntaxUID, _ => ByteString('1', '.', '2', '.', '8', '4', '0', '.', '1', '0', '0', '0', '8', '.', '1', '.', '2', '.', '1', '.', '9', '9'))))
      .map(_.bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "not deflate meta information" in {
    val bytes = fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(deflateDatasetFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectDeflatedChunk()
  }

}

