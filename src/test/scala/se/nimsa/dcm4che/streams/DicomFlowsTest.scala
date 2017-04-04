package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.Tag
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

}
