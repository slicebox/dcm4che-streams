package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.{Tag, VR}
import org.scalatest.{FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomModifyFlow.TagModification

class DicomModifyFlowTest extends TestKit(ActorSystem("DicomFlowSpec")) with FlatSpecLike with Matchers {

  import DicomData._
  import DicomParts._
  import DicomModifyFlow.modifyFlow

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  "The modify flow" should "modify the value of the specified attributes" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromTag(Tag.PatientName), _ => mikeBytes, insert = false),
        TagModification(TagPath.fromTag(Tag.StudyDate), _ => ByteString.empty, insert = false)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName, VR.PN, mikeBytes.length)
      .expectValueChunk(mikeBytes)
      .expectHeader(Tag.StudyDate, VR.DA, 0)
      .expectValueChunk(ByteString.empty)
      .expectDicomComplete()
  }

  it should "not modify attributes in datasets other than the dataset the tag path points to" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(TagModification(TagPath.fromTag(Tag.PatientName), _ => mikeBytes, insert = false)))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(0)
      .expectHeader(Tag.PatientName, VR.PN, patientNameJohnDoe.length - 8)
      .expectValueChunk(patientNameJohnDoe.drop(8))
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "insert attributes if not present" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(TagModification(TagPath.fromTag(Tag.StudyDate), _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate, VR.DA, studyDate.length - 8)
      .expectValueChunk(studyDate.drop(8))
      .expectHeader(Tag.PatientName, VR.PN, patientNameJohnDoe.length - 8)
      .expectValueChunk(patientNameJohnDoe.drop(8))
      .expectDicomComplete()
  }

  it should "insert attributes if not present also at end of dataset" in {
    val bytes = studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(TagModification(TagPath.fromTag(Tag.PatientName), _ => patientNameJohnDoe.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate, VR.DA, studyDate.length - 8)
      .expectValueChunk(studyDate.drop(8))
      .expectHeader(Tag.PatientName, VR.PN, patientNameJohnDoe.length - 8)
      .expectValueChunk(patientNameJohnDoe.drop(8))
      .expectDicomComplete()
  }

  it should "insert all relevant attributes below the current tag number" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromTag(Tag.SeriesDate), _ => studyDate.drop(8), insert = true),
        TagModification(TagPath.fromTag(Tag.StudyDate), _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate, VR.DA, studyDate.length - 8)
      .expectValueChunk(studyDate.drop(8))
      .expectHeader(Tag.SeriesDate, VR.DA, studyDate.length - 8)
      .expectValueChunk(studyDate.drop(8))
      .expectHeader(Tag.PatientName, VR.PN, patientNameJohnDoe.length - 8)
      .expectValueChunk(patientNameJohnDoe.drop(8))
      .expectDicomComplete()
  }

  it should "not insert attributes if dataset contains no attributes" in {
    val source = Source.empty
      .via(new DicomPartFlow())
      .via(modifyFlow(TagModification(TagPath.fromTag(Tag.SeriesDate), _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  it should "insert attributes in sequences if sequence is present but attribute is not present" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ itemEnd ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromSequence(Tag.DerivationCodeSequence).thenTag(Tag.StudyDate), _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(0)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "skip inserting attributes in missing sequences" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromSequence(Tag.DerivationCodeSequence).thenTag(Tag.StudyDate), _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "not insert unknown attributes" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromTag(0x00200021), _ => ByteString(1, 2, 3, 4), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomError()
  }

  it should "not insert sequence attributes" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(TagPath.fromTag(Tag.DerivationCodeSequence), _ => ByteString.empty, insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomError()
  }
}
