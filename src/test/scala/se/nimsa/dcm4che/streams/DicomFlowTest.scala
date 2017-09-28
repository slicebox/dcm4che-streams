package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.dcm4che3.data.Tag
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomPartFlow.partFlow

import scala.concurrent.ExecutionContextExecutor

class DicomFlowTest extends TestKit(ActorSystem("DicomFlowSpec")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  import DicomParts._
  import TestData._
  import TestUtils._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = system.terminate()

  "The delimitation always flow" should "insert delimitation parts at the end of sequences and items with determinate length" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 56) ++ item(16) ++ studyDate ++ item() ++ studyDate ++ itemEnd ++
        sequence(Tag.AbstractPriorCodeSequence) ++ item() ++ studyDate ++ itemEnd ++ item(16) ++ studyDate ++ sequenceEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 56)
      .expectItem(1, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectItem(2)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectSequence(Tag.AbstractPriorCodeSequence, -1)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectItem(2, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "handle sequences that end with an item delimitation" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 32) ++ item() ++ studyDate ++ itemEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 32)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "work in datasets with nested sequences" in {
    val bytes = studyDate ++ sequence(Tag.DerivationCodeSequence, 60) ++ item(52) ++ studyDate ++
      sequence(Tag.DerivationCodeSequence, 24) ++ item(16) ++ studyDate ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectSequence(Tag.DerivationCodeSequence, 60)
      .expectItem(1, 52)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectSequence(Tag.DerivationCodeSequence, 24)
      .expectItem(1, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "handle empty sequences and items" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 52) ++ item(16) ++ studyDate ++ item(0) ++ item(12) ++ sequence(Tag.DerivationCodeSequence, 0)

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 52)
      .expectItem(1, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectItem(2, 0)
      .expectItemDelimitation()
      .expectItem(3, 12)
      .expectSequence(Tag.DerivationCodeSequence, 0)
      .expectSequenceDelimitation()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "not insert duplicate delimiters in streams where they are already inserted" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 24) ++ item(16) ++ studyDate

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))
      .via(DicomFlowFactory.create(new DicomFlow with DelimitationAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 24)
      .expectItem(1, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  "The value always flow" should "insert empty value chunks after length zero headers" in {
    val bytes = patientNameJohnDoe ++ emptyPatientName

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with ValueAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk(8)
      .expectHeader(Tag.PatientName)
      .expectValueChunk(0)
      .expectDicomComplete()
  }

  it should "not insert duplicate delimiters in streams where they are already inserted" in {
    val bytes = emptyPatientName

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with ValueAlways))
      .via(DicomFlowFactory.create(new DicomFlow with ValueAlways))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk(0)
      .expectDicomComplete()
  }

}
