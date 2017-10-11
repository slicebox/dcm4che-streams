package se.nimsa.dcm4che.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.Tag
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomPartFlow.partFlow

import scala.concurrent.ExecutionContextExecutor

class DicomFlowTest extends TestKit(ActorSystem("DicomFlowSpec")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  import DicomParts._
  import TestData._
  import TestUtils._
  import DicomFlows._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = system.terminate()

  "The dicom flow" should "call the correct events for streamed dicom parts" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++
      patientNameJohnDoe ++ sequence(Tag.DerivationCodeSequence) ++ item() ++ studyDate ++ itemEnd ++ sequenceEnd ++
      pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow {
        override def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = TestPart("Fragments End") :: Nil
        override def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart] = TestPart("Fragments Item Start") :: Nil
        override def onFragmentsStart(part: DicomFragments): List[DicomPart] = TestPart("Fragments Start") :: Nil
        override def onHeader(part: DicomHeader): List[DicomPart] = TestPart("Header") :: Nil
        override def onPreamble(part: DicomPreamble): List[DicomPart] = TestPart("Preamble") :: Nil
        override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = TestPart("Sequence End") :: Nil
        override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = TestPart("Sequence Item End") :: Nil
        override def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = TestPart("Sequence Item Start") :: Nil
        override def onSequenceStart(part: DicomSequence): List[DicomPart] = TestPart("Sequence Start") :: Nil
        override def onValueChunk(part: DicomValueChunk): List[DicomPart] = TestPart("Value Chunk") :: Nil
        override def onDeflatedChunk(part: DicomDeflatedChunk): List[DicomPart] = Nil
        override def onUnknownPart(part: DicomUnknownPart): List[DicomPart] = Nil
        override def onPart(part: DicomPart): List[DicomPart] = Nil
      }))

    source.runWith(TestSink.probe[DicomPart])
      .expectTestPart("Preamble")
      .expectTestPart("Header")
      .expectTestPart("Value Chunk")
      .expectTestPart("Header")
      .expectTestPart("Value Chunk")
      .expectTestPart("Header")
      .expectTestPart("Value Chunk")
      .expectTestPart("Sequence Start")
      .expectTestPart("Sequence Item Start")
      .expectTestPart("Header")
      .expectTestPart("Value Chunk")
      .expectTestPart("Sequence Item End")
      .expectTestPart("Sequence End")
      .expectTestPart("Fragments Start")
      .expectTestPart("Fragments Item Start")
      .expectTestPart("Value Chunk")
      .expectTestPart("Fragments End")
      .expectDicomComplete()
  }

  "The guaranteed delimitation flow" should "insert delimitation parts at the end of sequences and items with determinate length" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 56) ++ item(16) ++ studyDate ++ item() ++ studyDate ++ itemEnd ++
        sequence(Tag.AbstractPriorCodeSequence) ++ item() ++ studyDate ++ itemEnd ++ item(16) ++ studyDate ++ sequenceEnd

    var expectedDelimitationLengths = List(0, 8, 0, 8, 0, 8)

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with JustEmit with GuaranteedDelimitationEvents {
        override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = {
          part.bytes.length shouldBe expectedDelimitationLengths.head
          expectedDelimitationLengths = expectedDelimitationLengths.tail
          super.onSequenceItemEnd(part)
        }
        override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = {
          part.bytes.length shouldBe expectedDelimitationLengths.head
          expectedDelimitationLengths = expectedDelimitationLengths.tail
          super.onSequenceEnd(part)
        }
      }))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 56)
      .expectItem(1, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      //.expectItemDelimitation() // delimitations not emitted by default
      .expectItem(2)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      //.expectSequenceDelimitation()
      .expectSequence(Tag.AbstractPriorCodeSequence, -1)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectItem(2, 16)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      //.expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "handle sequences that end with an item delimitation" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 32) ++ item() ++ studyDate ++ itemEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(guaranteedDelimitationFlow())

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
      .via(guaranteedDelimitationFlow())

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
      .via(guaranteedDelimitationFlow())

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

  it should "handle empty attributes in sequences" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 44) ++ item(36) ++ emptyPatientName ++
        sequence(Tag.DerivationCodeSequence, 16) ++ item(8) ++ emptyPatientName

    val source = Source.single(bytes)
      .via(partFlow)
      .via(guaranteedDelimitationFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, 44)
      .expectItem(1, 36)
      .expectHeader(Tag.PatientName)
      .expectSequence(Tag.DerivationCodeSequence, 16)
      .expectItem(1, 8)
      .expectHeader(Tag.PatientName)
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  "The guaranteed value flow" should "call onValueChunk callback also after length zero headers" in {
    val bytes = patientNameJohnDoe ++ emptyPatientName

    var expectedChunkLengths = List(8, 0)

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with JustEmit with GuaranteedValueEvent {
        override def onValueChunk(part: DicomValueChunk): List[DicomPart] = {
          part.bytes.length shouldBe expectedChunkLengths.head
          expectedChunkLengths = expectedChunkLengths.tail
          super.onValueChunk(part)
        }
      }))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk(8)
      .expectHeader(Tag.PatientName)
      .expectDicomComplete()
  }

  it should "emit empty chunks when overriding onValueChunk appropriately" in {
    val bytes = patientNameJohnDoe ++ emptyPatientName

    val source = Source.single(bytes)
      .via(partFlow)
      .via(guaranteedValueFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk(8)
      .expectHeader(Tag.PatientName)
      .expectValueChunk(0)
      .expectDicomComplete()
  }

  def endEventTestFlow(): Flow[DicomPart, DicomPart, NotUsed] =
    DicomFlowFactory.create(new DicomFlow with JustEmit with EndEvent {
      override def onEnd(): List[DicomPart] = DicomEndMarker :: Nil
    })

  "The end event flow" should "notify when dicom stream ends" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(endEventTestFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .request(1)
      .expectNextChainingPF {
        case DicomEndMarker => true
      }
      .expectDicomComplete()
  }

  "Dicom flows with tag path tracking" should "update the tag path through attributes, sequences and fragments" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ // FMI
      patientNameJohnDoe ++ // attribute
      sequence(Tag.DerivationCodeSequence) ++ item() ++ studyDate ++ itemEnd ++ item() ++ // sequence
      sequence(Tag.DerivationCodeSequence, 24) ++ item(16) ++ studyDate ++ // nested sequence (determinate length)
      itemEnd ++ sequenceEnd ++
      pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragmentsEnd

    var expectedPaths = List(
      None, // preamble
      Some(TagPath.fromTag(Tag.FileMetaInformationGroupLength)), None, // FMI group length header, then value
      Some(TagPath.fromTag(Tag.TransferSyntaxUID)), None, // Transfer syntax header, then value
      Some(TagPath.fromTag(Tag.PatientName)), None, // Patient name header, then value
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence)), // sequence start
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 1)), // item start
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 1).thenTag(Tag.StudyDate)), // study date header
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 1)), // study date value
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence)), // item end
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2)), // item start
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2).thenSequence(Tag.DerivationCodeSequence)), // sequence start
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2).thenSequence(Tag.DerivationCodeSequence, 1)), // item start
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2).thenSequence(Tag.DerivationCodeSequence, 1).thenTag(Tag.StudyDate)), // Study date header
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2).thenSequence(Tag.DerivationCodeSequence, 1)), // Study date value
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2).thenSequence(Tag.DerivationCodeSequence)), //  item end (inserted)
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence, 2)), // sequence end (inserted)
      Some(TagPath.fromSequence(Tag.DerivationCodeSequence)), // item end
      None, // sequence end
      Some(TagPath.fromTag(Tag.PixelData)), // fragments start
      Some(TagPath.fromTag(Tag.PixelData)), // item start
      Some(TagPath.fromTag(Tag.PixelData)), // fragment data
      None // fragments end
    )

    def check(tagPath: Option[TagPath]): Unit = {
      tagPath shouldBe expectedPaths.head
      expectedPaths = expectedPaths.tail
    }

    val source = Source.single(bytes)
      .via(partFlow)
      .via(DicomFlowFactory.create(new DicomFlow with TreatAsPart with TagPathTracking {
        override def onPart(part: DicomPart): List[DicomPart] = {
          check(tagPath)
          part :: Nil
        }
      }))

    source.runWith(TestSink.probe[DicomPart])
      .request(23)
      .expectNextN(23)
  }

}
