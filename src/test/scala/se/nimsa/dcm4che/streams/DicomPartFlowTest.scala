package se.nimsa.dcm4che.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.{Tag, VR}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomPartFlow.partFlow

import scala.concurrent.{Await, ExecutionContextExecutor}

class DicomPartFlowTest extends TestKit(ActorSystem("DicomFlowSpec")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  import DicomParts._
  import TestData._
  import TestUtils._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = system.terminate()

  "A DICOM flow" should "produce a preamble, FMI tags and attribute tags for a complete DICOM file" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

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

  it should "read files without preamble but with FMI" in {
    val bytes = fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "read a file with neither FMI nor preamble" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "not output value chunks when value length is zero" in {
    val bytes = ByteString(8, 0, 32, 0, 68, 65, 0, 0) ++ ByteString(16, 0, 16, 0, 80, 78, 0, 0)
    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectHeader(Tag.PatientName)
      .expectDicomComplete()
  }

  it should "output a warning message when non-meta information is included in the header" in {
    val bytes = fmiGroupLength(tsuidExplicitLE, studyDate) ++ tsuidExplicitLE ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "treat a preamble alone as a valid DICOM file" in {
    val bytes = preamble

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectDicomComplete()
  }

  it should "skip very long (and obviously erroneous) transfer syntaxes (see warning log message)" in {
    val malformedTsuid = tsuidExplicitLE.take(6) ++ ByteString(20, 8) ++ tsuidExplicitLE.takeRight(20) ++ ByteString.fromArray(new Array[Byte](2048))
    val bytes = fmiGroupLength(malformedTsuid) ++ malformedTsuid ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "fail reading a truncated DICOM file" in {
    val bytes = patientNameJohnDoe.dropRight(2)

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectDicomError()
  }

  it should "inflate deflated datasets" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate)

    val source = Source.single(bytes)
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

  it should "inflate gzip deflated datasets (with warning message)" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate, gzip = true)

    val source = Source.single(bytes)
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

  it should "pass through deflated data when asked not to inflate" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate)

    val source = Source.single(bytes)
      .via(new DicomPartFlow(inflate = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectDeflatedChunk()
      .expectDicomComplete()
  }

  it should "read DICOM data with fragments" in {
    val bytes = pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment(1, 4)
      .expectValueChunk()
      .expectFragment(2, 4)
      .expectValueChunk()
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  it should "issue a warning when a fragments delimitation tag has nonzero length" in {
    val bytes = pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ sequenceEndNonZeroLength

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment(1, 4)
      .expectValueChunk()
      .expectFragment(2, 4)
      .expectValueChunk()
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  it should "parse a tag which is not an item, item data nor fragments delimitation inside fragments as unknown" in {
    val bytes = pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ studyDate ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment(1, 4)
      .expectValueChunk()
      .expectUnknownPart()
      .expectFragment(2, 4)
      .expectValueChunk()
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  it should "read DICOM data containing a sequence" in {
    val bytes = sequence(Tag.DerivationCodeSequence) ++ item ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ sequenceEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(1)
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "read DICOM data containing a sequence in a sequence" in {
    val bytes = sequence(Tag.DerivationCodeSequence) ++ item ++ sequence(Tag.DerivationCodeSequence) ++ item ++ patientNameJohnDoe ++ itemEnd ++ sequenceEnd ++ studyDate ++ itemEnd ++ sequenceEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(1)
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(1)
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "read a valid DICOM file correctly when data chunks are very small" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new Chunker(chunkSize = 1))
      .via(new DicomPartFlow())

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

  it should "not accept a non-DICOM file" in {
    val bytes = ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomError()
  }

  it should "read DICOM files with explicit VR big-endian transfer syntax" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitBE) ++ tsuidExplicitBE ++ patientNameJohnDoeBE

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

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

  it should "read DICOM files with implicit VR little endian transfer syntax" in {
    val bytes = preamble ++ fmiGroupLength(tsuidImplicitLE) ++ tsuidImplicitLE ++ patientNameJohnDoeImplicit

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

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

  it should "stop reading data when a stop tag is reached" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow(stopTag = Some(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "chunk value data according to max chunk size" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow(chunkSize = 5))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectValueChunk()
      .expectValueChunk()
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "chunk deflated data according to max chunk size" in {
    val bytes = fmiGroupLength(tsuidDeflatedExplicitLE) ++ tsuidDeflatedExplicitLE ++ deflate(patientNameJohnDoe ++ studyDate)

    val source = Source.single(bytes)
      .via(new DicomPartFlow(chunkSize = 25, inflate = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectDeflatedChunk()
      .expectDeflatedChunk()
      .expectDicomComplete()
  }

  it should "accept meta information encoded with implicit VR" in {
    val bytes = preamble ++ tsuidExplicitLEImplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "handle values with length larger than the signed int range" in {
    val length = Int.MaxValue.toLong + 1
    val bytes = ByteString(0xe0, 0x7f, 0x10, 0x00, 0x4f, 0x57, 0, 0) ++ intToBytes(length.toInt, bigEndian = false)

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PixelData, VR.OW, length)
      .expectValueChunk(ByteString.empty)
      .expectDicomComplete()
  }

  it should "handle sequences and items of determinate length" in {
    val bytes = studyDate ++ (sequence(Tag.DerivationCodeSequence, 8 + 18 + 16) ++ item(18 + 16) ++ studyDate ++ patientNameJohnDoe) ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "handle fragments with empty basic offset table (first item)" in {
    val bytes = pixeDataFragments ++ fragment(0) ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment(1, 0)
      .expectFragment(2, 4)
      .expectValueChunk(4)
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  it should "read MR file" in {
    import scala.concurrent.duration.DurationInt

    val imageInformationTags = Seq(Tag.InstanceNumber, Tag.ImageIndex, Tag.NumberOfFrames, Tag.SmallestImagePixelValue, Tag.LargestImagePixelValue).sorted

    val file = new File(getClass.getResource("test.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(partFlow)
      .via(DicomFlows.tagFilter(_ => true)(tagPath => imageInformationTags.contains(tagPath.tag)))
      .via(DicomFlows.printFlow)

    val f = source.runWith(Sink.ignore)
    Await.ready(f, 30.seconds)
  }
}
