package se.nimsa.dcm4che.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContextExecutor


class DicomFlowsTest extends TestKit(ActorSystem("DicomAttributesSinkSpec")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  import DicomFlows._
  import DicomModifyFlow._
  import DicomPartFlow._
  import DicomParts._
  import TestData._
  import TestUtils._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = system.terminate()

  "A DICOM attributes flow" should "combine headers and value chunks into attributes" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectAttribute(Tag.PatientName, 8)
      .expectAttribute(Tag.StudyDate, 8)
      .expectDicomComplete()
  }

  it should "combine items in fragments into fragment elements" in {
    val bytes = pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragmentData(4)
      .expectFragmentData(4)
      .expectFragmentsDelimitation()
      .expectDicomComplete()
  }

  it should "handle attributes and fragments of zero length" in {
    val bytes = ByteString(8, 0, 32, 0, 68, 65, 0, 0) ++ patientNameJohnDoe ++
      pixeDataFragments ++ fragment(0) ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(attributeFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectAttribute(Tag.StudyDate, 0)
      .expectAttribute(Tag.PatientName, 8)
      .expectFragments()
      .expectFragmentData(0)
      .expectFragmentData(4)
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
      .via(whitelistFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "also apply to FMI is instructed" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "only apply to attributes in the root dataset" in {
    val bytes = sequence(Tag.DerivationCodeSequence) ++ item ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ sequenceEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  it should "also work on fragments" in {
    val bytes = pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragment(4) ++ ByteString(5, 6, 7, 8) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter(Seq.empty))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  "The DICOM group length discard filter" should "discard group length elements except 0002,0000" in {
    val groupLength = ByteString(8, 0, 0, 0, 85, 76, 4, 0) ++ intToBytesLE(studyDate.size)
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

  it should "discard group length elements except 0002,0000 when testing with dicom file" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(partFlow)
      .via(groupLengthDiscardFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.FileMetaInformationVersion)
      .expectValueChunk()
  }

  "The DICOM file meta information discard filter" should "discard file meta informaton" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(fmiDiscardFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "discard file meta information when testing with dicom files" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(partFlow)
      .via(fmiDiscardFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.SpecificCharacterSet)
      .expectValueChunk()
      .expectHeader(Tag.ImageType)
  }


  "The DICOM blacklist filter" should "filter elements matching the blacklist condition" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ fmiVersion ++ tsuidExplicitLE ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(tagFilter(tagPath => DicomParsing.isFileMetaInformation(tagPath.tag), keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "filter elements matching the blacklist condition when testing with sample dicom files" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(new DicomPartFlow())
      .via(tagFilter(tagPath => DicomParsing.isFileMetaInformation(tagPath.tag), keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.SpecificCharacterSet)
      .expectValueChunk()
      .expectHeader(Tag.ImageType)
  }


  it should "filter leave the dicom file unchanged when blacklist condition does not match any attribute" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(new DicomPartFlow())
      .via(tagFilter(tagPath => DicomParsing.isPrivateAttribute(tagPath.tag), keepPreamble = true))

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk()
      .expectHeader(Tag.FileMetaInformationVersion)
      .expectValueChunk()
  }


  "The DICOM whitelist filter" should "filter elements not matching the whitelist condition" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ fmiVersion ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(tagFilter(tagPath => groupNumber(tagPath.tag) < 8, keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "filter elements not matching the whitelist condition when testing with sample dicom files" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(partFlow)
      .via(tagFilter(tagPath => tagPath.tag != Tag.PatientName, keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The deflate flow" should "recreate the dicom parts of a dataset which has been deflated and inflated again" in {
    val bytes = fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(deflateDatasetFlow())
      .via(modifyFlow(
        TagModification.contains(TagPath.fromTag(Tag.FileMetaInformationGroupLength), _ => fmiGroupLength(tsuidDeflatedExplicitLE), insert = false),
        TagModification.contains(TagPath.fromTag(Tag.TransferSyntaxUID), _ => tsuidDeflatedExplicitLE.drop(8), insert = false)))
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

  it should "not ouput anything when the stream is empty" in {
    val bytes = ByteString.empty

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(deflateDatasetFlow())

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  "A collect attributes flow" should "first produce an attributes part followed by the input dicom parts" in {
    val bytes = studyDate ++ patientNameJohnDoe
    val tags = Set(Tag.StudyDate, Tag.PatientName)
    val source = Source.single(bytes)
      .via(partFlow)
      .via(collectAttributesFlow(tags))

    source.runWith(TestSink.probe[DicomPart])
      .request(1)
      .expectNextChainingPF {
        case DicomAttributes(attributes) =>
          attributes should have length 2
          attributes.head.header.tag shouldBe Tag.StudyDate
          attributes(1).header.tag shouldBe Tag.PatientName
      }
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "produce an empty attributes part when stream is empty" in {
    val bytes = ByteString.empty

    val source = Source.single(bytes)
      .via(partFlow)
      .via(collectAttributesFlow(Set.empty))

    source.runWith(TestSink.probe[DicomPart])
      .request(1)
      .expectNextChainingPF {
        case DicomAttributes(attributes) => attributes shouldBe empty
      }
      .expectDicomComplete()
  }

  it should "produce an empty attributes part when no relevant attributes are present" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(partFlow)
      .via(collectAttributesFlow(Set(Tag.Modality, Tag.SeriesInstanceUID)))

    source.runWith(TestSink.probe[DicomPart])
      .request(1)
      .expectNextChainingPF {
        case DicomAttributes(attributes) => attributes shouldBe empty
      }
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "apply the stop tag appropriately" in {
    val bytes = studyDate ++ patientNameJohnDoe ++ pixelData(2000)

    val source = Source.single(bytes)
      .via(new DicomPartFlow(chunkSize = 500))
      .via(collectAttributesFlow(Set(Tag.StudyDate, Tag.PatientName), maxBufferSize = 1000))

    source.runWith(TestSink.probe[DicomPart])
      .request(1)
      .expectNextChainingPF {
        case DicomAttributes(attributes) =>
          attributes should have length 2
          attributes.head.header.tag shouldBe Tag.StudyDate
          attributes.last.header.tag shouldBe Tag.PatientName
      }
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.PixelData)
      .expectValueChunk()
      .expectValueChunk()
      .expectValueChunk()
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "fail if max buffer size is exceeded" in {
    val bytes = studyDate ++ patientNameJohnDoe ++ pixelData(2000)

    val source = Source.single(bytes)
      .via(new DicomPartFlow(chunkSize = 500))
      .via(collectAttributesFlow(_ == Tag.PatientName, _ > Tag.PixelData, maxBufferSize = 1000))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomError()
  }

  "The bulk data filter flow" should "remove pixel data" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ pixelData(1000)

    val source = Source.single(bytes)
      .via(partFlow)
      .via(bulkDataFilter)

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

  it should "not remove pixel data in sequences" in {
    val bytes = sequence(Tag.DerivationCodeSequence) ++ item ++ patientNameJohnDoe ++ pixelData(100) ++ itemEnd ++ sequenceEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(bulkDataFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem(1)
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.PixelData)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "only remove waveform data when inside waveform sequence" in {
    val bytes = waveformSeqStart ++ item ++ patientNameJohnDoe ++ waveformData(100) ++ itemEnd ++ sequenceEnd ++ patientNameJohnDoe ++ waveformData(100)

    val source = Source.single(bytes)
      .via(partFlow)
      .via(bulkDataFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.WaveformSequence)
      .expectItem(1)
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectHeader(Tag.WaveformData)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The FMI group length flow" should "calculate and emit the correct group length attribute" in {
    val correctLength = tsuidExplicitLE.length
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE.drop(2)) ++ tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk(intToBytesLE(correctLength))
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "work also in flows with file meta information only" in {
    val correctLength = tsuidExplicitLE.length
    val bytes = preamble ++ tsuidExplicitLE // missing file meta information group length

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk(intToBytesLE(correctLength))
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "work in flows without preamble" in {
    val correctLength = tsuidExplicitLE.length
    val bytes = tsuidExplicitLE ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk(intToBytesLE(correctLength))
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "not emit anything in empty flows" in {
    val bytes = ByteString.empty

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  it should "not emit a group length attribute when there is no FMI" in {
    val bytes = preamble ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectPreamble()
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "keep a zero length group length attribute" in {
    val bytes = fmiGroupLength(ByteString.empty) ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk(ByteString(0, 0, 0, 0))
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "ignore DICOM parts of unknown type" in {
    case object SomePart extends DicomPart {
      def bigEndian: Boolean = false
      def bytes: ByteString = ByteString.empty
    }

    val correctLength = tsuidExplicitLE.length
    val bytes = preamble ++ tsuidExplicitLE // missing file meta information group length

    val source = Source.single(bytes)
      .via(partFlow)
      .prepend(Source.single(SomePart))
      .via(fmiGroupLengthFlow)

    source.runWith(TestSink.probe[DicomPart])
      .request(1)
      .expectNextChainingPF {
        case SomePart => true
      }
      .expectPreamble()
      .expectHeader(Tag.FileMetaInformationGroupLength)
      .expectValueChunk(intToBytesLE(correctLength))
      .expectHeader(Tag.TransferSyntaxUID)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The sequence length filter" should "replace determinate length sequences and items with indeterminate, and insert delimitations" in {
    val bytes =
      sequence(Tag.DerivationCodeSequence, 56) ++ item(16) ++ studyDate ++ item() ++ studyDate ++ itemEnd ++
        sequence(Tag.AbstractPriorCodeSequence) ++ item() ++ studyDate ++ itemEnd ++ item(16) ++ studyDate ++ sequenceEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(sequenceLengthFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
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
      .expectItem(2)
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
      .via(sequenceLengthFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "should not remove length from items in fragments" in {
    val bytes =
      pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragmentsEnd ++
        sequence(Tag.DerivationCodeSequence, 40) ++ item(32) ++
        pixeDataFragments ++ fragment(4) ++ ByteString(1, 2, 3, 4) ++ fragmentsEnd

    val source = Source.single(bytes)
      .via(partFlow)
      .via(sequenceLengthFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectFragments()
      .expectFragment(1, 4)
      .expectValueChunk()
      .expectFragmentsDelimitation()
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
      .expectFragments()
      .expectFragment(1, 4)
      .expectValueChunk()
      .expectFragmentsDelimitation()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

  it should "work in datasets with nested sequences" in {
    val bytes = studyDate ++ sequence(Tag.DerivationCodeSequence, 60) ++ item(52) ++ studyDate ++
      sequence(Tag.DerivationCodeSequence, 24) ++ item(16) ++ studyDate ++ patientNameJohnDoe

    val source = Source.single(bytes)
      .via(partFlow)
      .via(sequenceLengthFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
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
      .via(sequenceLengthFilter)

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectItem(1, -1)
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectItemDelimitation()
      .expectItem(2, -1)
      .expectItemDelimitation()
      .expectItem(3, -1)
      .expectSequence(Tag.DerivationCodeSequence, -1)
      .expectSequenceDelimitation()
      .expectItemDelimitation()
      .expectSequenceDelimitation()
      .expectDicomComplete()
  }

}

