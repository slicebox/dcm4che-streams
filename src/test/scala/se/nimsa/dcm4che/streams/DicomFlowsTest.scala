package se.nimsa.dcm4che.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data._
import org.scalatest.{FlatSpecLike, Matchers}
import se.nimsa.dcm4che.streams.DicomPartFlow.partFlow
import se.nimsa.dcm4che.streams.DicomParts.{DicomAttributes, DicomPart}


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

  it should "also apply to attributes in sequences" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter(Seq(Tag.StudyDate)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "also work on fragments" in {
    val bytes = pixeDataFragments ++ itemStart(4) ++ ByteString(1, 2, 3, 4) ++ itemStart(4) ++ ByteString(5, 6, 7, 8) ++ seqEnd

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(whitelistFilter(Seq.empty))

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

  it should "discard group length elements except 0002,0000 when testing with dicom file" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(DicomPartFlow.partFlow)
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
      .via(DicomPartFlow.partFlow)
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
      .via(blacklistFilter((tag: Int) => DicomParsing.isFileMetaInformation(tag), keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.StudyDate)
      .expectValueChunk()
      .expectDicomComplete()
  }

  it should "filter elements matching the blacklist condition when testing with sample dicom files" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(new DicomPartFlow())
      .via(blacklistFilter((tag: Int) => DicomParsing.isFileMetaInformation(tag), keepPreamble = false))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.SpecificCharacterSet)
      .expectValueChunk()
      .expectHeader(Tag.ImageType)
  }


  it should "filter leave the dicom file unchanged when blacklist condition does not match any attribute" in {
    val file = new File(getClass.getResource("CT0055.dcm").toURI)
    val source = FileIO.fromPath(file.toPath)
      .via(new DicomPartFlow())
      .via(blacklistFilter(DicomParsing.isPrivateAttribute))

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
      .via(whitelistFilter((tag: Int) => DicomParsing.groupNumber(tag) >= 8))

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
      .via(DicomPartFlow.partFlow)
      .via(whitelistFilter(_ == Tag.PatientName))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName)
      .expectValueChunk()
      .expectDicomComplete()
  }

  "The modify flow" should "modify the value of the specified attributes" in {
    val bytes = patientNameJohnDoe ++ studyDate

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(
        TagModification(Tag.PatientName, _ => mikeBytes, insert = false),
        TagModification(Tag.StudyDate, _ => ByteString.empty, insert = false)))

    source.runWith(TestSink.probe[DicomPart])
      .expectHeader(Tag.PatientName, VR.PN, mikeBytes.length)
      .expectValueChunk(mikeBytes)
      .expectHeader(Tag.StudyDate, VR.DA, 0)
      .expectValueChunk(ByteString.empty)
      .expectDicomComplete()
  }

  it should "not modify attributes in sequences" in {
    val bytes = seqStart ++ itemNoLength ++ patientNameJohnDoe ++ studyDate ++ itemEnd ++ seqEnd

    val mikeBytes = ByteString('M', 'i', 'k', 'e')

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(modifyFlow(TagModification(Tag.PatientName, _ => mikeBytes, insert = false)))

    source.runWith(TestSink.probe[DicomPart])
      .expectSequence(Tag.DerivationCodeSequence)
      .expectItem()
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
      .via(modifyFlow(TagModification(Tag.StudyDate, _ => studyDate.drop(8), insert = true)))

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
      .via(modifyFlow(TagModification(Tag.PatientName, _ => patientNameJohnDoe.drop(8), insert = true)))

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
        TagModification(Tag.SeriesDate, _ => studyDate.drop(8), insert = true),
        TagModification(Tag.StudyDate, _ => studyDate.drop(8), insert = true)))

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
      .via(modifyFlow(TagModification(Tag.SeriesDate, _ => studyDate.drop(8), insert = true)))

    source.runWith(TestSink.probe[DicomPart])
      .expectDicomComplete()
  }

  "The deflate flow" should "recreate the dicom parts of a dataset which has been deflated and inflated again" in {
    val bytes = fmiGroupLength(tsuidExplicitLE) ++ tsuidExplicitLE ++ patientNameJohnDoe ++ studyDate

    val source = Source.single(bytes)
      .via(new DicomPartFlow())
      .via(deflateDatasetFlow())
      .via(modifyFlow(
        TagModification(Tag.FileMetaInformationGroupLength, _ => fmiGroupLength(tsuidDeflatedExplicitLE), insert = false),
        TagModification(Tag.TransferSyntaxUID, _ => tsuidDeflatedExplicitLE.drop(8), insert = false)))
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

}

