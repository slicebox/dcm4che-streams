package se.nimsa.dcm4che.streams

import java.util.zip.Deflater

import akka.stream.testkit.TestSubscriber
import akka.util.ByteString
import se.nimsa.dcm4che.streams.DicomFlows.{DicomAttribute, DicomFragment}
import se.nimsa.dcm4che.streams.DicomPartFlow._

object DicomData {

  def deflate(bytes: ByteString, gzip: Boolean = false): ByteString = {
    val deflater = if (gzip) new Deflater() else new Deflater(-1, true)
    deflater.setInput(bytes.toArray)
    val buffer = new Array[Byte](bytes.length)
    var out = ByteString.empty
    var n = 1
    while (n > 0) {
      n = deflater.deflate(buffer, 0, buffer.length, Deflater.FULL_FLUSH)
      out = out ++ ByteString.fromArray(buffer.take(n))
    }
    out
  }

  def intToBytesLE(i: Int): ByteString = ByteString(i.toByte, (i >> 8).toByte, (i >> 16).toByte, (i >> 24).toByte)

  val preamble = ByteString.fromArray(new Array[Byte](128)) ++ ByteString('D', 'I', 'C', 'M')

  def fmiGroupLength(fmis: ByteString*) = ByteString(2, 0, 0, 0, 85, 76, 4, 0) ++ intToBytesLE(fmis.map(_.length).sum)

  val tsuidExplicitLE = ByteString(2, 0, 16, 0, 85, 73, 20, 0, '1', '.', '2', '.', '8', '4', '0', '.', '1', '0', '0', '0', '8', '.', '1', '.', '2', '.', '1', 0)
  val tsuidExplicitBE = ByteString(2, 0, 16, 0, 85, 73, 20, 0, '1', '.', '2', '.', '8', '4', '0', '.', '1', '0', '0', '0', '8', '.', '1', '.', '2', '.', '2', 0)
  val tsuidImplicitLE = ByteString(2, 0, 16, 0, 85, 73, 18, 0, '1', '.', '2', '.', '8', '4', '0', '.', '1', '0', '0', '0', '8', '.', '1', '.', '2', 0)
  val tsuidDeflatedExplicitLE = ByteString(2, 0, 16, 0, 85, 73, 22, 0, '1', '.', '2', '.', '8', '4', '0', '.', '1', '0', '0', '0', '8', '.', '1', '.', '2', '.', '1', '.', '9', '9')
  val patientNameJohnDoe = ByteString(16, 0, 16, 0, 80, 78, 8, 0, 'J', 'o', 'h', 'n', '^', 'D', 'o', 'e')
  val patientNameJohnDoeBE = ByteString(0, 16, 0, 16, 80, 78, 0, 8, 'J', 'o', 'h', 'n', '^', 'D', 'o', 'e')
  val patientNameJohnDoeImplicit = ByteString(16, 0, 16, 0, 8, 0, 0, 0, 'J', 'o', 'h', 'n', '^', 'D', 'o', 'e')
  val studyDate = ByteString(8, 0, 32, 0, 84, 77, 10, 0, 49, 56, 51, 49, 51, 56, 46, 55, 54, 53)

  def itemStart(length: Byte) = ByteString(254, 255, 0, 224, length, 0, 0, 0)

  val itemNoLength = ByteString(254, 255, 0, 224, -1, -1, -1, -1)

  val itemEnd = ByteString(254, 255, 13, 224, 0, 0, 0, 0)
  val seqEnd = ByteString(254, 255, 221, 224, 0, 0, 0, 0)
  val seqEndNonZeroLength = ByteString(254, 255, 221, 224, 10, 0, 0, 0)
  val pixeDataFragments = ByteString(224, 127, 16, 0, 79, 87, 0, 0, 255, 255, 255, 255) // VR = OW, length = -1

  val seqStart = ByteString(0x08, 0x00, 0x15, 0x92, 'S', 'Q', 0, 0, -1, -1, -1, -1)

  implicit class DicomPartProbe(probe: TestSubscriber.Probe[DicomPart]) {
    def expectPreamble() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomPreamble => true
      }

    def expectValueChunk() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomValueChunk => true
      }

    def expectItem() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomItem => true
      }

    def expectItemDelimitation() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomItemDelimitation => true
      }

    def expectFragments() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragments => true
      }

    def expectFragmentsDelimitation() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragmentsDelimitation => true
      }

    def expectHeader(tag: Int) = probe
      .request(1)
      .expectNextChainingPF {
        case h: DicomHeader if h.tag == tag => true
      }

    def expectSequence(tag: Int) = probe
      .request(1)
      .expectNextChainingPF {
        case h: DicomSequence if h.tag == tag => true
      }

    def expectSequenceDelimitation() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomSequenceDelimitation => true
      }

    def expectUnknownPart() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomUnknownPart => true
      }

    def expectDeflatedChunk() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomDeflatedChunk => true
      }

    def expectAttribute(tag: Int) = probe
      .request(1)
      .expectNextChainingPF {
        case a: DicomAttribute if a.header.tag == tag => true
      }

    def expectFragment() = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragment => true
      }

    def expectDicomComplete() = probe
      .request(1)
      .expectComplete()

    def expectDicomError() = probe
      .request(1)
      .expectError()
  }

}