package se.nimsa.dcm4che.streams

import java.util.zip.Deflater

import akka.stream.testkit.TestSubscriber
import akka.util.ByteString
import org.dcm4che3.data.VR
import se.nimsa.dcm4che.streams.DicomParts._

object TestUtils {

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

  type PartProbe = TestSubscriber.Probe[DicomPart]

  implicit class DicomPartProbe(probe: PartProbe) {
    def expectPreamble(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomPreamble => true
      }

    def expectValueChunk(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomValueChunk => true
      }

    def expectValueChunk(bytes: ByteString): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case chunk: DicomValueChunk => chunk.bytes == bytes
      }

    def expectItem(index: Int): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case item: DicomItem => item.index == index
      }

    def expectItemDelimitation(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomItemDelimitation => true
      }

    def expectFragments(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragments => true
      }

    def expectFragmentsDelimitation(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragmentsDelimitation => true
      }

    def expectHeader(tag: Int): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case h: DicomHeader if h.tag == tag => true
      }

    def expectHeader(tag: Int, vr: VR, length: Long): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case h: DicomHeader if h.tag == tag && h.vr == vr && h.length == length => true
      }

    def expectSequence(tag: Int): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case h: DicomSequence if h.tag == tag => true
      }

    def expectSequenceDelimitation(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomSequenceDelimitation => true
      }

    def expectUnknownPart(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomUnknownPart => true
      }

    def expectDeflatedChunk(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomDeflatedChunk => true
      }

    def expectAttribute(tag: Int): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case a: DicomAttribute if a.header.tag == tag => true
      }

    def expectFragment(): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case _: DicomFragment => true
      }

    def expectDicomComplete(): PartProbe = probe
      .request(1)
      .expectComplete()

    def expectDicomError(): Throwable = probe
      .request(1)
      .expectError()

    def expectAttributesPart(attributesPart: DicomAttributes): PartProbe = probe
      .request(1)
      .expectNextChainingPF {
        case p: DicomAttributes => p == attributesPart
      }
  }

}
