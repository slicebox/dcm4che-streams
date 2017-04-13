/*
 * Copyright 2017 Lars Edenbrandt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.nimsa.dcm4che.streams

import java.util.zip.Inflater

import akka.stream._
import akka.stream.stage._
import akka.util.ByteString
import org.dcm4che3.data.{ElementDictionary, VR}
import org.dcm4che3.io.DicomStreamException
import org.dcm4che3.util.TagUtils
import se.nimsa.dcm4che.streams.DicomParsing.DICOM_PREAMBLE_LENGTH
import se.nimsa.dcm4che.streams.DicomPartFlow._

/**
  * Flow which ingests a stream of bytes and outputs a stream of DICOM file parts such as specified by the <code>DicomPart</code>
  * trait. Example DICOM parts are the preamble, headers (tag, VR, length), value chunks (the data in an attribute divided into chunks),
  * items, sequences and fragments.
  *
  * This class is heavily and exclusively based on the dcm4che
  * <a href="https://github.com/dcm4che/dcm4che/blob/master/dcm4che-core/src/test/java/org/dcm4che3/io/DicomInputStreamTest.java">DicomInputStream</a>
  * class, but adapted to output streaming results using AKKA Streams.
  *
  * @param chunkSize the maximum size of a DICOM attribute data chunk
  * @param stopTag optional stop tag (exclusive) after which reading of incoming data bytes is stopped
  * @param inflate indicates whether deflated DICOM data should be deflated and parsed or passed on as deflated data chunks.
  */
class DicomPartFlow(chunkSize: Int = 8192, stopTag: Option[Int] = None, inflate: Boolean = true) extends ByteStringParser[DicomPart] with DicomParsing {

  import ByteStringParser._

  val transferSyntaxLengthLimit = 1024

  protected class DicomParsingLogic extends ParsingLogic with StageLogging {

    sealed trait HeaderState {
      val bigEndian: Boolean
      val explicitVR: Boolean
    }

    case class DatasetHeaderState(bigEndian: Boolean, explicitVR: Boolean) extends HeaderState

    case class FmiHeaderState(tsuid: Option[String], bigEndian: Boolean, explicitVR: Boolean, hasFmi: Boolean, pos: Long, fmiEndPos: Option[Long]) extends HeaderState

    case class ValueState(bigEndian: Boolean, bytesLeft: Int, nextStep: ParseStep[DicomPart])

    case class FragmentsState(bigEndian: Boolean, explicitVR: Boolean) extends HeaderState

    abstract class DicomParseStep extends ParseStep[DicomPart] {
      override def onTruncation(reader: ByteReader): Unit = throw new DicomStreamException("DICOM file is truncated")
    }

    case object AtBeginning extends DicomParseStep {
      def parse(reader: ByteReader) = {
        val maybePreamble =
          if (!isUpstreamClosed || reader.remainingSize >= DICOM_PREAMBLE_LENGTH) {
            reader.ensure(DICOM_PREAMBLE_LENGTH)
            if (DicomParsing.isPreamble(reader.remainingData.take(DICOM_PREAMBLE_LENGTH)))
              Some(DicomPreamble(bytes = reader.take(DICOM_PREAMBLE_LENGTH)))
            else None
          }
          else None
        if (maybePreamble.isDefined && !reader.hasRemaining && isUpstreamClosed)
          ParseResult(maybePreamble, FinishedParser)
        else {
          reader.ensure(8)
          DicomParsing.dicomInfo(reader.remainingData.take(8)).map { info =>
            val nextState = if (info.hasFmi)
              InFmiHeader(FmiHeaderState(None, info.bigEndian, info.explicitVR, info.hasFmi, 0, None))
            else
              InDatasetHeader(DatasetHeaderState(info.bigEndian, info.explicitVR), None)
            ParseResult(maybePreamble, nextState)
          }.getOrElse(throw new DicomStreamException("Not a DICOM stream"))
        }
      }
    }

    case class InFmiHeader(state: FmiHeaderState) extends DicomParseStep {
      def parse(reader: ByteReader) = {
        val (tag, vr, headerLength, valueLength) = readHeader(reader, state)
        if (DicomParsing.groupNumber(tag) != 2) {
          log.warning("Missing or wrong File Meta Information Group Length (0002,0000)")
          reader.ensure(valueLength + 2)
          ParseResult(None, toDatasetStep(reader.remainingData.drop(valueLength).take(2), state))
        } else {
          // no meta attributes can lead to vr = null
          val updatedVr = if (vr == VR.UN) ElementDictionary.getStandardElementDictionary.vrOf(tag) else vr
          val bytes = reader.take(headerLength)
          val updatedPos = state.pos + headerLength + valueLength
          val updatedState = tag match {
            case 0x00020000 => // meta info length
              reader.ensure(4)
              val valueBytes = reader.remainingData.take(4)
              state.copy(pos = updatedPos, fmiEndPos = Some(updatedPos + DicomParsing.bytesToInt(valueBytes, 0, state.bigEndian)))
            case 0x00020010 => // transfer syntax
              if (valueLength < transferSyntaxLengthLimit) {
                reader.ensure(valueLength)
                val valueBytes = reader.remainingData.take(valueLength)
                state.copy(tsuid = Some(valueBytes.utf8String.trim), pos = updatedPos)
              } else {
                log.warning("Transfer syntax data is very large, skipping")
                state.copy(pos = updatedPos)
              }
            case _ =>
              state.copy(pos = updatedPos)
          }
          val part = Some(DicomHeader(tag, updatedVr, valueLength, isFmi = true, state.bigEndian, bytes))
          val nextStep = updatedState.fmiEndPos.filter(_ <= updatedPos) match {
            case Some(_) =>
              reader.ensure(valueLength + 2)
              toDatasetStep(reader.remainingData.drop(valueLength).take(2), updatedState)
            case None =>
              InFmiHeader(updatedState)
          }
          ParseResult(part, InValue(ValueState(updatedState.bigEndian, valueLength, nextStep)))
        }
      }
    }

    case class InDatasetHeader(state: DatasetHeaderState, inflater: Option[InflateData]) extends DicomParseStep {
      def parse(reader: ByteReader) = {
        val attribute = readDatasetHeader(reader, state)
        val nextState = attribute.map {
          case DicomHeader(_, _, length, _, bigEndian, _) => InValue(ValueState(bigEndian, length, InDatasetHeader(state, inflater)))
          case DicomFragments(_, _, bigEndian, _) => InFragments(FragmentsState(bigEndian, state.explicitVR), inflater)
          case _ => InDatasetHeader(state, inflater)
        }.getOrElse(FinishedParser)
        ParseResult(attribute, nextState)
      }
    }

    case class InValue(state: ValueState) extends DicomParseStep {
      def parse(reader: ByteReader) = {
        val parseResult =
          if (state.bytesLeft <= chunkSize)
            ParseResult(Some(DicomValueChunk(state.bigEndian, reader.take(state.bytesLeft), last = true)), state.nextStep)
          else
            ParseResult(Some(DicomValueChunk(state.bigEndian, reader.take(chunkSize), last = false)), InValue(state.copy(bytesLeft = state.bytesLeft - chunkSize)))
        state.nextStep match {
          case ds: InDatasetHeader if ds.inflater.isDefined && !isInflating =>
            startInflating(ds.inflater.get, reader)
          case _ =>
        }
        parseResult
      }
    }

    case class InFragments(state: FragmentsState, inflater: Option[InflateData]) extends DicomParseStep {
      def parse(reader: ByteReader) = {
        val (tag, _, headerLength, valueLength) = readHeader(reader, state)
        tag match {
          case 0xFFFEE000 => // begin item
            ParseResult(Some(DicomItem(valueLength, state.bigEndian, reader.take(headerLength))), InValue(ValueState(state.bigEndian, valueLength, this)))
          case 0xFFFEE0DD => // end fragments
            if (valueLength != 0) {
              log.warning(s"Unexpected fragments delimitation length $valueLength")
            }
            ParseResult(Some(DicomFragmentsDelimitation(state.bigEndian, reader.take(headerLength))), InDatasetHeader(DatasetHeaderState(state.bigEndian, state.explicitVR), inflater))
          case _ =>
            log.warning(s"Unexpected attribute (${TagUtils.toHexString(tag)}) in fragments with length=$valueLength")
            ParseResult(Some(DicomUnknownPart(state.bigEndian, reader.take(headerLength + valueLength))), this)
        }
      }
    }

    case class InDeflatedData(bigEndian: Boolean) extends DicomParseStep {
      def parse(reader: ByteReader) = ParseResult(Some(DicomDeflatedChunk(bigEndian, reader.take(chunkSize))), this)

      override def onTruncation(reader: ByteReader): Unit = {
        emit(objOut, DicomDeflatedChunk(bigEndian, reader.takeAll()))
        completeStage()
      }
    }

    def toDatasetStep(firstTwoBytes: ByteString, state: FmiHeaderState): DicomParseStep = {
      val tsuid = state.tsuid.getOrElse {
        log.warning("Missing Transfer Syntax (0002,0010) - assume Explicit VR Little Endian")
        "1.2.840.10008.1.2.1"
      }

      val deflatedTs = tsuid == "1.2.840.10008.1.2.1.99" || tsuid == "1.2.840.10008.1.2.4.95"

      if (deflatedTs)
        if (inflate) {
          val inflater =
            if (hasZLIBHeader(firstTwoBytes)) {
              log.warning("Deflated DICOM Stream with ZLIB Header")
              new Inflater()
            }
            else
              new Inflater(true)
          InDatasetHeader(DatasetHeaderState(
            bigEndian = tsuid == "1.2.840.10008.1.2.2",
            explicitVR = tsuid != "1.2.840.10008.1.2"),
            Some(InflateData(inflater, new Array[Byte](chunkSize))))
        }
        else
          InDeflatedData(state.bigEndian)
      else
        InDatasetHeader(DatasetHeaderState(
          bigEndian = tsuid == "1.2.840.10008.1.2.2",
          explicitVR = tsuid != "1.2.840.10008.1.2"),
          None)
    }

    private def hasZLIBHeader(firstTwoBytes: ByteString): Boolean = {
      bytesToUShortBE(firstTwoBytes, 0) == 0x789C
    }

    def readHeader(reader: ByteReader, dicomState: HeaderState): (Int, VR, Int, Int) = {
      reader.ensure(8)
      val tagVr = reader.remainingData.take(8)
      val (tag, vr) = DicomParsing.tagVr(tagVr, dicomState.bigEndian, dicomState.explicitVR)
      if (vr == null)
        (tag, vr, 8, bytesToInt(tagVr, 4, dicomState.bigEndian))
      else if (dicomState.explicitVR)
        if (vr.headerLength == 8)
          (tag, vr, 8, bytesToUShort(tagVr, 6, dicomState.bigEndian))
        else {
          reader.ensure(12)
          (tag, vr, 12, bytesToInt(reader.remainingData, 8, dicomState.bigEndian))
        }
      else
        (tag, VR.UN, 8, bytesToInt(tagVr, 4, dicomState.bigEndian))
    }

    def readDatasetHeader(reader: ByteReader, state: DatasetHeaderState): Option[DicomPart] = {
      val (tag, vr, headerLength, valueLength) = readHeader(reader, state)
      // println(s"$tag $vr $headerLength $valueLength")
      if (stopTag.isDefined && tag == stopTag.get)
        None
      else if (vr != null) {
        val updatedVr1 = if (vr == VR.UN) ElementDictionary.getStandardElementDictionary.vrOf(tag) else vr
        val updatedVr2 = if ((updatedVr1 == VR.UN) && valueLength == -1) VR.SQ else updatedVr1
        val bytes = reader.take(headerLength)
        if (vr == VR.SQ)
          Some(DicomSequence(tag, state.bigEndian, bytes))
        else if (valueLength == -1)
          Some(DicomFragments(tag, vr, state.bigEndian, bytes))
        else
          Some(DicomHeader(tag, updatedVr2, valueLength, isFmi = false, state.bigEndian, bytes))
      } else
        tag match {
          case 0xFFFEE000 => Some(DicomItem(valueLength, state.bigEndian, reader.take(8)))
          case 0xFFFEE00D => Some(DicomItemDelimitation(state.bigEndian, reader.take(8)))
          case 0xFFFEE0DD => Some(DicomSequenceDelimitation(state.bigEndian, reader.take(8)))
          case _ => Some(DicomUnknownPart(state.bigEndian, reader.take(headerLength))) // cannot happen
        }
    }

    startWith(AtBeginning)
  }

  override def createLogic(attr: Attributes) = new DicomParsingLogic()

}

object DicomPartFlow {

  trait DicomPart {
    def bigEndian: Boolean

    def bytes: ByteString
  }

  case class DicomPreamble(bigEndian: Boolean = false, bytes: ByteString) extends DicomPart

  case class DicomHeader(tag: Int, vr: VR, length: Int, isFmi: Boolean, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomValueChunk(bigEndian: Boolean, bytes: ByteString, last: Boolean) extends DicomPart

  case class DicomDeflatedChunk(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomItem(length: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomItemDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequence(tag: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequenceDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragments(tag: Int, vr: VR, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragmentsDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomUnknownPart(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  val partFlow = new DicomPartFlow()
}
