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
import org.dcm4che3.data.{ElementDictionary, Tag, UID, VR}
import org.dcm4che3.io.DicomStreamException
import se.nimsa.dcm4che.streams.DicomParts._

/**
  * Flow which ingests a stream of bytes and outputs a stream of DICOM data parts as specified by the <code>DicomPart</code>
  * trait. Example DICOM parts are the preamble, headers (tag, VR, length), value chunks (the data in an attribute divided into chunks),
  * items, sequences and fragments.
  *
  * This class is heavily and exclusively based on the dcm4che
  * <a href="https://github.com/dcm4che/dcm4che/blob/master/dcm4che-core/src/test/java/org/dcm4che3/io/DicomInputStreamTest.java">DicomInputStream</a>
  * class, but adapted to output streaming results using AKKA Streams.
  *
  * @param chunkSize the maximum size of a DICOM attribute data chunk
  * @param stopTag   optional stop tag (exclusive) after which reading of incoming data bytes is stopped
  * @param inflate   indicates whether deflated DICOM data should be deflated and parsed or passed on as deflated data chunks.
  */
class DicomParseFlow(chunkSize: Int = 8192, stopTag: Option[Int] = None, inflate: Boolean = true) extends ByteStringParser[DicomPart] with DicomParsing {

  import ByteStringParser._

  val transferSyntaxLengthLimit = 1024

  protected class DicomParsingLogic extends ParsingLogic with StageLogging {

    sealed trait HeaderState {
      val bigEndian: Boolean
      val explicitVR: Boolean
    }

    case class DatasetHeaderState(itemIndex: Int, bigEndian: Boolean, explicitVR: Boolean) extends HeaderState

    case class FmiHeaderState(tsuid: Option[String], bigEndian: Boolean, explicitVR: Boolean, hasFmi: Boolean, pos: Long, fmiEndPos: Option[Long]) extends HeaderState

    case class ValueState(bigEndian: Boolean, bytesLeft: Long, nextStep: ParseStep[DicomPart])

    case class FragmentsState(fragmentIndex: Int, bigEndian: Boolean, explicitVR: Boolean) extends HeaderState

    abstract class DicomParseStep extends ParseStep[DicomPart] {
      override def onTruncation(reader: ByteReader): Unit = throw new DicomStreamException("DICOM file is truncated")
    }

    case object AtBeginning extends DicomParseStep {
      def parse(reader: ByteReader): ParseResult[DicomPart] = {
        val maybePreamble =
          if (!isUpstreamClosed || reader.remainingSize >= dicomPreambleLength) {
            reader.ensure(dicomPreambleLength)
            if (isPreamble(reader.remainingData.take(dicomPreambleLength)))
              Some(DicomPreamble(bytes = reader.take(dicomPreambleLength)))
            else None
          }
          else None
        if (maybePreamble.isDefined && !reader.hasRemaining && isUpstreamClosed)
          ParseResult(maybePreamble, FinishedParser)
        else {
          reader.ensure(8)
          dicomInfo(reader.remainingData.take(8)).map { info =>
            val nextState = if (info.hasFmi)
              InFmiHeader(FmiHeaderState(None, info.bigEndian, info.explicitVR, info.hasFmi, 0, None))
            else
              InDatasetHeader(DatasetHeaderState(0, info.bigEndian, info.explicitVR), None)
            ParseResult(maybePreamble, nextState)
          }.getOrElse(throw new DicomStreamException("Not a DICOM stream"))
        }
      }
    }

    case class InFmiHeader(state: FmiHeaderState) extends DicomParseStep {
      def parse(reader: ByteReader): ParseResult[DicomPart] = {
        val (tag, vr, headerLength, valueLength) = readHeader(reader, state)
        if (groupNumber(tag) != 2) {
          log.warning("Missing or wrong File Meta Information Group Length (0002,0000)")
          ParseResult(None, toDatasetStep(ByteString(0, 0), state))
        } else {
          // no meta attributes can lead to vr = null
          val updatedVr = if (vr == VR.UN) ElementDictionary.getStandardElementDictionary.vrOf(tag) else vr
          val bytes = reader.take(headerLength)
          val updatedPos = state.pos + headerLength + valueLength
          val updatedState = tag match {
            case Tag.FileMetaInformationGroupLength =>
              reader.ensure(4)
              val valueBytes = reader.remainingData.take(4)
              state.copy(pos = updatedPos, fmiEndPos = Some(updatedPos + intToUnsignedLong(bytesToInt(valueBytes, state.bigEndian))))
            case Tag.TransferSyntaxUID =>
              if (valueLength < transferSyntaxLengthLimit) {
                reader.ensure(valueLength.toInt)
                val valueBytes = reader.remainingData.take(valueLength.toInt)
                state.copy(tsuid = Some(valueBytes.utf8String.trim), pos = updatedPos)
              } else {
                log.warning("Transfer syntax data is very large, skipping")
                state.copy(pos = updatedPos)
              }
            case _ =>
              state.copy(pos = updatedPos)
          }
          val part = Some(DicomHeader(tag, updatedVr, valueLength, isFmi = true, state.bigEndian, state.explicitVR, bytes))
          val nextStep = updatedState.fmiEndPos.filter(_ <= updatedPos) match {
            case Some(_) =>
              reader.ensure(valueLength.toInt + 2)
              toDatasetStep(reader.remainingData.drop(valueLength.toInt).take(2), updatedState)
            case None =>
              InFmiHeader(updatedState)
          }
          ParseResult(part, InValue(ValueState(updatedState.bigEndian, valueLength, nextStep)), acceptUpstreamFinish = false)
        }
      }
    }

    case class InDatasetHeader(state: DatasetHeaderState, inflater: Option[InflateData]) extends DicomParseStep {
      def parse(reader: ByteReader): ParseResult[DicomPart] = {
        val part = readDatasetHeader(reader, state)
        val nextState = part.map {
          case DicomHeader(_, _, length, _, bigEndian, _, _) =>
            if (length > 0)
              InValue(ValueState(bigEndian, length, InDatasetHeader(state, inflater)))
            else
              InDatasetHeader(state, inflater)
          case DicomFragments(_, _, _, bigEndian, _) => InFragments(FragmentsState(fragmentIndex = 0, bigEndian, state.explicitVR), inflater)
          case DicomSequence(_, _, _, _) => InDatasetHeader(state.copy(itemIndex = 0), inflater)
          case DicomSequenceItem(index, _, _, _) => InDatasetHeader(state.copy(itemIndex = index), inflater)
          case DicomSequenceItemDelimitation(index, _, _) => InDatasetHeader(state.copy(itemIndex = index), inflater)
          case _ => InDatasetHeader(state, inflater)
        }.getOrElse(FinishedParser)
        ParseResult(part, nextState, acceptUpstreamFinish = !nextState.isInstanceOf[InValue])
      }
    }

    case class InValue(state: ValueState) extends DicomParseStep {
      def parse(reader: ByteReader): ParseResult[DicomPart] = {
        val parseResult =
          if (state.bytesLeft <= chunkSize)
            ParseResult(Some(DicomValueChunk(state.bigEndian, reader.take(state.bytesLeft.toInt), last = true)), state.nextStep)
          else
            ParseResult(Some(DicomValueChunk(state.bigEndian, reader.take(chunkSize), last = false)), InValue(state.copy(bytesLeft = state.bytesLeft - chunkSize)))
        state.nextStep match {
          case ds: InDatasetHeader if ds.inflater.isDefined && !isInflating =>
            startInflating(ds.inflater.get, reader)
          case _ =>
        }
        parseResult
      }
      override def onTruncation(reader: ByteReader): Unit =
        if (reader.hasRemaining)
          super.onTruncation(reader)
        else {
          emit(objOut, DicomValueChunk(state.bigEndian, ByteString.empty, last = true))
          completeStage()
        }
    }

    case class InFragments(state: FragmentsState, inflater: Option[InflateData]) extends DicomParseStep {
      def parse(reader: ByteReader): ParseResult[DicomPart] = {
        val (tag, _, headerLength, valueLength) = readHeader(reader, state)
        tag match {

          case 0xFFFEE000 => // begin fragment
            val nextState =
              if (valueLength > 0)
                InValue(ValueState(state.bigEndian, valueLength, this.copy(state = state.copy(fragmentIndex = state.fragmentIndex + 1))))
              else
                this.copy(state = state.copy(fragmentIndex = state.fragmentIndex + 1))
            ParseResult(
              Some(DicomFragmentsItem(state.fragmentIndex + 1, valueLength, state.bigEndian, reader.take(headerLength))),
              nextState
            )

          case 0xFFFEE0DD => // end fragments
            if (valueLength != 0) {
              log.warning(s"Unexpected fragments delimitation length $valueLength")
            }
            ParseResult(Some(DicomFragmentsDelimitation(state.bigEndian, reader.take(headerLength))), InDatasetHeader(DatasetHeaderState(0, state.bigEndian, state.explicitVR), inflater))

          case _ =>
            log.warning(s"Unexpected attribute (${tagToString(tag)}) in fragments with length=$valueLength")
            ParseResult(Some(DicomUnknownPart(state.bigEndian, reader.take(headerLength + valueLength.toInt))), this)
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
        UID.ExplicitVRLittleEndian
      }

      val bigEndian = tsuid == UID.ExplicitVRBigEndianRetired
      val explicitVR = tsuid != UID.ImplicitVRLittleEndian

      if (isDeflated(tsuid))
        if (inflate) {
          val inflater =
            if (hasZLIBHeader(firstTwoBytes)) {
              log.warning("Deflated DICOM Stream with ZLIB Header")
              new Inflater()
            } else
              new Inflater(true)
          InDatasetHeader(
            DatasetHeaderState(0, bigEndian, explicitVR),
            Some(InflateData(inflater, new Array[Byte](chunkSize))))
        } else
          InDeflatedData(state.bigEndian)
      else
        InDatasetHeader(DatasetHeaderState(0, bigEndian, explicitVR), None)
    }

    private def hasZLIBHeader(firstTwoBytes: ByteString): Boolean = {
      bytesToUShortBE(firstTwoBytes) == 0x789C
    }

    def readHeader(reader: ByteReader, dicomState: HeaderState): (Int, VR, Int, Long) = {
      reader.ensure(8)
      val tagVrBytes = reader.remainingData.take(8)
      val (tag, vr) = tagVr(tagVrBytes, dicomState.bigEndian, dicomState.explicitVR)
      if (vr == null)
        (tag, vr, 8, lengthToLong(bytesToInt(tagVrBytes.drop(4), dicomState.bigEndian)))
      else if (dicomState.explicitVR)
        if (vr.headerLength == 8)
          (tag, vr, 8, lengthToLong(bytesToUShort(tagVrBytes.drop(6), dicomState.bigEndian)))
        else {
          reader.ensure(12)
          (tag, vr, 12, lengthToLong(bytesToInt(reader.remainingData.drop(8), dicomState.bigEndian)))
        }
      else
        (tag, vr, 8, lengthToLong(bytesToInt(tagVrBytes.drop(4), dicomState.bigEndian)))
    }

    def readDatasetHeader(reader: ByteReader, state: DatasetHeaderState): Option[DicomPart] = {
      val (tag, vr, headerLength, valueLength) = readHeader(reader, state)
      // println(s"$tag $vr $headerLength $valueLength")
      if (stopTag.isDefined && tag == stopTag.get)
        None
      else if (vr != null) {
        val bytes = reader.take(headerLength)
        if (vr == VR.SQ || vr == VR.UN && valueLength == -1)
          Some(DicomSequence(tag, valueLength, state.bigEndian, bytes))
        else if (valueLength == -1)
          Some(DicomFragments(tag, valueLength, vr, state.bigEndian, bytes))
        else
          Some(DicomHeader(tag, vr, valueLength, isFmi = false, state.bigEndian, state.explicitVR, bytes))
      } else
        tag match {
          case 0xFFFEE000 => Some(DicomSequenceItem(state.itemIndex + 1, valueLength, state.bigEndian, reader.take(8)))
          case 0xFFFEE00D => Some(DicomSequenceItemDelimitation(state.itemIndex, state.bigEndian, reader.take(8)))
          case 0xFFFEE0DD => Some(DicomSequenceDelimitation(state.bigEndian, reader.take(8)))
          case _ => Some(DicomUnknownPart(state.bigEndian, reader.take(headerLength))) // cannot happen
        }
    }

    startWith(AtBeginning)
  }

  override def createLogic(attr: Attributes) = new DicomParsingLogic()

}

object DicomParseFlow {

  val parseFlow = new DicomParseFlow()
}
