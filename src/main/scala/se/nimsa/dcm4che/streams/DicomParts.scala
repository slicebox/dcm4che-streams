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

import akka.util.ByteString
import org.dcm4che3.data.{SpecificCharacterSet, VR}
import org.dcm4che3.util.ByteUtils


object DicomParts {

  trait DicomPart {
    def bigEndian: Boolean

    def bytes: ByteString
  }

  case class DicomPreamble(bytes: ByteString) extends DicomPart {
    def bigEndian = false
  }

  case class DicomHeader(tag: Int, vr: VR, length: Int, isFmi: Boolean, bigEndian: Boolean, explicitVR: Boolean, bytes: ByteString) extends DicomPart {

    def updateLength(newLength: Int) : DicomHeader = {

      val updated = if ((bytes.size >= 8) && (explicitVR) && (vr.headerLength == 8)) { //explicit vr
          ByteUtils.shortToBytes(newLength, bytes.toArray, 6, bigEndian)
        } else if ((bytes.size >= 12) && (explicitVR) && (vr.headerLength == 12)) { //explicit vr
          ByteUtils.shortToBytes(newLength, bytes.toArray, 8, bigEndian)
        } else if ((bytes.size >= 8) && (!explicitVR) && (vr.headerLength == 8)) { //implicit vr
          ByteUtils.shortToBytes(newLength, bytes.toArray, 4, bigEndian)
        } else {
          ByteUtils.shortToBytes(newLength, bytes.toArray, 8, bigEndian) //implicit vr
        }

      DicomHeader(tag, vr, newLength, isFmi, bigEndian, explicitVR, ByteString.fromArray(updated))
    }

  }

  case class DicomValueChunk(bigEndian: Boolean, bytes: ByteString, last: Boolean) extends DicomPart

  case class DicomDeflatedChunk(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomItem(length: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomItemDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequence(tag: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequenceDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragments(tag: Int, vr: VR, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragmentsDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomUnknownPart(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragment(bigEndian: Boolean, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
  }

  case class DicomAttribute(header: DicomHeader, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)

    def bigEndian = header.bigEndian

    // LO: long string 64 chars max
    // SH: short string 16 chars max
    // PN: person name
    // UI: 64 chars max
    def updateStringValue(newValue: String, cs: SpecificCharacterSet = SpecificCharacterSet.ASCII): DicomAttribute = {
      val newBytes = header.vr.toBytes(newValue, cs)
      val needsPadding = newBytes.size % 2 == 1
      val newLength = if (needsPadding) newBytes.size + 1 else newBytes.size
      val updatedHeader = header.updateLength(newLength)
      val updatedValue = if (needsPadding) {
        ByteString.fromArray(newBytes :+ header.vr.paddingByte().toByte)
      } else {
        ByteString.fromArray(newBytes)
      }
      DicomAttribute(updatedHeader, Seq(DicomValueChunk(header.bigEndian, updatedValue, true)))
    }

    // DA: A string of characters of the format YYYYMMDD, 8 bytes fixed
    def updateDateValue(newValue: String, cs: SpecificCharacterSet = SpecificCharacterSet.ASCII): DicomAttribute = {
      val newBytes = header.vr.toBytes(newValue, cs)
      val updatedValue = ByteString.fromArray(newBytes)
      DicomAttribute(header, Seq(DicomValueChunk(header.bigEndian, updatedValue, true)))
    }

    def asDicomParts: Seq[DicomPart] = header +: valueChunks
  }

}