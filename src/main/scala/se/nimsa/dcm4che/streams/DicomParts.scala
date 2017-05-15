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
import org.dcm4che3.util.TagUtils


object DicomParts {

  trait DicomPart {
    def bigEndian: Boolean
    def bytes: ByteString
  }

  case class DicomPreamble(bytes: ByteString) extends DicomPart {
    def bigEndian = false
  }

  case class DicomHeader(tag: Int, vr: VR, length: Int, isFmi: Boolean, bigEndian: Boolean, explicitVR: Boolean, bytes: ByteString) extends DicomPart {

    def withUpdatedLength(newLength: Int) : DicomHeader = {

      val updated = if ((bytes.size >= 8) && explicitVR && (vr.headerLength == 8)) { //explicit vr
          bytes.take(6) ++ DicomParsing.shortToBytes(newLength.toShort, bigEndian)
        } else if ((bytes.size >= 12) && explicitVR && (vr.headerLength == 12)) { //explicit vr
          bytes.take(8) ++ DicomParsing.intToBytes(newLength, bigEndian)
        } else { //implicit vr
          bytes.take(4) ++ DicomParsing.intToBytes(newLength, bigEndian)
        }

      DicomHeader(tag, vr, newLength, isFmi, bigEndian, explicitVR, updated)
    }

    override def toString = s"DicomHeader ${TagUtils.toHexString(tag)} ${if (isFmi) "(meta) " else ""}$vr ${if (!explicitVR) "(implicit) " else ""}length = $length ${if (bigEndian) "(big endian) " else "" }$bytes"
  }

  case class DicomValueChunk(bigEndian: Boolean, bytes: ByteString, last: Boolean) extends DicomPart {}

  case class DicomDeflatedChunk(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomItem(length: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart {
    override def toString = s"DicomItem length = $length ${if (bigEndian) "(big endian) " else "" }$bytes"
  }

  case class DicomItemDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequence(tag: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart {
    override def toString = s"DicomSequence ${TagUtils.toHexString(tag)} ${if (bigEndian) "(big endian) " else "" }$bytes"
  }

  case class DicomSequenceDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragments(tag: Int, vr: VR, bigEndian: Boolean, bytes: ByteString) extends DicomPart {
    override def toString = s"DicomFragments ${TagUtils.toHexString(tag)} $vr ${if (bigEndian) "(big endian) " else "" }$bytes"
  }

  case class DicomFragmentsDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomUnknownPart(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragment(bigEndian: Boolean, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
  }

  case class DicomAttribute(header: DicomHeader, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def valueBytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
    def bytes = header.bytes ++ valueBytes
    def bigEndian = header.bigEndian

    // LO: long string 64 chars max
    // SH: short string 16 chars max
    // PN: person name
    // UI: 64 chars max
    def withUpdatedStringValue(newValue: String, cs: SpecificCharacterSet = SpecificCharacterSet.ASCII): DicomAttribute = {
      val newBytes = header.vr.toBytes(newValue, cs)
      val needsPadding = newBytes.size % 2 == 1
      val newLength = if (needsPadding) newBytes.size + 1 else newBytes.size
      val updatedHeader = header.withUpdatedLength(newLength)
      val updatedValue = if (needsPadding)
        ByteString.fromArray(newBytes :+ header.vr.paddingByte().toByte)
      else
        ByteString.fromArray(newBytes)
      DicomAttribute(updatedHeader, Seq(DicomValueChunk(header.bigEndian, updatedValue, last = true)))
    }

    // DA: A string of characters of the format YYYYMMDD, 8 bytes fixed
    def withUpdatedDateValue(newValue: String, cs: SpecificCharacterSet = SpecificCharacterSet.ASCII): DicomAttribute = {
      val newBytes = header.vr.toBytes(newValue, cs)
      val updatedValue = ByteString.fromArray(newBytes)
      DicomAttribute(header, Seq(DicomValueChunk(header.bigEndian, updatedValue, last = true)))
    }

    def asDicomParts: Seq[DicomPart] = header +: valueChunks
  }

  case object DicomEndMarker extends DicomPart {
    def bigEndian = false
    def bytes = ByteString.empty
  }

  case class DicomAttributes(attributes: Seq[DicomAttribute]) extends DicomPart {
    def bigEndian = attributes.headOption.exists(_.bigEndian)
    def bytes = attributes.map(_.bytes).reduce(_ ++ _)
  }

}
