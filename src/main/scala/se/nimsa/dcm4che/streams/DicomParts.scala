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


object DicomParts {

  /**
    * The most general representation of a DICOM stream chunk
    */
  trait DicomPart {
    def bigEndian: Boolean
    def bytes: ByteString
  }

  /**
    * A `DicomPart` with a tag number (e.g. `DicomHeader`, `DicomSequence`, 'DicomFragments')
    */
  trait TagPart extends DicomPart {
    def tag: Int
  }

  /**
    * A 'DicomPart' with a length attribute (e.g. `DicomHeader`, `DicomSequence`, 'DicomItem')
    */
  trait LengthPart extends DicomPart {
    def length: Long
    def hasLength: Boolean = length >= 0
  }

  case class DicomPreamble(bytes: ByteString) extends DicomPart {
    def bigEndian = false
  }

  case class DicomHeader(tag: Int, vr: VR, length: Long, isFmi: Boolean, bigEndian: Boolean, explicitVR: Boolean, bytes: ByteString) extends DicomPart with TagPart with LengthPart {

    def withUpdatedLength(newLength: Long): DicomHeader = {

      val updated = if ((bytes.size >= 8) && explicitVR && (vr.headerLength == 8)) { //explicit vr
        bytes.take(6) ++ shortToBytes(newLength.toShort, bigEndian)
      } else if ((bytes.size >= 12) && explicitVR && (vr.headerLength == 12)) { //explicit vr
        bytes.take(8) ++ intToBytes(newLength.toInt, bigEndian)
      } else { //implicit vr
        bytes.take(4) ++ intToBytes(newLength.toInt, bigEndian)
      }

      DicomHeader(tag, vr, newLength, isFmi, bigEndian, explicitVR, updated)
    }

    override def toString = s"DicomHeader ${tagToString(tag)} ${if (isFmi) "(meta) " else ""}$vr ${if (!explicitVR) "(implicit) " else ""}length = ${bytes.length} value length = $length ${if (bigEndian) "(big endian) " else ""}$bytes"
  }

  object DicomHeader {
    def apply(tag: Int, vr: VR, length: Long, isFmi: Boolean, bigEndian: Boolean, explicitVR: Boolean): DicomHeader = {
      val tagBytes = tagToBytes(tag, bigEndian)
      val headerBytes =
        if (explicitVR) {
          val vrBytes = shortToBytesBE(vr.code.toShort)
          val lengthBytes =
            if (vr.headerLength() == 8)
              shortToBytes(length.toShort, bigEndian)
            else
              ByteString(0, 0) ++ intToBytes(length.toInt, bigEndian)
          tagBytes ++ vrBytes ++ lengthBytes
        } else {
          val lengthBytes = intToBytes(length.toInt, bigEndian)
          tagBytes ++ lengthBytes
        }
      DicomHeader(tag, vr, length, isFmi, bigEndian, explicitVR, headerBytes)
    }
  }

  case class DicomValueChunk(bigEndian: Boolean, bytes: ByteString, last: Boolean) extends DicomPart {
    override def toString = s"DicomValueChunk ${if (last) "(last) " else ""}length = ${bytes.length} ${if (bigEndian) "(big endian) " else ""}ASCII = '${bytes.utf8String}' $bytes"
  }

  case class DicomDeflatedChunk(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  trait DicomItem extends LengthPart {
    def index: Int
    def length: Long
  }

  case class DicomSequenceItem(index: Int, length: Long, bigEndian: Boolean, bytes: ByteString) extends DicomItem {
    override def toString = s"DicomSequenceItem index = $index length = $length ${if (bigEndian) "(big endian) " else ""}$bytes"
  }

  case class DicomFragmentsItem(index: Int, length: Long, bigEndian: Boolean, bytes: ByteString) extends DicomItem {
    override def toString = s"DicomFragmentsItem index = $index length = $length ${if (bigEndian) "(big endian) " else ""}$bytes"
  }

  case class DicomSequenceItemDelimitation(index: Int, bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomSequence(tag: Int, length: Long, bigEndian: Boolean, bytes: ByteString) extends DicomPart with TagPart with LengthPart {
    override def toString = s"DicomSequence ${tagToString(tag)} length = $length ${if (bigEndian) "(big endian) " else ""}$bytes"
  }

  case class DicomSequenceDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragments(tag: Int, length: Long, vr: VR, bigEndian: Boolean, bytes: ByteString) extends DicomPart with TagPart with LengthPart {
    override def toString = s"DicomFragments ${tagToString(tag)} $vr ${if (bigEndian) "(big endian) " else ""}$bytes"
  }

  case class DicomFragmentsDelimitation(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomUnknownPart(bigEndian: Boolean, bytes: ByteString) extends DicomPart

  case class DicomFragment(index: Int, bigEndian: Boolean, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes: ByteString = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
  }

  case class DicomAttribute(header: DicomHeader, valueChunks: Seq[DicomValueChunk]) extends DicomPart with TagPart with LengthPart {
    override val tag: Int = header.tag
    override val length: Long = header.length
    def valueBytes: ByteString = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
    def bytes: ByteString = header.bytes ++ valueBytes
    def bigEndian: Boolean = header.bigEndian

    def withUpdatedValue(newValue: String, cs: SpecificCharacterSet = SpecificCharacterSet.ASCII): DicomAttribute = {
      val newBytes = header.vr.toBytes(newValue, cs)
      val needsPadding = newBytes.size % 2 == 1
      val newLength = if (needsPadding) newBytes.size + 1L else newBytes.size
      val updatedHeader = header.withUpdatedLength(newLength)
      val updatedValue = if (needsPadding)
        ByteString.fromArray(newBytes :+ header.vr.paddingByte().toByte)
      else
        ByteString.fromArray(newBytes)
      DicomAttribute(updatedHeader, Seq(DicomValueChunk(header.bigEndian, updatedValue, last = true)))
    }

    def asDicomParts: Seq[DicomPart] = header +: valueChunks
  }

  case object DicomEndMarker extends DicomPart {
    def bigEndian: Boolean = false
    def bytes: ByteString = ByteString.empty
  }

  case class DicomAttributes(attributes: Seq[DicomAttribute]) extends DicomPart {
    def bigEndian: Boolean = attributes.headOption.exists(_.bigEndian)
    def bytes: ByteString = attributes.map(_.bytes).reduce(_ ++ _)
  }

}
