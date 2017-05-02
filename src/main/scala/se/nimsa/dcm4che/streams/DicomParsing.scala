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
import org.dcm4che3.data.{ElementDictionary, VR}
import org.dcm4che3.io.DicomStreamException
import org.dcm4che3.data.UID._
import org.dcm4che3.data.Tag._

/**
  * Helper methods for parsing binary DICOM data.
  */
trait DicomParsing {

  val DICOM_PREAMBLE_LENGTH = 132

  case class Info(bigEndian: Boolean, explicitVR: Boolean, hasFmi: Boolean) {
    /**
      * Best guess for transfer syntax.
      * @return transfer syntax uid value
      */
    def assumedTransferSyntax = if (explicitVR) {
      if (bigEndian) {
        ExplicitVRBigEndianRetired
      } else {
        ExplicitVRLittleEndian
      }
    } else {
      ImplicitVRLittleEndian
    }
  }
  case class Attribute(tag: Int, vr: VR, length: Int, value: ByteString)

  def dicomInfo(data: ByteString): Option[Info] =
    dicomInfo(data, assumeBigEndian = false)
      .orElse(dicomInfo(data, assumeBigEndian = true))

  private def dicomInfo(data: ByteString, assumeBigEndian: Boolean): Option[Info] = {
    val tag1 = bytesToTag(data, 0, assumeBigEndian)
    val vr = ElementDictionary.vrOf(tag1, null)
    if (vr == VR.UN)
      None
    else {
      if (bytesToVR(data, 4) == vr.code)
        Some(Info(
          bigEndian = assumeBigEndian,
          explicitVR = true,
          hasFmi = isFileMetaInformation(tag1)))
      else if (bytesToInt(data, 4, assumeBigEndian) >= 0)
        if (assumeBigEndian)
          throw new DicomStreamException("Implicit VR Big Endian encoded DICOM Stream")
        else
          Some(Info(
            bigEndian = false,
            explicitVR = false,
            hasFmi = isFileMetaInformation(tag1)))
      else
        None
    }
  }

  // parse dicom UID attribute from buffer
  def parseUIDAttribute(data: ByteString, explicitVR: Boolean, assumeBigEndian: Boolean): Attribute = {
    def valueWithoutPadding(value: ByteString) =
      if (value.takeRight(1).contains(0.toByte)) {
        value.dropRight(1)
      } else {
        value
      }

    val maybeHeader = if (explicitVR) {
      readHeaderExplicitVR(data, assumeBigEndian)
    } else {
      readHeaderImplicitVR(data)
    }

    if (maybeHeader.isEmpty) {
      throw new DicomStreamException("Could not parse DICOM data element from stream.")
    }

    val (tag, vr, headerLength, length) = maybeHeader.get
    val value = data.drop(headerLength).take(length)
    Attribute(tag, vr, length, valueWithoutPadding(value))
  }


  def isHeader(data: ByteString) = dicomInfo(data).isDefined

  def readHeader(buffer: ByteString, assumeBigEndian: Boolean, explicitVR: Boolean): Option[(Int, VR, Int, Int)] = {
    if (explicitVR) {
      readHeaderExplicitVR(buffer, assumeBigEndian)
    } else {
      readHeaderImplicitVR(buffer)
    }
  }

  /**
    * Read header of data element for explicit VR
    * @param buffer current buffer
    * @param assumeBigEndian true if big endian, false otherwise
    * @return
    */
  def readHeaderExplicitVR(buffer: ByteString, assumeBigEndian: Boolean): Option[(Int, VR, Int, Int)] = {
    if (buffer.size >= 8) {
      val tagVr = buffer.take(8)
      val (tag, vr) = DicomParsing.tagVr(tagVr, assumeBigEndian, true)
      if (vr == null) {
        // special case: sequences, length might be undefined '0xFFFFFFFF'
        val valueLength = bytesToInt(tagVr, 4, assumeBigEndian)
        if (valueLength == -1) {
          // length of sequence undefined, not supported
          None
        } else {
          Some((tag, vr, 8, bytesToInt(tagVr, 4, assumeBigEndian)))
        }
      } else if (vr.headerLength == 8) {
        Some((tag, vr, 8, bytesToUShort(tagVr, 6, assumeBigEndian)))
      } else {
        if (buffer.size >= 12) {
          Some((tag, vr, 12, bytesToInt(buffer, 8, assumeBigEndian)))
        } else {
          None
        }
      }
    } else {
      None
    }
  }

  /**
    * Read header of data element for implicit VR, handles special case for FileMetaInformationVersion.
    * @param buffer current buffer
    * @return
    */
  def readHeaderImplicitVR(buffer: ByteString): Option[(Int, VR, Int, Int)] = {
    val assumeBigEndian = false // implicit VR
    if (buffer.size >= 8) {
      val tag = DicomParsing.bytesToTag(buffer, 0, assumeBigEndian)
      val vr = ElementDictionary.getStandardElementDictionary.vrOf(tag)
      if ((vr == VR.OB) && (tag == FileMetaInformationVersion)) {
        Some((tag, vr, 8, bytesToInt(buffer, 4, assumeBigEndian)))
      } else if (vr.headerLength == 8) {
        val valueLength = bytesToInt(buffer, 4, assumeBigEndian)
        if ((tag == 0xFFFEE000 || tag == 0xFFFEE00D || tag == 0xFFFEE0DD) && valueLength == -1) {
          // special case: sequences, with undefined length '0xFFFFFFFF' not supported
          None
        } else {
          Some((tag, vr, 8, valueLength))
        }
      } else {
        if (buffer.size >= 12) {
          Some((tag, vr, 12, bytesToInt(buffer, 8, assumeBigEndian)))
        } else {
          None
        }
      }
    } else {
      None
    }
  }


  def isPreamble(data: ByteString): Boolean = data.slice(128, 132) == ByteString('D', 'I', 'C', 'M')

  def tagVr(data: ByteString, bigEndian: Boolean, explicitVr: Boolean): (Int, VR) = {
    val tag = bytesToTag(data, 0, bigEndian)
    if (tag == 0xFFFEE000 || tag == 0xFFFEE00D || tag == 0xFFFEE0DD)
      (tag, null)
    else if (explicitVr)
      (tag, VR.valueOf(bytesToVR(data, 4)))
    else
      (tag, VR.UN)
  }
  def isSequenceDelimiter(tag: Int) = groupNumber(tag) == 0xFFFE
  def isFileMetaInformation(tag: Int) = (tag & 0xFFFF0000) == 0x00020000
  def isPrivateAttribute(tag: Int) = groupNumber(tag) % 2 == 1

  def isGroupLength(tag: Int) = elementNumber(tag) == 0

  def groupNumber(tag: Int) = tag >>> 16
  def elementNumber(tag: Int) = tag & '\uffff'

  def bytesToShort(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToShortBE(bytes, off) else bytesToShortLE(bytes, off)
  def bytesToShortBE(bytes: ByteString, off: Int) = (bytes(off) << 8) + (bytes(off + 1) & 255)
  def bytesToShortLE(bytes: ByteString, off: Int) = (bytes(off + 1) << 8) + (bytes(off) & 255)
  def bytesToLong(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToLongBE(bytes, off) else bytesToLongLE(bytes, off)
  def bytesToLongBE(bytes: ByteString, off: Int) = (bytes(off).toLong << 56) + ((bytes(off + 1) & 255).toLong << 48) + ((bytes(off + 2) & 255).toLong << 40) + ((bytes(off + 3) & 255).toLong << 32) + ((bytes(off + 4) & 255).toLong << 24) + ((bytes(off + 5) & 255) << 16).toLong + ((bytes(off + 6) & 255) << 8).toLong + (bytes(off + 7) & 255).toLong
  def bytesToLongLE(bytes: ByteString, off: Int) = (bytes(off + 7).toLong << 56) + ((bytes(off + 6) & 255).toLong << 48) + ((bytes(off + 5) & 255).toLong << 40) + ((bytes(off + 4) & 255).toLong << 32) + ((bytes(off + 3) & 255).toLong << 24) + ((bytes(off + 2) & 255) << 16).toLong + ((bytes(off + 1) & 255) << 8).toLong + (bytes(off) & 255).toLong
  def bytesToDouble(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToDoubleBE(bytes, off) else bytesToDoubleLE(bytes, off)
  def bytesToDoubleBE(bytes: ByteString, off: Int) = java.lang.Double.longBitsToDouble(bytesToLongBE(bytes, off))
  def bytesToDoubleLE(bytes: ByteString, off: Int) = java.lang.Double.longBitsToDouble(bytesToLongLE(bytes, off))
  def bytesToFloat(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToFloatBE(bytes, off) else bytesToFloatLE(bytes, off)
  def bytesToFloatBE(bytes: ByteString, off: Int) = java.lang.Float.intBitsToFloat(bytesToIntBE(bytes, off))
  def bytesToFloatLE(bytes: ByteString, off: Int) = java.lang.Float.intBitsToFloat(bytesToIntLE(bytes, off))
  def bytesToUShort(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToUShortBE(bytes, off) else bytesToUShortLE(bytes, off)
  def bytesToUShortBE(bytes: ByteString, off: Int) = ((bytes(off) & 255) << 8) + (bytes(off + 1) & 255)
  def bytesToUShortLE(bytes: ByteString, off: Int) = ((bytes(off + 1) & 255) << 8) + (bytes(off) & 255)
  def bytesToTag(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToTagBE(bytes, off) else bytesToTagLE(bytes, off)
  def bytesToTagBE(bytes: ByteString, off: Int) = bytesToIntBE(bytes, off)
  def bytesToTagLE(bytes: ByteString, off: Int) = (bytes(off + 1) << 24) + ((bytes(off) & 255) << 16) + ((bytes(off + 3) & 255) << 8) + (bytes(off + 2) & 255)
  def bytesToVR(bytes: ByteString, off: Int) = bytesToUShortBE(bytes, off)
  def bytesToInt(bytes: ByteString, off: Int, bigEndian: Boolean) = if (bigEndian) bytesToIntBE(bytes, off) else bytesToIntLE(bytes, off)
  def bytesToIntBE(bytes: ByteString, off: Int) = (bytes(off) << 24) + ((bytes(off + 1) & 255) << 16) + ((bytes(off + 2) & 255) << 8) + (bytes(off + 3) & 255)
  def bytesToIntLE(bytes: ByteString, off: Int) = (bytes(off + 3) << 24) + ((bytes(off + 2) & 255) << 16) + ((bytes(off + 1) & 255) << 8) + (bytes(off) & 255)
  def asUnsignedInt(value: Int) : Long = value & 0x00000000ffffffffL
}

object DicomParsing extends DicomParsing
