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


/**
  * Helper methods for parsing binary DICOM data.
  */
trait DicomParsing {
  val TRANSFER_SYNTAX_IMPLICIT_VR_LITTLE_ENDIAN = "1.2.840.10008.1.2"
  val TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN = "1.2.840.10008.1.2.1"
  val TRANSFER_SYNTAX_EXPLICIT_VR_BIG_ENDIAN = "1.2.840.10008.1.2.2"

  case class Info(bigEndian: Boolean, explicitVR: Boolean, hasFmi: Boolean) {
    /**
      * Best guess for transfer syntax.
      * @return transfer syntax uid value
      */
    def assumedTransferSyntax = if (explicitVR) {
      if (bigEndian) {
        TRANSFER_SYNTAX_EXPLICIT_VR_BIG_ENDIAN
      } else {
        TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN
      }
    } else {
      TRANSFER_SYNTAX_IMPLICIT_VR_LITTLE_ENDIAN
    }
  }
  case class Attribute(tag: Int, length: Int, value: ByteString)

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

  // parse dicom attribute from file meta info, explict VR, little endian
  def fileMetaInformationUIDAttribute(data: ByteString, explicitVR: Boolean, assumeBigEndian: Boolean): Attribute = {
    val tag1 = bytesToTag(data, 0, assumeBigEndian)

    if (explicitVR) {
      if (bytesToVR(data, 4) == VR.UI.code()) {
        val length = bytesToShort(data, 6, assumeBigEndian)
        val value = data.drop(8).take(length)
        Attribute(tag1, length, valueWithoutPadding(value))
      } else {
        throw new DicomStreamException("Attribute in file meta information: expected eplicit VR of type UI")
      }
    } else {
      // implicit VR, little endian
      val length = bytesToInt(data, 4, false)
      val value = data.drop(8).take(length)
      Attribute(tag1, length, valueWithoutPadding(value))
    }
  }

  def valueWithoutPadding(value: ByteString) =
    if (value.takeRight(1).contains(0.toByte)) {
      value.dropRight(1)
    } else {
      value
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

  def isFileMetaInformation(tag: Int) = (tag & 0xFFFF0000) == 0x00020000
  def isMediaStorageSOPClassUID(tag: Int) = tag == 0x00020002
  def isTransferSyntaxUID(tag: Int) = tag == 0x00020010
  def isSOPClassUID(tag: Int) = tag == 0x00080016

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
}

object DicomParsing extends DicomParsing
