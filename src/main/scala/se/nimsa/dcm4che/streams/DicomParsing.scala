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
import org.dcm4che3.data.{ElementDictionary, UID, VR}
import org.dcm4che3.io.DicomStreamException

/**
  * Helper methods for parsing binary DICOM data.
  */
trait DicomParsing {

  val dicomPreambleLength = 132

  case class Info(bigEndian: Boolean, explicitVR: Boolean, hasFmi: Boolean) {
    /**
      * Best guess for transfer syntax.
      *
      * @return transfer syntax uid value
      */
    def assumedTransferSyntax = if (explicitVR) {
      if (bigEndian) {
        UID.ExplicitVRBigEndianRetired
      } else {
        UID.ExplicitVRLittleEndian
      }
    } else {
      UID.ImplicitVRLittleEndian
    }
  }

  case class Attribute(tag: Int, vr: VR, length: Int, value: ByteString)

  def dicomInfo(data: ByteString): Option[Info] =
    dicomInfo(data, assumeBigEndian = false)
      .orElse(dicomInfo(data, assumeBigEndian = true))

  private def dicomInfo(data: ByteString, assumeBigEndian: Boolean): Option[Info] = {
    val tag1 = bytesToTag(data, assumeBigEndian)
    val vr = ElementDictionary.vrOf(tag1, null)
    if (vr == VR.UN)
      None
    else {
      if (bytesToVR(data.drop(4)) == vr.code)
        Some(Info(
          bigEndian = assumeBigEndian,
          explicitVR = true,
          hasFmi = isFileMetaInformation(tag1)))
      else if (bytesToInt(data.drop(4), assumeBigEndian) >= 0)
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


  def readHeader(buffer: ByteString, assumeBigEndian: Boolean, explicitVR: Boolean): Option[(Int, VR, Int, Int)] = {
    if (explicitVR) {
      readHeaderExplicitVR(buffer, assumeBigEndian)
    } else {
      readHeaderImplicitVR(buffer)
    }
  }

  /**
    * Read header of data element for explicit VR
    *
    * @param buffer          current buffer
    * @param assumeBigEndian true if big endian, false otherwise
    * @return
    */
  def readHeaderExplicitVR(buffer: ByteString, assumeBigEndian: Boolean): Option[(Int, VR, Int, Int)] = {
    if (buffer.size >= 8) {
      val (tag, vr) = DicomParsing.tagVr(buffer, assumeBigEndian, explicitVr = true)
      if (vr == null) {
        // special case: sequences, length might be undefined '0xFFFFFFFF'
        val valueLength = bytesToInt(buffer.drop(4), assumeBigEndian)
        if (valueLength == -1) {
          // length of sequence undefined, not supported
          None
        } else {
          Some((tag, vr, 8, bytesToInt(buffer.drop(4), assumeBigEndian)))
        }
      } else if (vr.headerLength == 8) {
        Some((tag, vr, 8, bytesToUShort(buffer.drop(6), assumeBigEndian)))
      } else {
        if (buffer.size >= 12) {
          Some((tag, vr, 12, bytesToInt(buffer.drop(8), assumeBigEndian)))
        } else {
          None
        }
      }
    } else {
      None
    }
  }

  /**
    * Read header of data element for implicit VR.
    *
    * @param buffer current buffer
    * @return
    */
  def readHeaderImplicitVR(buffer: ByteString): Option[(Int, VR, Int, Int)] =
    if (buffer.size >= 8) {
      val assumeBigEndian = false // implicit VR
      val tag = DicomParsing.bytesToTag(buffer, assumeBigEndian)
      val vr = ElementDictionary.getStandardElementDictionary.vrOf(tag)
      val valueLength = bytesToInt(buffer.drop(4), assumeBigEndian)
      if (tag == 0xFFFEE000 || tag == 0xFFFEE00D || tag == 0xFFFEE0DD)
        if (valueLength == -1)
          None // special case: sequences, with undefined length '0xFFFFFFFF' not supported
        else
          Some((tag, null, 8, valueLength))
      else
        Some((tag, vr, 8, valueLength))
    } else
      None

  def tagVr(data: ByteString, bigEndian: Boolean, explicitVr: Boolean): (Int, VR) = {
    val tag = bytesToTag(data, bigEndian)
    if (tag == 0xFFFEE000 || tag == 0xFFFEE00D || tag == 0xFFFEE0DD)
      (tag, null)
    else if (explicitVr)
      (tag, VR.valueOf(bytesToVR(data.drop(4))))
    else
      (tag, VR.UN)
  }

  def isPreamble(data: ByteString): Boolean = data.slice(128, 132) == ByteString('D', 'I', 'C', 'M')
  def isHeader(data: ByteString) = dicomInfo(data).isDefined

  def isSequenceDelimiter(tag: Int) = groupNumber(tag) == 0xFFFE
  def isFileMetaInformation(tag: Int) = (tag & 0xFFFF0000) == 0x00020000
  def isPrivateAttribute(tag: Int) = groupNumber(tag) % 2 == 1
  def isGroupLength(tag: Int) = elementNumber(tag) == 0

  def isDeflated(transferSyntaxUid: String) = transferSyntaxUid == UID.DeflatedExplicitVRLittleEndian || transferSyntaxUid == UID.JPIPReferencedDeflate

  def groupNumber(tag: Int) = tag >>> 16
  def elementNumber(tag: Int) = tag & '\uffff'

  def bytesToShort(bytes: ByteString, bigEndian: Boolean): Short = if (bigEndian) bytesToShortBE(bytes) else bytesToShortLE(bytes)
  def bytesToShortBE(bytes: ByteString): Short = ((bytes(0) << 8) + (bytes(1) & 255)).toShort
  def bytesToShortLE(bytes: ByteString): Short = ((bytes(1) << 8) + (bytes(0) & 255)).toShort
  def bytesToLong(bytes: ByteString, bigEndian: Boolean): Long = if (bigEndian) bytesToLongBE(bytes) else bytesToLongLE(bytes)
  def bytesToLongBE(bytes: ByteString): Long = (bytes(0).toLong << 56) + ((bytes(1) & 255).toLong << 48) + ((bytes(2) & 255).toLong << 40) + ((bytes(3) & 255).toLong << 32) + ((bytes(4) & 255).toLong << 24) + ((bytes(5) & 255) << 16).toLong + ((bytes(6) & 255) << 8).toLong + (bytes(7) & 255).toLong
  def bytesToLongLE(bytes: ByteString): Long = (bytes(7).toLong << 56) + ((bytes(6) & 255).toLong << 48) + ((bytes(5) & 255).toLong << 40) + ((bytes(4) & 255).toLong << 32) + ((bytes(3) & 255).toLong << 24) + ((bytes(2) & 255) << 16).toLong + ((bytes(1) & 255) << 8).toLong + (bytes(0) & 255).toLong
  def bytesToDouble(bytes: ByteString, bigEndian: Boolean): Double = if (bigEndian) bytesToDoubleBE(bytes) else bytesToDoubleLE(bytes)
  def bytesToDoubleBE(bytes: ByteString): Double = java.lang.Double.longBitsToDouble(bytesToLongBE(bytes))
  def bytesToDoubleLE(bytes: ByteString): Double = java.lang.Double.longBitsToDouble(bytesToLongLE(bytes))
  def bytesToFloat(bytes: ByteString, bigEndian: Boolean): Float = if (bigEndian) bytesToFloatBE(bytes) else bytesToFloatLE(bytes)
  def bytesToFloatBE(bytes: ByteString): Float = java.lang.Float.intBitsToFloat(bytesToIntBE(bytes))
  def bytesToFloatLE(bytes: ByteString): Float = java.lang.Float.intBitsToFloat(bytesToIntLE(bytes))
  def bytesToUShort(bytes: ByteString, bigEndian: Boolean): Int = if (bigEndian) bytesToUShortBE(bytes) else bytesToUShortLE(bytes)
  def bytesToUShortBE(bytes: ByteString): Int = ((bytes(0) & 255) << 8) + (bytes(1) & 255)
  def bytesToUShortLE(bytes: ByteString): Int = ((bytes(1) & 255) << 8) + (bytes(0) & 255)
  def bytesToTag(bytes: ByteString, bigEndian: Boolean): Int = if (bigEndian) bytesToTagBE(bytes) else bytesToTagLE(bytes)
  def bytesToTagBE(bytes: ByteString): Int = bytesToIntBE(bytes)
  def bytesToTagLE(bytes: ByteString): Int = (bytes(1) << 24) + ((bytes(0) & 255) << 16) + ((bytes(3) & 255) << 8) + (bytes(2) & 255)
  def bytesToVR(bytes: ByteString): Int = bytesToUShortBE(bytes)
  def bytesToInt(bytes: ByteString, bigEndian: Boolean): Int = if (bigEndian) bytesToIntBE(bytes) else bytesToIntLE(bytes)
  def bytesToIntBE(bytes: ByteString): Int = (bytes(0) << 24) + ((bytes(1) & 255) << 16) + ((bytes(2) & 255) << 8) + (bytes(3) & 255)
  def bytesToIntLE(bytes: ByteString): Int = (bytes(3) << 24) + ((bytes(2) & 255) << 16) + ((bytes(1) & 255) << 8) + (bytes(0) & 255)
  def shortToBytes(i: Short, bigEndian: Boolean): ByteString = if (bigEndian) shortToBytesBE(i) else shortToBytesLE(i)
  def shortToBytesBE(i: Short): ByteString = ByteString((i >> 8).toByte, i.toByte)
  def shortToBytesLE(i: Short): ByteString = ByteString(i.toByte, (i >> 8).toByte)
  def intToBytes(i: Int, bigEndian: Boolean): ByteString = if (bigEndian) intToBytesBE(i) else intToBytesLE(i)
  def intToBytesBE(i: Int): ByteString = ByteString((i >> 24).toByte, (i >> 16).toByte, (i >> 8).toByte, i.toByte)
  def intToBytesLE(i: Int): ByteString = ByteString(i.toByte, (i >> 8).toByte, (i >> 16).toByte, (i >> 24).toByte)

}

object DicomParsing extends DicomParsing
