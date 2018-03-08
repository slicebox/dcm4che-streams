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
    def assumedTransferSyntax: String = if (explicitVR) {
      if (bigEndian) {
        UID.ExplicitVRBigEndianRetired
      } else {
        UID.ExplicitVRLittleEndian
      }
    } else {
      UID.ImplicitVRLittleEndian
    }
  }

  case class Attribute(tag: Int, vr: VR, length: Long, value: ByteString)

  def dicomInfo(data: ByteString): Option[Info] =
    dicomInfo(data, assumeBigEndian = false)
      .orElse(dicomInfo(data, assumeBigEndian = true))

  private def dicomInfo(data: ByteString, assumeBigEndian: Boolean): Option[Info] = {
    val tag1 = bytesToTag(data, assumeBigEndian)
    val vr = ElementDictionary.vrOf(tag1, null)
    println(vr)
    if (vr == VR.UN)
      None
    else {
      if (bytesToVR(data.drop(4)) == vr.code)
        Some(Info(
          bigEndian = assumeBigEndian,
          explicitVR = true,
          hasFmi = isFileMetaInformation(tag1)))
      else if (intToUnsignedLong(bytesToInt(data.drop(4), assumeBigEndian)) >= 0)
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
    val value = data.drop(headerLength).take(length.toInt)
    Attribute(tag, vr, length, valueWithoutPadding(value))
  }

  def lengthToLong(length: Int): Long = if (length == -1) -1L else intToUnsignedLong(length)

  def readHeader(buffer: ByteString, assumeBigEndian: Boolean, explicitVR: Boolean): Option[(Int, VR, Int, Long)] = {
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
  def readHeaderExplicitVR(buffer: ByteString, assumeBigEndian: Boolean): Option[(Int, VR, Int, Long)] = {
    if (buffer.size >= 8) {
      val (tag, vr) = DicomParsing.tagVr(buffer, assumeBigEndian, explicitVr = true)
      if (vr == null) {
        // special case: sequences, length might be undefined '0xFFFFFFFF'
        val valueLength = bytesToInt(buffer.drop(4), assumeBigEndian)
        if (valueLength == -1) {
          // length of sequence undefined, not supported
          None
        } else {
          Some((tag, vr, 8, lengthToLong(bytesToInt(buffer.drop(4), assumeBigEndian))))
        }
      } else if (vr.headerLength == 8) {
        Some((tag, vr, 8, lengthToLong(bytesToUShort(buffer.drop(6), assumeBigEndian))))
      } else {
        if (buffer.size >= 12) {
          Some((tag, vr, 12, lengthToLong(bytesToInt(buffer.drop(8), assumeBigEndian))))
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
  def readHeaderImplicitVR(buffer: ByteString): Option[(Int, VR, Int, Long)] =
    if (buffer.size >= 8) {
      val assumeBigEndian = false // implicit VR
      val tag = bytesToTag(buffer, assumeBigEndian)
      val vr = ElementDictionary.getStandardElementDictionary.vrOf(tag)
      val valueLength = bytesToInt(buffer.drop(4), assumeBigEndian)
      if (tag == 0xFFFEE000 || tag == 0xFFFEE00D || tag == 0xFFFEE0DD)
        if (valueLength == -1)
          None // special case: sequences, with undefined length '0xFFFFFFFF' not supported
        else
          Some((tag, null, 8, lengthToLong(valueLength)))
      else
        Some((tag, vr, 8, lengthToLong(valueLength)))
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

  def isPreamble(data: ByteString): Boolean = data.length >= dicomPreambleLength && data.slice(dicomPreambleLength - 4, dicomPreambleLength) == ByteString('D', 'I', 'C', 'M')
  def isHeader(data: ByteString): Boolean = dicomInfo(data).isDefined

  def isSequenceDelimiter(tag: Int): Boolean = groupNumber(tag) == 0xFFFE
  def isFileMetaInformation(tag: Int): Boolean = (tag & 0xFFFF0000) == 0x00020000
  def isPrivateAttribute(tag: Int): Boolean = groupNumber(tag) % 2 == 1
  def isGroupLength(tag: Int): Boolean = elementNumber(tag) == 0

  def isDeflated(transferSyntaxUid: String): Boolean = transferSyntaxUid == UID.DeflatedExplicitVRLittleEndian || transferSyntaxUid == UID.JPIPReferencedDeflate

}

object DicomParsing extends DicomParsing
