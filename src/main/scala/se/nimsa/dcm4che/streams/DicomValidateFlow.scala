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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.dcm4che3.io.DicomStreamException
import se.nimsa.dcm4che.streams.DicomFlows.ValidationContext
import se.nimsa.dcm4che.streams.DicomParsing.{Info, isPreamble, dicomPreambleLength}
import org.dcm4che3.data.Tag._

/**
  * A flow which passes on the input bytes unchanged, fails for non-DICOM files, validates for DICOM files with supported
  * Media Storage SOP Class UID, Transfer Syntax UID combination passed as context.
  * @param contexts supported MediaStorageSOPClassUID, TransferSynatxUID combinations
  */
class DicomValidateFlow(contexts: Option[Seq[ValidationContext]]) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("DicomValidateFlow.in")
  val out = Outlet[ByteString]("DicomValidateFlow.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var buffer = ByteString.empty
    var isValidated = false
    // 378  bytes should be enough when passing context - Media Storage SOP Class UID and Transfer Syntax UID
    // otherwise read to first header
    val maxBufferLength = if (contexts.isDefined) 512 else 140

    setHandlers(in, out, new InHandler with OutHandler {

      override def onPull(): Unit = {
        pull(in)
      }

      override def onPush(): Unit = {
        val chunk = grab(in)
        if (isValidated)
          push(out, chunk)
        else {
          buffer = buffer ++ chunk

          if (buffer.length >= maxBufferLength) {
            if (isPreamble(buffer)) {
              if (DicomParsing.isHeader(buffer.drop(dicomPreambleLength))) {
                if (contexts.isDefined) {
                  val info = DicomParsing.dicomInfo(buffer.drop(dicomPreambleLength)).get
                  validateFileMetaInformation(buffer.drop(dicomPreambleLength), info)
                } else {
                  setValidated()
                }
              } else {
                setFailed()
              }
            } else if (DicomParsing.isHeader(buffer)) {
              if (contexts.isDefined) {
                val info = DicomParsing.dicomInfo(buffer).get
                validateSOPClassUID(buffer, info)
              } else {
                setValidated()
              }
            } else {
              setFailed()
            }
          } else {
            pull(in)
          }
        }
      }

      override def onUpstreamFinish() = {
        if (!isValidated)
          if (contexts.isDefined) {
            if (buffer.length >= dicomPreambleLength && isPreamble(buffer)) {
              val info = DicomParsing.dicomInfo(buffer.drop(dicomPreambleLength)).get
              validateFileMetaInformation(buffer.drop(dicomPreambleLength), info)
            } else if (buffer.length >= 8 && DicomParsing.isHeader(buffer)) {
              val info = DicomParsing.dicomInfo(buffer).get
              validateSOPClassUID(buffer, info)
            } else {
              setFailed()
            }
          } else {
            if (buffer.length == dicomPreambleLength && isPreamble(buffer))
              setValidated()
            else if (buffer.length >= 8 && DicomParsing.isHeader(buffer))
              setValidated()
            else
              setFailed()
          }
        completeStage()
      }


      // Find and validate MediaSOPClassUID and TranferSyntaxUID
      private def validateFileMetaInformation(data: ByteString, info: Info) = {
        var currentData = data

        val (failed, tailData) = findAndValidateField(data, info, "MediaStorageSOPClassUID", (tag: Int) => tag == MediaStorageSOPClassUID, (tag: Int) => (tag & 0xFFFF0000) > 0x00020000)
        if (!failed) {
          currentData = tailData
          val mscu = DicomParsing.parseUIDAttribute(currentData, info.explicitVR, info.bigEndian)

          val (nextFailed, nextTailData) = findAndValidateField(currentData, info, "TransferSyntaxUID", (tag: Int) => tag == TransferSyntaxUID, (tag: Int) => (tag & 0xFFFF0000) > 0x00020000)
          if (!nextFailed) {
            currentData = nextTailData
            val tsuid = DicomParsing.parseUIDAttribute(currentData, info.explicitVR, info.bigEndian)

            val currentContext = ValidationContext(mscu.value.utf8String, tsuid.value.utf8String)
            if (contexts.get.contains(currentContext)) {
              setValidated()
            } else {
              failStage(new DicomStreamException(s"The presentation context [SOPClassUID = ${mscu.value.utf8String}, TransferSyntaxUID = ${tsuid.value.utf8String}] is not supported"))
            }
          }
        }
      }


      // Find and validate SOPCLassUID
      private def validateSOPClassUID(data: ByteString, info: Info) = {

        val (failed, tailData) = findAndValidateField(data, info, "SOPClassUID", (tag: Int) => tag == SOPClassUID, (tag: Int) => tag > SOPClassUID)

        if (!failed) {
          // SOP CLass UID
          val scuid = DicomParsing.parseUIDAttribute(tailData, info.explicitVR, info.bigEndian)

          // transfer syntax: best guess
          val tsuid = info.assumedTransferSyntax

          val currentContext = ValidationContext(scuid.value.utf8String, tsuid)
          if (contexts.get.contains(currentContext)) {
            setValidated()
          } else {
            failStage(new DicomStreamException(s"The presentation context [SOPClassUID = ${scuid.value.utf8String}, TransferSyntaxUID = $tsuid] is not supported"))
          }
        }
      }

      /**
        * Utility method. Search after DICOM Header in byte stream.
        * @param data bytes stream
        * @param info info object obtained earlier
        * @param fieldName dicom field name, used for error log
        * @param found field found condition, typically comparison of dicom tags
        * @param stopSearching stop condition
        * @return
        */
      def findAndValidateField(data: ByteString, info: Info, fieldName: String, found: (Int) => Boolean, stopSearching: (Int) => Boolean ) = {
        var currentTag = -1
        var failed = false
        var currentData = data
        def takeMax8(buffer: ByteString) = if (buffer.size >= 8) buffer.take(8) else buffer

        while (!found(currentTag) && !failed) {
          val maybeHeader = DicomParsing.readHeader(currentData, info.bigEndian, info.explicitVR)
          if (maybeHeader.isDefined) {
            val (tag, _, headerLength, length) = maybeHeader.get
            if (tag < currentTag) {
              failed = true
              failStage(new DicomStreamException(s"Parse error. Invalid tag order or invalid DICOM header: ${takeMax8(currentData)}."))
            } else {
              currentTag = tag
              if (!found(tag)) {
                currentData = currentData.drop(headerLength + length)
              }
              if (stopSearching(currentTag) || currentData.size < 8) {
                // read past stop criteria without finding tag, or not enough data left in buffer
                failed = true
                failStage(new DicomStreamException(s"Not a valid DICOM file. Could not find $fieldName."))
              }
            }
          } else {
            // could not parse header
            failed = true
            failStage(new DicomStreamException(s"Parse error. Invalid DICOM header: ${takeMax8(currentData)}."))
          }
        }
        (failed, currentData)
      }

      def setValidated() = {
        isValidated = true
        push(out, buffer)
      }

      def setFailed() = failStage(new DicomStreamException("Not a DICOM stream"))

    })
  }
}
