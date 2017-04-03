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

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import org.dcm4che3.data.{ElementDictionary, VR}
import org.dcm4che3.io.DicomStreamException


/**
  * Various flows for transforming streams of <code>DicomPart</code>s.
  */
object DicomFlows {

  import DicomPartFlow._
  import DicomParsing._

  case class DicomAttribute(header: DicomHeader, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)

    def bigEndian = header.bigEndian
  }

  case class DicomFragment(bigEndian: Boolean, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
  }

  case class Context(sopClassUID: String, transferSyntax: String)


  private class DicomValidateFlow(contexts: Option[Seq[Context]]) extends GraphStage[FlowShape[ByteString, ByteString]] {
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
                if (isHeader(buffer.drop(132))) {
                  if (contexts.isDefined) {
                    val info = DicomParsing.dicomInfo(buffer.drop(132)).get
                    validateFileMetaInformation(buffer.drop(132), info)
                  } else {
                    setValidated()
                  }
                } else {
                  setFailed()
                }
              } else if (isHeader(buffer)) {
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
              if (buffer.length >= 132 && isPreamble(buffer)) {
                val info = DicomParsing.dicomInfo(buffer.drop(132)).get
                validateFileMetaInformation(buffer.drop(132), info)
              } else if (buffer.length >= 8 && isHeader(buffer)) {
                val info = DicomParsing.dicomInfo(buffer).get
                validateSOPClassUID(buffer, info)
              } else {
                setFailed()
              }
            } else {
              if (buffer.length == 132 && isPreamble(buffer))
                setValidated()
              else if (buffer.length >= 8 && isHeader(buffer))
                setValidated()
              else
                setFailed()
            }
          completeStage()
        }

        def isHeader(data: ByteString) = DicomParsing.dicomInfo(data).isDefined

        def readHeader(buffer: ByteString, assumeBigEndian: Boolean, explicitVR: Boolean): (Int, VR, Int, Int) = {
          val tagVr = buffer.take(8)
          val (tag, vr) = DicomParsing.tagVr(tagVr, assumeBigEndian, explicitVR)
          if (vr == null)
            (tag, vr, 8, bytesToInt(tagVr, 4, assumeBigEndian))
          else if (explicitVR)
            if (vr.headerLength == 8)
              (tag, vr, 8, bytesToUShort(tagVr, 6, assumeBigEndian))
            else {
              (tag, vr, 12, bytesToInt(buffer, 8, assumeBigEndian))
            }
          else {
            // Implicit VR
            val vr = ElementDictionary.vrOf(tag, null)
            if (vr == VR.OB) {
              (tag, vr, 8, bytesToInt(tagVr, 4, assumeBigEndian))
            } else if (vr.headerLength == 8) {
              (tag, vr, 8, bytesToInt(tagVr, 4, assumeBigEndian))
            } else {
              (tag, vr, 12, bytesToInt(buffer, 8, assumeBigEndian))
            }
          }
        }

        // Find  and validate MediaSOPClassUID and TranferSyntaxUID
        private def validateFileMetaInformation(data: ByteString, info: Info) = {
          var currentTag = -1
          var currentData = data

          val (failed, tailData) = findAndValidateField(data, info, "MediaStorageSOPClassUID", isMediaStorageSOPClassUID, (tag: Int) => ((tag & 0xFFFF0000) > 0x00020000))
          if (!failed) {
            currentData = tailData
            // Media SOP CLass UID
            val mscu = DicomParsing.fileMetaInformationUIDAttribute(currentData, info.explicitVR, info.bigEndian)

            val (nextFailed, nextTailData) = findAndValidateField(currentData, info, "TransferSyntaxUID", isTransferSyntaxUID, (tag: Int) => ((tag & 0xFFFF0000) > 0x00020000))
            if (!nextFailed) {
              currentData = nextTailData
              val tsuid = DicomParsing.fileMetaInformationUIDAttribute(currentData, info.explicitVR, info.bigEndian) // Transfer Syntax UID

              val currentContext = Context(mscu.value.utf8String, tsuid.value.utf8String)
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

          val (failed, tailData) = findAndValidateField(data, info, "SOPClassUID", isSOPClassUID, (tag: Int) => tag > 0x00080016)

          if (!failed) {
            // SOP CLass UID
            val scuid = DicomParsing.fileMetaInformationUIDAttribute(tailData, info.explicitVR, info.bigEndian)

            // transfer syntax: best guess
            val tsuid = info.assumedTransferSyntax

            val currentContext = Context(scuid.value.utf8String, tsuid)
            if (contexts.get.contains(currentContext)) {
              setValidated()
            } else {
              failStage(new DicomStreamException(s"The presentation context [SOPClassUID = ${scuid.value.utf8String}, TransferSyntaxUID = ${tsuid}] is not supported"))
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

          while (!found(currentTag) && !failed) {
            val (tag, vr, headerLength, length) = readHeader(currentData, info.bigEndian, info.explicitVR)

            if (tag < currentTag) {
              failed = true
              failStage(new DicomStreamException(s"Parse error. Invalid tag order or invalid DICOM header: ${currentData.take(8)}."))
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

  /**
    * Print each element and then pass it on unchanged.
    *
    * @tparam A the type of elements in the flow
    * @return the associated flow
    */
  def printFlow[A]: Flow[A, A, NotUsed] = Flow.fromFunction { a =>
    println(a)
    a
  }

  /**
    * Combine each header and associated value chunks into a single DicomAttribute element. Memory consumption in value
    * data is unbounded (there is no way of knowing how many chunks are in an attribute). This transformation should
    * therefore be used for data which has been vetted in prior transformations.
    */
  val attributeFlow = Flow[DicomPart]
    .statefulMapConcat {
      () =>
        var currentData: Option[DicomAttribute] = None
        var inFragments = false
        var currentFragment: Option[DicomFragment] = None

      {
        case header: DicomHeader =>
          currentData = Some(DicomAttribute(header, Seq.empty))
          Nil
        case fragments: DicomFragments =>
          inFragments = true
          fragments :: Nil
        case endFragments: DicomFragmentsDelimitation =>
          currentFragment = None
          inFragments = false
          endFragments :: Nil
        case item: DicomItem if inFragments =>
          currentFragment = Some(DicomFragment(item.bigEndian, Seq.empty))
          Nil
        case valueChunk: DicomValueChunk if inFragments =>
          currentFragment = currentFragment.map(f => f.copy(valueChunks = f.valueChunks :+ valueChunk))
          if (valueChunk.last)
            currentFragment.map(_ :: Nil).getOrElse(Nil)
          else
            Nil
        case valueChunk: DicomValueChunk =>
          currentData = currentData.map(d => d.copy(valueChunks = d.valueChunks :+ valueChunk))
          if (valueChunk.last)
            currentData.map(_ :: Nil).getOrElse(Nil)
          else
            Nil
        case part => part :: Nil
      }
    }

  /**
    * Filter a stream of dicom parts such that all attributes except those with tags in the white list are discarded.
    * Applies to headers and subsequent value chunks, DicomAttribute:s (as produced by the associated flow) and
    * fragments. Sequences, items, preamble, deflated chunks and unknown parts are not affected.
    *
    * @param tagsWhitelist list of tags to keep
    * @param applyToFmi    if false, this filter does not affect the FMI
    * @return the associated filter Flow
    */
  def partFilter(tagsWhitelist: Seq[Int], applyToFmi: Boolean = false) = Flow[DicomPart].statefulMapConcat {
    () =>
      var discarding = false

    {
      case dicomHeader: DicomHeader if tagsWhitelist.contains(dicomHeader.tag) || dicomHeader.isFmi && !applyToFmi =>
        discarding = false
        dicomHeader :: Nil
      case _: DicomHeader =>
        discarding = true
        Nil

      case valueChunk: DicomValueChunk => if (discarding) Nil else valueChunk :: Nil
      case fragment: DicomFragment => if (discarding) Nil else fragment :: Nil

      case dicomFragments: DicomFragments if tagsWhitelist.contains(dicomFragments.tag) =>
        discarding = false
        dicomFragments :: Nil
      case _: DicomFragments =>
        discarding = true
        Nil

      case _: DicomItem if discarding => Nil
      case _: DicomItemDelimitation if discarding => Nil
      case _: DicomFragmentsDelimitation if discarding =>
        discarding = false
        Nil

      case dicomAttribute: DicomAttribute if tagsWhitelist.contains(dicomAttribute.header.tag) || dicomAttribute.header.isFmi && !applyToFmi =>
        discarding = false
        dicomAttribute :: Nil
      case _: DicomAttribute =>
        discarding = false
        Nil

      case dicomPart =>
        discarding = false
        dicomPart :: Nil
    }
  }

  /**
    * A flow which passes on the input bytes unchanged, but fails for non-DICOM files, determined by the first
    * attribute found
    */
  val validateFlow = Flow[ByteString].via(new DicomValidateFlow(None))

  /**
    * A flow which passes on the input bytes unchanged, fails for non-DICOM files, validates for DICOM files with supported
    * Media Storage SOP Class UID, Transfer Syntax UID combination passed as context
    */
  def validateFlowWithContext(contexts: Seq[Context]) = Flow[ByteString].via(new DicomValidateFlow(Some(contexts)))
}
