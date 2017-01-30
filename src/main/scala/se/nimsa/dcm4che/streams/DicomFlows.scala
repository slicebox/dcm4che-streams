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
import org.dcm4che3.io.DicomStreamException

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

  private class DicomValidateFlow() extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in = Inlet[ByteString]("DicomValidateFlow.in")
    val out = Outlet[ByteString]("DicomValidateFlow.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var buffer = ByteString.empty
      var isValidated = false

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
            if (buffer.length >= 140)
              if (isPreamble(buffer))
                if (isHeader(buffer.drop(132)))
                  setValidated()
                else
                  setFailed()
              else if (isHeader(buffer))
                setValidated()
              else
                setFailed()
            else
              pull(in)
          }
        }

        override def onUpstreamFinish() = {
          if (!isValidated)
            if (buffer.length == 132 && isPreamble(buffer))
              setValidated()
            else if (buffer.length >= 8 && isHeader(buffer))
              setValidated()
            else
              setFailed()
          completeStage()
        }

        def isHeader(data: ByteString) = DicomParsing.dicomInfo(data).isDefined

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
  val validateFlow = Flow[ByteString].via(new DicomValidateFlow)
}
