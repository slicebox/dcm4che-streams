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

import java.util.zip.Deflater

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

/**
  * Various flows for transforming streams of <code>DicomPart</code>s.
  */
object DicomFlows {

  import DicomPartFlow._

  case class DicomAttribute(header: DicomHeader, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)

    def bigEndian = header.bigEndian
  }

  case class DicomFragment(bigEndian: Boolean, valueChunks: Seq[DicomValueChunk]) extends DicomPart {
    def bytes = valueChunks.map(_.bytes).fold(ByteString.empty)(_ ++ _)
  }

  case class ValidationContext(sopClassUID: String, transferSyntax: String)


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
  def partFilter(tagsWhitelist: Seq[Int], applyToFmi: Boolean = false) = whitelistFilter(tagsWhitelist.contains(_), applyToFmi)

  /**
    * Filter a stream of dicom parts such that all attributes that are group length elements except
    * file meta information group length, will be discarded.
    *
    * @return the associated filter Flow
    */
  def groupLengthDiscardFilter = blacklistFilter(DicomParsing.isGroupLength)

  /**
    * Blacklist filter for DICOM parts.
    *
    * @param tagCondition blacklist condition
    * @param applyToFmi   if false, this filter does not affect the FMI
    * @return Flow of filtered parts
    */
  def blacklistFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean = false) = tagFilter(tagCondition, applyToFmi, isWhitelist = false)

  /**
    * Whitelist filter for DICOM parts.
    *
    * @param tagCondition whitelist condition
    * @param applyToFmi   if false, this filter does not affect the FMI
    * @return Flow  of filtered parts
    */
  def whitelistFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean = false) = tagFilter(tagCondition, applyToFmi, isWhitelist = true)


  private def tagFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean, isWhitelist: Boolean) = Flow[DicomPart].statefulMapConcat {
    () =>
      var discarding = false

      def shouldDiscard(tag: Int, isFmi: Boolean, applyToFmi: Boolean, isWhitelist: Boolean) = {
        if (isWhitelist) {
          // Whitelist: condition true or not appply to fmi => discard = false
          !(tagCondition(tag) || isFmi && !applyToFmi)
        } else {
          // Blacklist: condition true or condition true and apply to fmi => discard
          if (tagCondition(tag)) {
            if (isFmi) {
              applyToFmi
            } else {
              true
            }
          } else {
            false
          }
        }
      }

    {
      case dicomHeader: DicomHeader =>
        discarding = shouldDiscard(dicomHeader.tag, dicomHeader.isFmi, applyToFmi, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomHeader :: Nil
        }
      case valueChunk: DicomValueChunk => if (discarding) Nil else valueChunk :: Nil
      case fragment: DicomFragment => if (discarding) Nil else fragment :: Nil

      case dicomFragments: DicomFragments =>
        discarding = shouldDiscard(dicomFragments.tag, isFmi = false, applyToFmi = applyToFmi, isWhitelist = isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomFragments :: Nil
        }

      case _: DicomItem if discarding => Nil
      case _: DicomItemDelimitation if discarding => Nil
      case _: DicomFragmentsDelimitation if discarding =>
        discarding = false
        Nil

      case dicomAttribute: DicomAttribute =>
        discarding = shouldDiscard(dicomAttribute.header.tag, dicomAttribute.header.isFmi, applyToFmi, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomAttribute :: Nil
        }

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
  def validateFlowWithContext(contexts: Seq[ValidationContext]) = Flow[ByteString].via(new DicomValidateFlow(Some(contexts)))

  /**
    * Simple transformation flow for mapping the values of certain attributes to new values. The application is "flat"
    * in the sense that attributes are transformed no matter if they are part of sequences or not.
    *
    * @param transforms Each transform is a 2-tuple of tag number and value transform
    * @return the transformed flow of DICOM parts
    */
  def attributesTransformFlow(transforms: (Int, ByteString => ByteString)*) = Flow[DicomPart].statefulMapConcat {
    () =>
      val tags = transforms.map(_._1)
      var value = ByteString.empty
      var headerMaybe: Option[DicomHeader] = None
      var transformMaybe: Option[ByteString => ByteString] = None

      // update header length according to new value
      def updateHeader(header: DicomHeader, newLength: Int): DicomHeader = {
        val updatedBytes =
          if (header.vr.headerLength() == 8)
            DicomParsing.shortToBytes(newLength, header.bytes, 6, header.bigEndian)
          else
            DicomParsing.intToBytes(newLength, header.bytes, 8, header.bigEndian)
        header.copy(length = newLength, bytes = updatedBytes)
      }

    {
      case header: DicomHeader if tags.contains(header.tag) =>
        headerMaybe = Some(header)
        value = ByteString.empty
        transformMaybe = transforms.find(_._1 == header.tag).map(_._2)
        Nil
      case chunk: DicomValueChunk if transformMaybe.isDefined && headerMaybe.isDefined =>
        value = value ++ chunk.bytes
        if (chunk.last) {
          val newValue = transformMaybe.map(t => t(value)).getOrElse(value)
          val newHeader = headerMaybe.map(updateHeader(_, newValue.length)).get
          transformMaybe = None
          headerMaybe = None
          newHeader :: DicomValueChunk(chunk.bigEndian, newValue, last = true) :: Nil
        } else
          Nil
      case dicomPart => dicomPart :: Nil
    }
  }

  /**
    * A flow which deflates the dataset but leaves the meta information intact. Useful when the dicom parsing in DicomPartFlow
    * has inflated a deflated (1.2.840.10008.1.2.1.99 or 1.2.840.10008.1.2.4.95) file, and analyzed and possibly transformed its
    * attributes. At that stage, in order to maintain valid DICOM information, one can either change the transfer syntax to
    * an appropriate value for non-deflated data, or deflate the data again. This flow helps with the latter.
    *
    * @return
    */
  def deflateDatasetFlow() = {

    case object DicomEndMarker extends DicomPart {
      def bigEndian = false
      def bytes = ByteString.empty
    }

    Flow[DicomPart]
      .concat(Source.single(DicomEndMarker))
      .statefulMapConcat {
        () =>
          var inFmi = false
          val buffer = new Array[Byte](2048)
          val deflater = new Deflater(-1, true)

          def deflate(dicomPart: DicomPart) = {
            val input = dicomPart.bytes
            deflater.setInput(input.toArray)
            var output = ByteString.empty
            while (!deflater.needsInput) {
              val bytesDeflated = deflater.deflate(buffer)
              output = output ++ ByteString(buffer.take(bytesDeflated))
            }
            if (output.isEmpty) Nil else DicomDeflatedChunk(dicomPart.bigEndian, output) :: Nil
          }

          def finishDeflating() = {
            deflater.finish()
            var output = ByteString.empty
            var done = false
            while (!done) {
              val bytesDeflated = deflater.deflate(buffer)
              if (bytesDeflated == 0)
                done = true
              else
                output = output ++ ByteString(buffer.take(bytesDeflated))
            }
            deflater.end()
            if (output.isEmpty) Nil else DicomDeflatedChunk(bigEndian = false, output) :: Nil
          }

        {
          case preamble: DicomPreamble => // preamble, do not deflate
            preamble :: Nil
          case DicomEndMarker => // end of stream, make sure deflater writes final bytes
            finishDeflating()
          case deflatedChunk: DicomDeflatedChunk => // already deflated, pass as-is
            deflatedChunk :: Nil
          case header: DicomHeader if header.isFmi => // FMI, do not deflate and remember we are in FMI
            inFmi = true
            header :: Nil
          case attribute: DicomAttribute if attribute.header.isFmi => // Whole FMI attribute, same as above
            inFmi = true
            attribute :: Nil
          case header: DicomHeader => // Dataset header, remember we are no longer in FMI, deflate
            inFmi = false
            deflate(header)
          case dicomPart if inFmi => // For any dicom part within FMI, do not deflate
            dicomPart :: Nil
          case dicomPart => // For all other cases, deflate
            deflate(dicomPart)
        }
      }
  }

}
