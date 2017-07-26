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
import org.dcm4che3.data.Tag
import org.dcm4che3.io.DicomStreamException
import se.nimsa.dcm4che.streams.DicomParts._

/**
  * Various flows for transforming streams of <code>DicomPart</code>s.
  */
object DicomFlows {

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
    *
    * @param tagsWhitelist list of tags to keep.
    * @return the associated filter Flow
    */
  def whitelistFilter(tagsWhitelist: Seq[Int]): Flow[DicomPart, DicomPart, NotUsed] = whitelistFilter(tagsWhitelist.contains(_))

  /**
    * Filter a stream of dicom parts such that attributes with tags in the black list are discarded.
    *
    * @param tagsBlacklist list of tags to discard.
    * @return the associated filter Flow
    */
  def blacklistFilter(tagsBlacklist: Seq[Int]): Flow[DicomPart, DicomPart, NotUsed] = blacklistFilter(tagsBlacklist.contains(_))

  /**
    * Filter a stream of dicom parts such that all attributes that are group length elements except
    * file meta information group length, will be discarded. Group Length (gggg,0000) Standard Data Elements
    * have been retired in the standard.
    *
    * @return the associated filter Flow
    */
  def groupLengthDiscardFilter = blacklistFilter((tag: Int) => DicomParsing.isGroupLength(tag) && !DicomParsing.isFileMetaInformation(tag))

  /**
    * Discards the file meta information.
    *
    * @return the associated filter Flow
    */
  def fmiDiscardFilter = blacklistFilter((tag: Int) => DicomParsing.isFileMetaInformation(tag), keepPreamble = false)

  /**
    * Blacklist filter for DICOM parts.
    *
    * @param tagCondition blacklist tag condition
    * @param keepPreamble true if preamble should be kept, else false
    * @return Flow of filtered parts
    */
  def blacklistFilter(tagCondition: (Int) => Boolean, keepPreamble: Boolean = true) = tagFilter(tagCondition, isWhitelist = false, keepPreamble)

  /**
    * Tag based whitelist filter for DICOM parts.
    *
    * @param tagCondition whitelist condition
    * @param keepPreamble true if preamble should be kept, else false
    * @return Flow of filtered parts
    */
  def whitelistFilter(tagCondition: (Int) => Boolean, keepPreamble: Boolean = false): Flow[DicomPart, DicomPart, NotUsed] = tagFilter(tagCondition, isWhitelist = true, keepPreamble)


  private def tagFilter(tagCondition: (Int) => Boolean, isWhitelist: Boolean, keepPreamble: Boolean) = Flow[DicomPart].statefulMapConcat {
    () =>
      var discarding = false

      def shouldDiscard(tag: Int, isWhitelist: Boolean) = {
        if (isWhitelist) {
          !tagCondition(tag) // Whitelist: condition true => keep
        } else {
          tagCondition(tag) // Blacklist: condition true => discard
        }
      }

    {
      case dicomPreamble: DicomPreamble =>
        if (keepPreamble) {
          dicomPreamble :: Nil
        } else {
          Nil
        }

      case dicomHeader: DicomHeader =>
        discarding = shouldDiscard(dicomHeader.tag, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomHeader :: Nil
        }
      case valueChunk: DicomValueChunk => if (discarding) Nil else valueChunk :: Nil
      case fragment: DicomFragment => if (discarding) Nil else fragment :: Nil

      case dicomFragments: DicomFragments =>
        discarding = shouldDiscard(dicomFragments.tag, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomFragments :: Nil
        }

      case _: DicomItem if discarding => Nil
      case _: DicomItemDelimitation if discarding => Nil
      case _: DicomFragmentsDelimitation if discarding => Nil

      case _: DicomSequence if discarding => Nil
      case _: DicomSequenceDelimitation if discarding => Nil

      case dicomAttribute: DicomAttribute =>
        discarding = shouldDiscard(dicomAttribute.header.tag, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomAttribute :: Nil
        }

      case dicomPart =>
        if (isWhitelist) {
          Nil
        } else {
          discarding = false
          dicomPart :: Nil
        }
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
    * A flow which deflates the dataset but leaves the meta information intact. Useful when the dicom parsing in DicomPartFlow
    * has inflated a deflated (1.2.840.10008.1.2.1.99 or 1.2.840.10008.1.2.4.95) file, and analyzed and possibly transformed its
    * attributes. At that stage, in order to maintain valid DICOM information, one can either change the transfer syntax to
    * an appropriate value for non-deflated data, or deflate the data again. This flow helps with the latter.
    *
    * @return the associated DicomPart Flow
    */
  def deflateDatasetFlow() = Flow[DicomPart]
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
        case DicomEndMarker => // end of stream, make sure deflater writes final bytes if deflating has occurred
          if (deflater.getBytesRead > 0)
            finishDeflating()
          else
            Nil
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

  /**
    * Collect the attributes specified by the input set of tags while buffering all elements of the stream. When the
    * stream has moved past the last attribute to collect, a DicomAttributesElement is emitted containing a list of
    * DicomAttributeParts with the collected information, followed by all buffered elements. Remaining elements in the
    * stream are immediately emitted downstream without buffering.
    *
    * This flow is used when there is a need to "look ahead" for certain information in the stream so that streamed
    * elements can be processed correctly according to this information. As an example, an implementation may have
    * different graph paths for different modalities and the modality must be known before any elements are processed.
    *
    * @param tags          tag numbers of attributes to collect. Collection (and hence buffering) will end when the
    *                      stream moves past the highest tag number
    * @param maxBufferSize the maximum allowed size of the buffer (to avoid running out of memory). The flow will fail
    *                      if this limit is exceed. Set to 0 for an unlimited buffer size
    * @return A DicomPart Flow which will begin with a DicomAttributesPart followed by the input elements
    */
  def collectAttributesFlow(tags: Set[Int], maxBufferSize: Int = 1000000): Flow[DicomPart, DicomPart, NotUsed] =
    Flow[DicomPart]
      .concat(Source.single(DicomEndMarker))
      .statefulMapConcat {
        val stopTag = if (tags.isEmpty) 0 else tags.max

        () =>
          var reachedEnd = false
          var currentBufferSize = 0
          var currentAttribute: Option[DicomAttribute] = None
          var buffer: List[DicomPart] = Nil
          var attributes = Seq.empty[DicomAttribute]

          def attributesAndBuffer() = {
            val parts = DicomAttributes(attributes) :: buffer

            reachedEnd = true
            buffer = Nil
            currentBufferSize = 0

            parts
          }

        {
          case DicomEndMarker if reachedEnd =>
            Nil

          case DicomEndMarker =>
            attributesAndBuffer()

          case part if reachedEnd =>
            part :: Nil

          case part =>
            currentBufferSize = currentBufferSize + part.bytes.size
            if (maxBufferSize > 0 && currentBufferSize > maxBufferSize) {
              throw new DicomStreamException("Error collecting attributes: max buffer size exceeded")
            }

            buffer = buffer :+ part

            part match {
              case header: DicomHeader if tags.contains(header.tag) =>
                currentAttribute = Some(DicomAttribute(header, Seq.empty))
                Nil

              case _: DicomHeader =>
                currentAttribute = None
                Nil

              case valueChunk: DicomValueChunk =>

                currentAttribute match {
                  case Some(attribute) =>
                    val updatedAttribute = attribute.copy(valueChunks = attribute.valueChunks :+ valueChunk)
                    currentAttribute = Some(updatedAttribute)
                    if (valueChunk.last) {
                      attributes = attributes :+ updatedAttribute
                      currentAttribute = None
                      if (updatedAttribute.header.tag >= stopTag)
                        attributesAndBuffer()
                      else
                        Nil
                    } else
                      Nil

                  case None => Nil
                }

              case _ => Nil
            }
        }
      }

  /**
    * Remove attributes from stream that may contain large quantities of data (bulk data)
    *
    * Rules ported from [[https://github.com/dcm4che/dcm4che/blob/3.3.8/dcm4che-core/src/main/java/org/dcm4che3/io/BulkDataDescriptor.java#L58 dcm4che]].
    * Defined [[http://dicom.nema.org/medical/dicom/current/output/html/part04.html#table_Z.1-1 here in the DICOM standard]].
    * @return the associated DicomPart Flow
    */
  val bulkDataFilter = Flow[DicomPart]
    .statefulMapConcat {

      def normalizeRepeatingGroup(tag: Int) = {
        val gg000000 = tag & 0xffe00000
        if (gg000000 == 0x50000000 || gg000000 == 0x60000000) tag & 0xffe0ffff else tag
      }

      () =>
        var sequenceStack = Seq.empty[DicomSequence]
        var discarding = false

      {
        case sq: DicomSequence =>
          sequenceStack = sq +: sequenceStack
          sq :: Nil
        case sqd: DicomSequenceDelimitation =>
          sequenceStack = sequenceStack.drop(1)
          sqd :: Nil
        case dh: DicomHeader =>
          discarding =
            normalizeRepeatingGroup(dh.tag) match {
              case Tag.PixelDataProviderURL => true
              case Tag.AudioSampleData => true
              case Tag.CurveData => true
              case Tag.SpectroscopyData => true
              case Tag.OverlayData => true
              case Tag.EncapsulatedDocument => true
              case Tag.FloatPixelData => true
              case Tag.DoubleFloatPixelData => true
              case Tag.PixelData => sequenceStack.isEmpty
              case Tag.WaveformData => sequenceStack.length == 1 && sequenceStack.head.tag == Tag.WaveformSequence
              case _ => false
            }
          if (discarding) Nil else dh :: Nil
        case dvc: DicomValueChunk =>
          if (discarding) Nil else dvc :: Nil
        case p: DicomPart =>
          discarding = false
          p :: Nil
      }
    }

}
