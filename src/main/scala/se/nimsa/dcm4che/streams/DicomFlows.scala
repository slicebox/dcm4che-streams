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
import org.dcm4che3.data.{Tag, VR}
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
  val attributeFlow: Flow[DicomPart, DicomPart, NotUsed] = Flow[DicomPart]
    .statefulMapConcat {
      () =>
        var currentData: Option[DicomAttribute] = None
        var inFragments = false
        var currentFragment: Option[DicomFragment] = None

      {
        case header: DicomHeader =>
          currentData = Some(DicomAttribute(header, Seq.empty))
          if (header.length <= 0) currentData.get :: Nil else Nil
        case fragments: DicomFragments =>
          inFragments = true
          fragments :: Nil
        case endFragments: DicomFragmentsDelimitation =>
          currentFragment = None
          inFragments = false
          endFragments :: Nil
        case fragmentsItem: DicomFragmentsItem =>
          currentFragment = Some(DicomFragment(fragmentsItem.index, fragmentsItem.bigEndian, Seq.empty))
          if (fragmentsItem.length <= 0) currentFragment.get :: Nil else Nil
        case valueChunk: DicomValueChunk if inFragments =>
          currentFragment = currentFragment.map(f => f.copy(valueChunks = f.valueChunks :+ valueChunk))
          if (valueChunk.last)
            currentFragment.map(_ :: Nil).getOrElse(Nil)
          else
            Nil
        case valueChunk: DicomValueChunk =>
          currentData = currentData.map(d => d.copy(valueChunks = d.valueChunks :+ valueChunk))
          if (valueChunk.last) currentData.map(_ :: Nil).getOrElse(Nil) else Nil
        case part => part :: Nil
      }
    }

  /**
    * Filter a stream of dicom parts such that all attributes except those with tags in the white list are discarded.
    * Only attributes in the root dataset are considered. All other parts such as preamble are discarded.
    *
    * Note that it is up to the user of this function to make sure the modified DICOM data is valid.
    *
    * @param tagsWhitelist list of tags to keep.
    * @return the associated filter Flow
    */
  def whitelistFilter(tagsWhitelist: Set[Int]): Flow[DicomPart, DicomPart, NotUsed] =
    tagFilter(_ => false)(currentPath => currentPath.isRoot && tagsWhitelist.contains(currentPath.tag))

  /**
    * Filter a stream of dicom parts such that attributes with tag paths in the black list are discarded. Tag paths in
    * the blacklist are removed in the root dataset as well as any sequences, and entire sequences or items in sequences
    * can be removed.
    *
    * Note that it is up to the user of this function to make sure the modified DICOM data is valid.
    *
    * @param blacklistPaths list of tag paths to discard.
    * @return the associated filter Flow
    */
  def blacklistFilter(blacklistPaths: Set[TagPath]): Flow[DicomPart, DicomPart, NotUsed] =
    tagFilter(_ => true)(currentPath => !blacklistPaths.exists(currentPath.startsWithSuperPath))

  /**
    * Filter a stream of dicom parts such that all attributes that are group length elements except
    * file meta information group length, will be discarded. Group Length (gggg,0000) Standard Data Elements
    * have been retired in the standard.
    *
    * Note that it is up to the user of this function to make sure the modified DICOM data is valid.
    *
    * @return the associated filter Flow
    */
  def groupLengthDiscardFilter: Flow[DicomPart, DicomPart, NotUsed] =
    tagFilter(_ => true)(tagPath => !DicomParsing.isGroupLength(tagPath.tag) || DicomParsing.isFileMetaInformation(tagPath.tag))

  /**
    * Discards the file meta information.
    *
    * @return the associated filter Flow
    */
  def fmiDiscardFilter: Flow[DicomPart, DicomPart, NotUsed] =
    tagFilter(_ => false)(tagPath => !DicomParsing.isFileMetaInformation(tagPath.tag))

  /**
    * Filter a stream of dicom parts leaving only those for which the supplied tag condition is `true`. As the stream of
    * dicom parts is flowing, a `TagPath` state is updated. For each such update, the tag condition is evaluated. If it
    * renders `false`, parts are discarded until it renders `true` again.
    *
    * Note that it is up to the user of this function to make sure the modified DICOM data is valid. When filtering
    * items from a sequence, item indices are preserved (i.e. not updated).
    *
    * @param tagCondition     function that determines if dicom parts should be discarded based on the current tag path
    * @param defaultCondition determines whether to keep or discard elements with no tag path such as the preamble and
    *                         synthetic dicom parts inserted to hold state.
    * @return the filtered flow
    */
  def tagFilter(defaultCondition: DicomPart => Boolean)(tagCondition: TagPath => Boolean): Flow[DicomPart, DicomPart, NotUsed] =
    DicomFlowFactory.create(new DicomFlow with ItemHandling with TagPathTracking {

      var keeping = false

      override def onValueChunk(part: DicomValueChunk): List[DicomPart] = if (keeping) part :: Nil else Nil
      override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = if (keeping) part :: Nil else Nil
      override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = if (keeping) part :: Nil else Nil
      override def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = if (keeping) part :: Nil else Nil
      override def onPart(part: DicomPart): List[DicomPart] = {
        keeping = tagPath match {
          case Some(path) => tagCondition(path)
          case None => defaultCondition(part)
        }
        if (keeping) part :: Nil else Nil
      }
    })

  /**
    * A flow which passes on the input bytes unchanged, but fails for non-DICOM files, determined by the first
    * attribute found
    */
  val validateFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new DicomValidateFlow(None))

  /**
    * A flow which passes on the input bytes unchanged, fails for non-DICOM files, validates for DICOM files with supported
    * Media Storage SOP Class UID, Transfer Syntax UID combination passed as context
    */
  def validateFlowWithContext(contexts: Seq[ValidationContext]): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new DicomValidateFlow(Some(contexts)))

  /**
    * A flow which deflates the dataset but leaves the meta information intact. Useful when the dicom parsing in DicomPartFlow
    * has inflated a deflated (1.2.840.10008.1.2.1.99 or 1.2.840.10008.1.2.4.95) file, and analyzed and possibly transformed its
    * attributes. At that stage, in order to maintain valid DICOM information, one can either change the transfer syntax to
    * an appropriate value for non-deflated data, or deflate the data again. This flow helps with the latter.
    *
    * @return the associated DicomPart Flow
    */
  def deflateDatasetFlow(): Flow[DicomPart, DicomPart, NotUsed] =
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
  def collectAttributesFlow(tags: Set[Int], maxBufferSize: Int = 1000000): Flow[DicomPart, DicomPart, NotUsed] = {
    val maxTag = if (tags.isEmpty) 0 else tags.max
    val tagCondition = tags.contains _
    val stopCondition = if (tags.isEmpty) (_: Int) => true else (tag: Int) => tag > maxTag
    collectAttributesFlow(tagCondition, stopCondition, maxBufferSize)
  }

  /**
    * Collect attributes whenever the input tag condition yields `true` while buffering all elements of the stream. When
    * the stop condition yields `true`, a DicomAttributesElement is emitted containing a list of
    * DicomAttributeParts with the collected information, followed by all buffered elements. Remaining elements in the
    * stream are immediately emitted downstream without buffering.
    *
    * This flow is used when there is a need to "look ahead" for certain information in the stream so that streamed
    * elements can be processed correctly according to this information. As an example, an implementation may have
    * different graph paths for different modalities and the modality must be known before any elements are processed.
    *
    * @param tagCondition  function determining the condition(s) for which attributes are collected
    * @param stopCondition function determining the condition for when collection should stop and attributes are emitted
    * @param maxBufferSize the maximum allowed size of the buffer (to avoid running out of memory). The flow will fail
    *                      if this limit is exceed. Set to 0 for an unlimited buffer size
    * @return A DicomPart Flow which will begin with a DicomAttributesPart followed by the input elements
    */
  def collectAttributesFlow(tagCondition: Int => Boolean, stopCondition: Int => Boolean, maxBufferSize: Int): Flow[DicomPart, DicomPart, NotUsed] =
    Flow[DicomPart]
      .concat(Source.single(DicomEndMarker))
      .statefulMapConcat {
        () =>
          var reachedEnd = false
          var currentBufferSize = 0
          var currentAttribute: Option[DicomAttribute] = None
          var buffer: List[DicomPart] = Nil
          var attributes: List[DicomAttribute] = Nil

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
            if (maxBufferSize > 0 && currentBufferSize > maxBufferSize) {
              throw new DicomStreamException("Error collecting attributes: max buffer size exceeded")
            }

            buffer = buffer :+ part
            currentBufferSize = currentBufferSize + part.bytes.size

            part match {
              case header: DicomHeader if stopCondition(header.tag) =>
                attributesAndBuffer()

              case header: DicomHeader if tagCondition(header.tag) =>
                currentAttribute = Some(DicomAttribute(header, Seq.empty))
                if (header.length == 0) {
                  attributes = attributes :+ currentAttribute.get
                  currentAttribute = None
                }
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
                    }
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
    *
    * Note that it is up to the user of this function to make sure the modified DICOM data is valid.
    *
    * @return the associated DicomPart Flow
    */
  val bulkDataFilter: Flow[DicomPart, DicomPart, NotUsed] =
    Flow[DicomPart]
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

  /**
    * Buffers all file meta information attributes and calculates their lengths, then emits the correct file meta
    * information group length attribute followed by remaining FMI.
    */
  val fmiGroupLengthFlow: Flow[DicomPart, DicomPart, NotUsed] = Flow[DicomPart]
    .concat(Source.single(DicomEndMarker))
    .via(collectAttributesFlow(DicomParsing.isFileMetaInformation, (tag: Int) => !DicomParsing.isFileMetaInformation(tag), 1000000))
    .via(tagFilter(_ => true)(tagPath => !DicomParsing.isFileMetaInformation(tagPath.tag)))
    .statefulMapConcat {

      () =>
        var fmi = List.empty[DicomPart]
        var hasEmitted = false

      {
        case fmiAttributes: DicomAttributes =>
          if (fmiAttributes.attributes.nonEmpty) {
            val fmiAttributesNoLength = fmiAttributes.attributes
              .filter(_.header.tag != Tag.FileMetaInformationGroupLength)
            val length = fmiAttributesNoLength.map(_.bytes.length).sum
            val lengthHeader = DicomHeader(Tag.FileMetaInformationGroupLength, VR.OB, 4, isFmi = true, bigEndian = false, explicitVR = true, ByteString(2, 0, 0, 0, 85, 76, 4, 0))
            val lengthChunk = DicomValueChunk(bigEndian = false, intToBytesLE(length), last = true)
            val fmiParts = fmiAttributesNoLength.toList.flatMap(attribute => attribute.header :: attribute.valueChunks.toList)
            fmi = lengthHeader :: lengthChunk :: fmiParts
          }
          Nil

        case preamble: DicomPreamble if hasEmitted => preamble :: Nil
        case preamble: DicomPreamble if !hasEmitted =>
          hasEmitted = true
          preamble :: fmi

        case header: DicomHeader if hasEmitted => header :: Nil
        case header: DicomHeader if !hasEmitted =>
          hasEmitted = true
          fmi ::: header :: Nil

        case DicomEndMarker if hasEmitted => Nil
        case DicomEndMarker if !hasEmitted =>
          hasEmitted = true
          fmi

        case part => part :: Nil
      }
    }

  /**
    * Sets any sequences and/or items with determinate length to undeterminate length (length = -1) and inserts
    * delimiters.
    *
    * Most flows dealing with sequences require this filter to function as intended, see the corresponding flow
    * documentation.
    */
  val sequenceLengthFilter: Flow[DicomPart, DicomPart, NotUsed] =
    DicomFlowFactory.create(new DicomFlow with DelimitationAlways)
      .via(DicomFlowFactory.create(new DicomFlow with ItemHandling {
        val undeterminateBytes = ByteString(0xFF, 0xFF, 0xFF, 0xFF)
        val zeroBytes = ByteString(0x00, 0x00, 0x00, 0x00)

        override def onSequenceStart(part: DicomSequence): List[DicomPart] =
          part.copy(length = -1, bytes = part.bytes.dropRight(4) ++ undeterminateBytes) :: Nil
        override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] =
          part.copy(bytes = tagToBytes(0xFFFEE0DD, part.bigEndian) ++ zeroBytes) :: Nil
        override def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] =
          part.copy(length = -1, bytes = part.bytes.dropRight(4) ++ undeterminateBytes) :: Nil
        override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] =
          part.copy(bytes = tagToBytes(0xFFFEE00D, part.bigEndian) ++ zeroBytes) :: Nil
      }))
}


