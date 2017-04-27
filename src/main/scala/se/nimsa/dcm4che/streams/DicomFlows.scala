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
    *
    * @param tagsWhitelist list of tags to keep.
    * @return the associated filter Flow
    */
  def partFilter(tagsWhitelist: Seq[Int]) = whitelistFilter(tagsWhitelist.contains(_))

  /**
    * Filter a stream of dicom parts such that all attributes that are group length elements except
    * file meta information group length, will be discarded.
    * @return the associated filter Flow
    */
  def groupLengthDiscardFilter = blacklistFilter((tag: Int) => DicomParsing.isGroupLength(tag) && !DicomParsing.isFileMetaInformation(tag))

  /**
    * Discards the file meta information.
    * @return the associated filter Flow
    */
  def fmiDiscardFilter = blacklistFilter((tag: Int) => DicomParsing.isFileMetaInformation(tag), keepPreamble = false)

  /**
    * Blacklist filter for DICOM parts.
    * @param tagCondition blacklist tag condition
    * @param keepPreamble true if preamble should be kept, else false
    * @return Flow of filtered parts
    */
  def blacklistFilter(tagCondition: (Int) => Boolean, keepPreamble: Boolean = true) = tagFilter(tagCondition, isWhitelist = false, keepPreamble)

  /**
    * Tag based whitelist filter for DICOM parts.
    * @param tagCondition whitelist condition
    * @param keepPreamble true if preamble should be kept, else false
    * @return Flow  of filtered parts
    */
  def whitelistFilter(tagCondition: (Int) => Boolean, keepPreamble: Boolean = false) = tagFilter(tagCondition, isWhitelist = true, keepPreamble)


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
      case _: DicomSequenceDelimitation  if discarding => Nil

      case dicomAttribute: DicomAttribute  =>
        discarding = shouldDiscard(dicomAttribute.header.tag, isWhitelist)
        if (discarding) {
          Nil
        } else {
          dicomAttribute :: Nil
        }

      case dicomPart => // FIXME: Unknown part?
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


}
