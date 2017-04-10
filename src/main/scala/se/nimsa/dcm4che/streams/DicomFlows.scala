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

  case class Context(sopClassUID: String, transferSyntax: String)



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
    * @return the associated filter Flow
    */
  def groupLengthDiscardFilter = blacklistFilter(DicomParsing.isGroupLength(_), applyToFmi = false)

  /**
    * Blacklist filter for DICOM parts.
    * @param tagCondition blacklist condition
    * @param applyToFmi if false, this filter does not affect the FMI
    * @return Flow of filtered parts
    */
  def blacklistFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean = false) = tagFilter(tagCondition, applyToFmi, isWhitelist = false)

  /**
    * Whitelist filter for DICOM parts.
    * @param tagCondition whitelist condition
    * @param applyToFmi if false, this filter does not affect the FMI
    * @return Flow  of filtered parts
    */
  def whitelistFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean = false) = tagFilter(tagCondition, applyToFmi, isWhitelist = true)


  private def tagFilter(tagCondition: (Int) => Boolean, applyToFmi: Boolean = false, isWhitelist: Boolean = true) = Flow[DicomPart].statefulMapConcat {
    () =>
      var discarding = false

      def shouldDiscard(tag: Int, isFmi: Boolean, applyToFmi: Boolean, isWhitelist:Boolean) = {
        if (isWhitelist) {
          // Whitelist: condition true or not appply to fmi => discard = false
          !((tagCondition(tag) || isFmi && !applyToFmi))
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
        discarding = shouldDiscard(dicomFragments.tag, false, applyToFmi, isWhitelist)
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

      case dicomAttribute: DicomAttribute  =>
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
  def validateFlowWithContext(contexts: Seq[Context]) = Flow[ByteString].via(new DicomValidateFlow(Some(contexts)))

}
