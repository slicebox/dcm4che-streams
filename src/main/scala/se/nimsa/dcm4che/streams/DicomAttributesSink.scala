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

import akka.stream.scaladsl.Sink
import org.dcm4che3.data.{Attributes, Fragments, Sequence}
import se.nimsa.dcm4che.streams.DicomParts._

import scala.concurrent.{ExecutionContext, Future}

object DicomAttributesSink {

  private case class AttributesData(attributesStack: Seq[Attributes],
                                    sequenceStack: Seq[Sequence],
                                    currentFragments: Option[Fragments])

  private case class AttributesSinkData(maybeFmi: Option[Attributes],
                                        maybeAttributesData: Option[AttributesData])

  private def setFmiValue(dicomAttribute: DicomAttribute, fmi: Attributes): Attributes = {
    val header = dicomAttribute.header
    if (header.length == 0)
      fmi.setNull(header.tag, header.vr)
    else {
      val bytes = dicomAttribute.valueBytes.toArray
      if (!DicomParsing.isGroupLength(header.tag)) {
        if (dicomAttribute.bigEndian != fmi.bigEndian) header.vr.toggleEndian(bytes, false)
        fmi.setBytes(header.tag, header.vr, bytes)
      }
    }
    fmi
  }

  private def setValue(dicomAttribute: DicomAttribute, attributesData: AttributesData): AttributesData = {
    val header = dicomAttribute.header
    if (header.length == 0) {
      attributesData.attributesStack.headOption.foreach(_.setNull(header.tag, header.vr))
      attributesData
    }
    else {
      val bytes = dicomAttribute.valueBytes.toArray
      if (!DicomParsing.isGroupLength(header.tag)) {
        if (attributesData.attributesStack.nonEmpty && dicomAttribute.bigEndian != attributesData.attributesStack.head.bigEndian)
          header.vr.toggleEndian(bytes, false)
        attributesData.attributesStack.headOption.foreach(_.setBytes(header.tag, header.vr, bytes))
      }
      attributesData
    }
  }

  /**
    * Creates a <code>Sink</code> which ingests DICOM parts as output by the <code>DicomPartFlow</code> followed by the
    * <code>DicomFlows.attributeFlow</code> and materializes into two dcm4che <code>Attributes</code> objects, one for
    * meta data and one for the dataset.
    *
    * Based heavily and exclusively on the dcm4che
    * <a href="https://github.com/dcm4che/dcm4che/blob/master/dcm4che-core/src/test/java/org/dcm4che3/io/DicomInputStreamTest.java">DicomInputStream</a>
    * class (complementing what is not covered by <code>DicomPartFlow</code>.
    *
    * @param ec an implicit ExecutionContext
    * @return a <code>Sink</code> for materializing a flow of DICOM parts into dcm4che <code>Attribute</code>s.
    */
  def attributesSink(implicit ec: ExecutionContext): Sink[DicomPart, Future[(Option[Attributes], Option[Attributes])]] =
    Sink.fold[AttributesSinkData, DicomPart](AttributesSinkData(None, None)) { case (attributesSinkData, dicomPart) =>
      dicomPart match {

        case dicomAttribute: DicomAttribute if dicomAttribute.header.isFmi =>
          val fmi = attributesSinkData.maybeFmi
            .getOrElse(new Attributes(dicomAttribute.bigEndian, 9))
          attributesSinkData.copy(maybeFmi = Some(setFmiValue(dicomAttribute, fmi)))

        case dicomAttribute: DicomAttribute =>
          val attributesData = attributesSinkData.maybeAttributesData
            .getOrElse(AttributesData(Seq(new Attributes(dicomAttribute.bigEndian, 64)), Seq.empty, None))
          attributesSinkData.copy(maybeAttributesData = Some(setValue(dicomAttribute, attributesData)))

        case sequence: DicomSequence =>
          val attributesData = attributesSinkData.maybeAttributesData
            .map { attributesData =>
              val newSeq = attributesData.attributesStack.head.newSequence(sequence.tag, 10)
              attributesData.copy(sequenceStack = newSeq +: attributesData.sequenceStack)
            }
            .getOrElse {
              val attributes = new Attributes(sequence.bigEndian, 64)
              AttributesData(
                Seq(attributes),
                attributes.newSequence(sequence.tag, 10) :: Nil,
                None)
            }
          attributesSinkData.copy(maybeAttributesData = Some(attributesData))

        case _: DicomSequenceDelimitation =>
          attributesSinkData.copy(maybeAttributesData = attributesSinkData.maybeAttributesData.map(attrsData => attrsData.copy(sequenceStack = attrsData.sequenceStack.tail)))

        case fragments: DicomFragments =>
          val attributesData = attributesSinkData.maybeAttributesData
            .getOrElse(AttributesData(
              Seq(new Attributes(fragments.bigEndian, 64)),
              Seq.empty,
              Some(new Fragments(null, fragments.tag, fragments.vr, fragments.bigEndian, 10))))
          attributesSinkData.copy(maybeAttributesData = Some(attributesData))

        case dicomFragment: DicomFragment =>
          attributesSinkData.maybeAttributesData.foreach { attributesData =>
            attributesData.currentFragments.foreach { fragments =>
              val bytes = dicomFragment.bytes.toArray
              if (dicomFragment.bigEndian != fragments.bigEndian)
                fragments.vr.toggleEndian(bytes, false)
              fragments.add(bytes)
            }
          }
          attributesSinkData

        case _: DicomFragmentsDelimitation =>
          attributesSinkData.maybeAttributesData.foreach { attributesData =>
            attributesData.attributesStack.headOption.foreach { attributes =>
              attributesData.currentFragments.foreach { fragments =>
                if (fragments.isEmpty)
                  attributes.setNull(fragments.tag, fragments.vr)
                else {
                  fragments.trimToSize()
                  attributes.setValue(fragments.tag, fragments.vr, fragments)
                }
              }
            }
          }
          attributesSinkData.copy(maybeAttributesData = attributesSinkData.maybeAttributesData.map(_.copy(currentFragments = None)))

        case _: DicomItem =>
          val maybeAttributesData = attributesSinkData.maybeAttributesData.flatMap { attributesData =>
            attributesData.sequenceStack.headOption.map { seq =>
              val attributes = new Attributes(seq.getParent.bigEndian)
              seq.add(attributes)
              attributesData.copy(attributesStack = attributes +: attributesData.attributesStack)
            }
          }
          attributesSinkData.copy(maybeAttributesData = maybeAttributesData)

        case _: DicomItemDelimitation =>
          val maybeAttributesData = attributesSinkData.maybeAttributesData.map { attributesData =>
            attributesData.attributesStack.headOption
              .map { attributes =>
                attributes.trimToSize()
                attributesData.copy(attributesStack = attributesData.attributesStack.tail)
              }
              .getOrElse(attributesData)
          }
          attributesSinkData.copy(maybeAttributesData = maybeAttributesData)

        case _ =>
          attributesSinkData

      }
    }.mapMaterializedValue(_.map { attributesSinkData =>
      (
        attributesSinkData.maybeFmi.map { fmi => fmi.trimToSize(); fmi },
        attributesSinkData.maybeAttributesData.flatMap(_.attributesStack.headOption.map { attributes => attributes.trimToSize(); attributes })
      )
    })

}
