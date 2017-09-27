package se.nimsa.dcm4che.streams

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import org.dcm4che3.data.{StandardElementDictionary, VR}
import se.nimsa.dcm4che.streams.DicomParts._
import se.nimsa.dcm4che.streams.TagPath.{TagPathSequence, TagPathTag}

object DicomModifyFlow {

  /**
    * Class used to specify modifications to individual attributes of a dataset
    *
    * @param tagPath      tag path
    * @param matches      function used to determine if another tag path is matching the tag path of this modification
    *                     (may mean e.g. equals, starts with or ends with)
    * @param modification a modification function
    * @param insert       if tag is absent in dataset it will be created and inserted when `true`
    */
  case class TagModification(tagPath: TagPathTag, matches: TagPath => Boolean, modification: ByteString => ByteString, insert: Boolean)

  object TagModification {

    /**
      * Modification that will modify dataset elements contained within its tag path. "Contained" means equals unless
      * item wildcards are used. E.g. if the modification tag path is (0008,9215)[*}.(0010,0010) the path
      * (0008,9215)[1].(0010,0010) will be contained in that path along with any other item indices. Useful for changing
      * specific attributes in a dataset.
      */
    def contains(tagPath: TagPathTag, modification: ByteString => ByteString, insert: Boolean) =
      TagModification(tagPath, tagPath.hasSubPath, modification, insert)

    /**
      * Modification that will modify dataset elements where the corresponding tag path ends with the tag path of this
      * modification. E.g. both the dataset attributes (0010,0010) and (0008,9215)[1].(0010,0010) will be modified if
      * this tag path is (0010,0010). Useful for changing all instances of a certain attribute.
      */
    def endsWith(tagPath: TagPathTag, modification: ByteString => ByteString, insert: Boolean) =
      TagModification(tagPath, _.endsWith(tagPath), modification, insert)
  }

  /**
    * Modification flow for inserting or overwriting the values of specified attributes. When inserting a new attribute,
    * the corresponding modification function will be called with an empty `ByteString`. A modification is specified by
    * a tag path, a modification function from current value bytes to their replacement, and a flag specifying whether
    * the attribute should be inserted if not present. Attributes are inserted only if they point to existing datasets.
    * That means that sequences are never inserted, only modified. Insertion works according to the `TagPathTag#contains`
    * method, meaning that if sequence wildcards are used in modifications they will apply to all items in a sequence.
    *
    * Note that modified DICOM data may not be valid. This function does not ensure values are padded to even length and
    * changing an attribute may lead to invalid group length attributes such as MediaStorageSOPInstanceUID. There are
    * utility functions in `se.nimsa.ddcm4che.streams` for padding values and flows for adjusting group lengths.
    *
    * @param modifications Any number of `TagModification`s each specifying a tag path, a modification function, and
    *                      a Boolean indicating whether absent values will be inserted or skipped.
    * @return the modified flow of DICOM parts
    */
  def modifyFlow(modifications: TagModification*): Flow[DicomPart, DicomPart, NotUsed] =
    Flow[DicomPart]
      .concat(Source.single(DicomEndMarker))
      .statefulMapConcat {
        () =>
          val sortedModifications = modifications.toList.sortWith((a, b) => a.tagPath < b.tagPath)

          var currentModification: Option[TagModification] = None // current modification
        var currentHeader: Option[DicomHeader] = None // header of current attribute being modified
        var currentTagPath: Option[TagPath] = None
          var value = ByteString.empty // value of current attribute being modified
        var bigEndian = false // endinaness of current attribute
        var explicitVR = true // VR representation of current attribute

          var tagPathSequence: Option[TagPathSequence] = None // current sequence path. None = currently in root dataset

          def updateSyntax(header: DicomHeader): Unit = {
            bigEndian = header.bigEndian
            explicitVR = header.explicitVR
          }

          def headerAndValueParts(tagPath: TagPath, modification: ByteString => ByteString): List[DicomPart] = {
            val valueBytes = modification(ByteString.empty)
            val vr = StandardElementDictionary.INSTANCE.vrOf(tagPath.tag)
            if (vr == VR.UN) throw new IllegalArgumentException("Tag is not present in dictionary, cannot determine value representation")
            if (vr == VR.SQ) throw new IllegalArgumentException("Cannot insert sequence attributes")
            val isFmi = DicomParsing.isFileMetaInformation(tagPath.tag)
            val header = DicomHeader(tagPath.tag, vr, valueBytes.length, isFmi, bigEndian, explicitVR)
            val value = DicomValueChunk(bigEndian, valueBytes, last = true)
            header :: value :: Nil
          }

          def isBetween(tagToTest: TagPath, upperTag: TagPath, lowerTagMaybe: Option[TagPath]) =
            tagToTest < upperTag && lowerTagMaybe.forall(_ < tagToTest)

          def isInDataset(tagToTest: TagPath, sequenceMaybe: Option[TagPathSequence]) =
            sequenceMaybe.map(tagToTest.startsWithSubPath).getOrElse(tagToTest.isRoot)

        {
          case header: DicomHeader =>
            updateSyntax(header)
            val tagPath = tagPathSequence.map(_.thenTag(header.tag)).getOrElse(TagPath.fromTag(header.tag))
            val insertParts = sortedModifications
              .filter(_.insert)
              .filter(m => isBetween(m.tagPath, tagPath, currentTagPath))
              .filter(m => isInDataset(m.tagPath, tagPathSequence))
              .flatMap(m => headerAndValueParts(m.tagPath, m.modification))
            val modifyPart = sortedModifications
              .find(_.matches(tagPath))
              .map { tagModification =>
                if (header.length > 0) {
                  currentHeader = Some(header)
                  currentModification = Some(tagModification)
                  value = ByteString.empty
                  Nil
                } else {
                  val newValue = tagModification.modification(ByteString.empty)
                  val newHeader = header.withUpdatedLength(newValue.length)
                  val chunkOrNot = if (newValue.nonEmpty) DicomValueChunk(bigEndian, newValue, last = true) :: Nil else Nil
                  newHeader :: chunkOrNot
                }
              }
              .getOrElse(header :: Nil)
            currentTagPath = Some(tagPath)
            insertParts ::: modifyPart

          case chunk: DicomValueChunk if currentModification.isDefined && currentHeader.isDefined =>
            value = value ++ chunk.bytes
            if (chunk.last) {
              val newValue = currentModification.get.modification(value)
              val newHeader = currentHeader.get.withUpdatedLength(newValue.length)
              currentModification = None
              currentHeader = None
              val chunkOrNot = if (newValue.nonEmpty) DicomValueChunk(bigEndian, newValue, last = true) :: Nil else Nil
              newHeader :: chunkOrNot
            } else
              Nil

          case sequence: DicomSequence =>
            tagPathSequence = tagPathSequence.map(_.thenSequence(sequence.tag)).orElse(Some(TagPath.fromSequence(sequence.tag)))
            sequence :: Nil

          case sequenceDelimitation: DicomSequenceDelimitation =>
            tagPathSequence = tagPathSequence.flatMap(_.previous)
            sequenceDelimitation :: Nil

          case fragments: DicomFragments =>
            tagPathSequence = tagPathSequence.map(_.thenSequence(fragments.tag)).orElse(Some(TagPath.fromSequence(fragments.tag)))
            fragments :: Nil

          case fragmentsDelimitation: DicomFragmentsDelimitation =>
            tagPathSequence = tagPathSequence.flatMap(_.previous)
            fragmentsDelimitation :: Nil

          case item: DicomSequenceItem =>
            tagPathSequence = tagPathSequence.flatMap(s => s.previous.map(_.thenSequence(s.tag, item.index)).orElse(Some(TagPath.fromSequence(s.tag, item.index))))
            item :: Nil

          case itemDelimitation: DicomSequenceItemDelimitation =>
            itemDelimitation :: Nil

          case DicomEndMarker =>
            sortedModifications
              .filter(_.insert)
              .filter(_.tagPath.isRoot)
              .filter(m => currentTagPath.exists(_ < m.tagPath))
              .flatMap(m => headerAndValueParts(m.tagPath, m.modification))

          case part =>
            part :: Nil
        }
      }

}
