package se.nimsa.dcm4che.streams

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import org.dcm4che3.io.DicomStreamException
import se.nimsa.dcm4che.streams.DicomParts._
import se.nimsa.dcm4che.streams.TagPath.{TagPathSequence, TagPathTag}

/**
  * This class defines events for modular construction of DICOM flows. Events correspond to the DICOM parts commonly
  * encountered in a stream of DICOM data. Subclasses and/or traits add functionality by overriding and implementing
  * events. The stream entering the stage can be modified as well as the mapping from DICOM part to event.
  */
abstract class DicomFlow {

  def onPreamble(part: DicomPreamble): List[DicomPart]
  def onHeader(part: DicomHeader): List[DicomPart]
  def onValueChunk(part: DicomValueChunk): List[DicomPart]
  def onSequenceStart(part: DicomSequence): List[DicomPart]
  def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart]
  def onFragmentsStart(part: DicomFragments): List[DicomPart]
  def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart]
  def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart]
  def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart]
  def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart]
  def onDeflatedChunk(part: DicomDeflatedChunk): List[DicomPart]
  def onUnknownPart(part: DicomUnknownPart): List[DicomPart]
  def onPart(part: DicomPart): List[DicomPart]

  /**
    * @return the flow of `DicomPart`s entering the stage
    */
  def baseFlow: Flow[DicomPart, DicomPart, NotUsed] = Flow[DicomPart]

  /**
    * This method defines the mapping from `DicomPart` to event used to create the stage, see 'DicomFlowFactory.create'.
    *
    * @param dicomPart the incoming 'DicomPart'
    * @return the `List` of `DicomPart`s
    */
  def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case part: DicomPreamble => onPreamble(part)
    case part: DicomHeader => onHeader(part)
    case part: DicomValueChunk => onValueChunk(part)
    case part: DicomSequence => onSequenceStart(part)
    case part: DicomSequenceDelimitation => onSequenceEnd(part)
    case part: DicomFragments => onFragmentsStart(part)
    case part: DicomFragmentsDelimitation => onFragmentsEnd(part)
    case part: DicomSequenceItem => onSequenceItemStart(part)
    case part: DicomSequenceItemDelimitation => onSequenceItemEnd(part)
    case part: DicomFragmentsItem => onFragmentsItemStart(part)
    case part: DicomDeflatedChunk => onDeflatedChunk(part)
    case part: DicomUnknownPart => onUnknownPart(part)
    case part => onPart(part)
  }
}

/**
  * Basic implementation of events where DICOM parts are simply passed on downstream. When implementing new flows,
  * this trait is most often included as the first mixin to provide basic behavior which is then overridden for the
  * appropriate events.
  */
trait JustEmit extends DicomFlow {
  def onPreamble(part: DicomPreamble): List[DicomPart] = part :: Nil
  def onHeader(part: DicomHeader): List[DicomPart] = part :: Nil
  def onValueChunk(part: DicomValueChunk): List[DicomPart] = part :: Nil
  def onSequenceStart(part: DicomSequence): List[DicomPart] = part :: Nil
  def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = part :: Nil
  def onFragmentsStart(part: DicomFragments): List[DicomPart] = part :: Nil
  def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = part :: Nil
  def onDeflatedChunk(part: DicomDeflatedChunk): List[DicomPart] = part :: Nil
  def onUnknownPart(part: DicomUnknownPart): List[DicomPart] = part :: Nil
  def onPart(part: DicomPart): List[DicomPart] = part :: Nil
  def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = part :: Nil
  def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = part :: Nil
  def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart] = part :: Nil
}

/**
  * Similar implementation to `JustEmit` with the difference that all events forward to the `onPart` event. Useful for
  * simple filters which implement a common behavior for all DICOM parts. This implementation is then provided in the
  * `onPart` method.
  */
trait TreatAsPart extends DicomFlow {
  def onPreamble(part: DicomPreamble): List[DicomPart] = onPart(part)
  def onHeader(part: DicomHeader): List[DicomPart] = onPart(part)
  def onValueChunk(part: DicomValueChunk): List[DicomPart] = onPart(part)
  def onSequenceStart(part: DicomSequence): List[DicomPart] = onPart(part)
  def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = onPart(part)
  def onFragmentsStart(part: DicomFragments): List[DicomPart] = onPart(part)
  def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = onPart(part)
  def onDeflatedChunk(part: DicomDeflatedChunk): List[DicomPart] = onPart(part)
  def onUnknownPart(part: DicomUnknownPart): List[DicomPart] = onPart(part)
  def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = onPart(part)
  def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = onPart(part)
  def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart] = onPart(part)
  def onPart(part: DicomPart): List[DicomPart]
}

/**
  * This mixin adds an event marking the end of the DICOM stream. It does not add DICOM parts to the stream.
  */
trait EndEvent extends DicomFlow {
  def onEnd(): List[DicomPart] = Nil

  override def baseFlow: Flow[DicomPart, DicomPart, NotUsed] =
    super.baseFlow.concat(Source.single(DicomEndMarker)) // add marker to end of stream

  override def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case DicomEndMarker => onEnd() // call event, do not emit marker
    case part => super.handlePart(part)
  }
}

object DicomValueChunkMarker extends DicomValueChunk(bigEndian = false, ByteString.empty, last = true)

/**
  * This mixin makes sure the `onValueChunk` event is called also for empty attributes. This special case requires
  * special handling since empty attributes consist of a `DicomHeader`, but is not followed by a `DicomValueChunk`.
  */
trait GuaranteedValueEvent extends DicomFlow {

  abstract override def onHeader(part: DicomHeader): List[DicomPart] =
    if (part.length == 0)
      super.onHeader(part) ::: onValueChunk(DicomValueChunkMarker)
    else
      super.onHeader(part)

  abstract override def onValueChunk(part: DicomValueChunk): List[DicomPart] =
    super.onValueChunk(part).filterNot(_ == DicomValueChunkMarker)
}

object DicomSequenceDelimitationMarker extends DicomSequenceDelimitation(bigEndian = false, ByteString.empty)

class DicomSequenceItemDelimitationMarker(item: Int) extends DicomSequenceItemDelimitation(item, bigEndian = false, ByteString.empty)

/**
  * By mixing in this trait, sequences and items with determinate length will be concluded by delimitation events, just
  * as is the case with sequences and items with indeterminate length and which are concluded by delimitation parts. This
  * makes it easier to create DICOM flows that react to the beginning and ending of sequences and items, as no special
  * care has to be taken for DICOM data with determinate length sequences and items.
  *
  * This implementation contains state (the number of bytes left to read in a sequence/item). The corresponding flow
  * must therefore be created anew for each stream.
  */
trait GuaranteedDelimitationEvents extends DicomFlow {
  var partStack: List[(LengthPart, Long)] = Nil

  def subtractLength(part: DicomPart): List[(LengthPart, Long)] =
    partStack.map {
      case (item: DicomSequenceItem, bytesLeft) => (item, bytesLeft - part.bytes.length)
      case (sequence, bytesLeft) => (sequence, bytesLeft - part.bytes.length)
    }

  def maybeDelimit(): List[DicomPart] = {
    val delimits: List[DicomPart] = partStack
      .filter(_._2 <= 0) // find items and sequences that have ended
      .map { // create delimiters for those
      case (item: DicomSequenceItem, _) => new DicomSequenceItemDelimitationMarker(item.index)
      case (_, _) => DicomSequenceDelimitationMarker
    }
    partStack = partStack.filter(_._2 > 0) // only keep items and sequences with bytes left to subtract
    delimits.flatMap { // call events, any items will be inserted in stream
      case d: DicomSequenceDelimitation => onSequenceEnd(d)
      case d: DicomSequenceItemDelimitation => onSequenceItemEnd(d)
    }
  }

  def subtractAndEmit[A <: DicomPart](part: A, handle: A => List[DicomPart]): List[DicomPart] = {
    partStack = subtractLength(part)
    handle(part) ::: maybeDelimit()
  }

  abstract override def onSequenceStart(part: DicomSequence): List[DicomPart] =
    if (part.hasLength) {
      partStack = (part, part.length) +: subtractLength(part)
      super.onSequenceStart(part) ::: maybeDelimit()
    } else subtractAndEmit(part, super.onSequenceStart)

  abstract override def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] =
    if (part.hasLength) {
      partStack = (part, part.length) +: subtractLength(part)
      super.onSequenceItemStart(part) ::: maybeDelimit()
    } else subtractAndEmit(part, super.onSequenceItemStart)

  abstract override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] =
    subtractAndEmit(part, (p: DicomSequenceDelimitation) =>
      super.onSequenceEnd(p).filterNot(_ == DicomSequenceDelimitationMarker))

  abstract override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] =
    subtractAndEmit(part, (p: DicomSequenceItemDelimitation) =>
      super.onSequenceItemEnd(p).filterNot(_.isInstanceOf[DicomSequenceItemDelimitationMarker]))

  abstract override def onHeader(part: DicomHeader): List[DicomPart] = subtractAndEmit(part, super.onHeader)
  abstract override def onValueChunk(part: DicomValueChunk): List[DicomPart] = subtractAndEmit(part, super.onValueChunk)
  abstract override def onFragmentsStart(part: DicomFragments): List[DicomPart] = subtractAndEmit(part, super.onFragmentsStart)
  abstract override def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart] = subtractAndEmit(part, super.onFragmentsItemStart)
  abstract override def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = subtractAndEmit(part, super.onFragmentsEnd)
}

/**
  * This mixin keeps track of the current tag path as the stream moves through attributes, sequences and fragments. The
  * tag path state is updated in the event callbacks. This means that implementations of events using this mixin must
  * remember to call the corresponding super method for the tag path to update.
  *
  * This implementation contains state (the current tag path). The corresponding flow must therefore be created anew for
  * each stream.
  */
trait TagPathTracking extends DicomFlow with GuaranteedDelimitationEvents with GuaranteedValueEvent {

  protected var tagPath: Option[TagPath] = None
  protected var inFragments = false

  abstract override def onHeader(part: DicomHeader): List[DicomPart] = {
    tagPath = tagPath.map {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected attribute ${tagToString(part.tag)} in tag path $t.")
      case s: TagPathSequence => s.thenTag(part.tag)
    }.orElse(Some(TagPath.fromTag(part.tag)))
    super.onHeader(part)
  }

  abstract override def onValueChunk(part: DicomValueChunk): List[DicomPart] = {
    if (part.last && !inFragments)
      tagPath = tagPath.flatMap(_.previous)
    super.onValueChunk(part)
  }

  abstract override def onSequenceStart(part: DicomSequence): List[DicomPart] = {
    tagPath = tagPath.map {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected sequence ${tagToString(part.tag)} in tag path $t.")
      case s: TagPathSequence => s.thenSequence(part.tag)
    }.orElse(Some(TagPath.fromSequence(part.tag)))
    super.onSequenceStart(part)
  }

  abstract override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = {
    tagPath = tagPath.flatMap {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected end of sequence in tag path $t.")
      case s: TagPathSequence => s.previous
    }
    super.onSequenceEnd(part)
  }

  abstract override def onFragmentsStart(part: DicomFragments): List[DicomPart] = {
    inFragments = true
    tagPath = tagPath.map {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected fragments ${tagToString(part.tag)} in tag path $t.")
      case s: TagPathSequence => s.thenTag(part.tag)
    }.orElse(Some(TagPath.fromTag(part.tag)))
    super.onFragmentsStart(part)
  }

  abstract override def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = {
    inFragments = false
    tagPath = tagPath.flatMap {
      case t: TagPathTag => t.previous.flatMap(_.previous)
      case s: TagPathSequence => throw new DicomStreamException(s"Unexpected end of fragments in tag path $s.")
    }
    super.onFragmentsEnd(part)
  }

  abstract override def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = {
    tagPath = tagPath.map {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected item with index ${part.index} in tag path $t.")
      case s: TagPathSequence => s.previous.map(_.thenSequence(s.tag, part.index)).getOrElse(TagPath.fromSequence(s.tag, part.index))
    }
    super.onSequenceItemStart(part)
  }

  abstract override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = {
    tagPath = tagPath.flatMap {
      case t: TagPathTag => throw new DicomStreamException(s"Unexpected end of fragments in tag path $t.")
      case s: TagPathSequence => s.previous.map(_.thenSequence(s.tag)).orElse(Some(TagPath.fromSequence(s.tag)))
    }
    super.onSequenceItemEnd(part)
  }
}

/**
  * Provides factory methods for creating `DicomFlow`s
  */
object DicomFlowFactory {

  def create(flow: DicomFlow): Flow[DicomPart, DicomPart, NotUsed] = flow.baseFlow.mapConcat(flow.handlePart)

}
