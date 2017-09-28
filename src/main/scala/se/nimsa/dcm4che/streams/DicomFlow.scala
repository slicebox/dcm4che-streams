package se.nimsa.dcm4che.streams

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import se.nimsa.dcm4che.streams.DicomParts._
import se.nimsa.dcm4che.streams.TagPath.{TagPathSequence, TagPathTag}

abstract class DicomFlow {

  def onPreamble(part: DicomPreamble): List[DicomPart] = onPart(part)
  def onHeader(part: DicomHeader): List[DicomPart] = onPart(part)
  def onValueChunk(part: DicomValueChunk): List[DicomPart] = onPart(part)
  def onSequenceStart(part: DicomSequence): List[DicomPart] = onPart(part)
  def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] = onPart(part)
  def onFragmentsStart(part: DicomFragments): List[DicomPart] = onPart(part)
  def onFragmentsEnd(part: DicomFragmentsDelimitation): List[DicomPart] = onPart(part)
  def onDeflatedChunk(part: DicomDeflatedChunk): List[DicomPart] = onPart(part)
  def onUnknownPart(part: DicomUnknownPart): List[DicomPart] = onPart(part)
  def onPart(part: DicomPart): List[DicomPart] = part :: Nil

  private[streams] def baseFlow: Flow[DicomPart, DicomPart, NotUsed] = Flow[DicomPart]

  private[streams] def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case part: DicomPreamble => onPreamble(part)
    case part: DicomHeader => onHeader(part)
    case part: DicomValueChunk => onValueChunk(part)
    case part: DicomSequence => onSequenceStart(part)
    case part: DicomSequenceDelimitation => onSequenceEnd(part)
    case part: DicomFragments => onFragmentsStart(part)
    case part: DicomFragmentsDelimitation => onFragmentsEnd(part)
    case part: DicomDeflatedChunk => onDeflatedChunk(part)
    case part: DicomUnknownPart => onUnknownPart(part)
    case part => onPart(part)
  }
}

trait DicomEndHandling extends DicomFlow {
  def onDicomEnd(): List[DicomPart] = Nil

  override private[streams] def baseFlow: Flow[DicomPart, DicomPart, NotUsed] =
    super.baseFlow.concat(Source.single(DicomEndMarker))

  override private[streams] def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case DicomEndMarker => onDicomEnd()
    case part => super.handlePart(part)
  }
}

trait ValueAlways extends DicomFlow {

  override def onHeader(part: DicomHeader): List[DicomPart] = {
    if (part.length == 0)
      part :: DicomValueChunk(part.bigEndian, ByteString.empty, last = true) :: Nil
    else
      super.onHeader(part)
  }

  override def onValueChunk(part: DicomValueChunk): List[DicomPart] =
    if (part.bytes.isEmpty) Nil else super.onValueChunk(part)
}

trait ItemHandling extends DicomFlow {
  def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = onPart(part)
  def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = onPart(part)
  def onFragmentsItemStart(part: DicomFragmentsItem): List[DicomPart] = onPart(part)


  override private[streams] def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case part: DicomSequenceItem => onSequenceItemStart(part)
    case part: DicomSequenceItemDelimitation => onSequenceItemEnd(part)
    case part: DicomFragmentsItem => onFragmentsItemStart(part)
    case part => super.handlePart(part)
  }
}

trait AnyItemHandling extends DicomFlow {
  def onItemStart(part: DicomItem): List[DicomPart] = onPart(part)
  def onItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] = onPart(part)

  override private[streams] def handlePart(dicomPart: DicomPart): List[DicomPart] = dicomPart match {
    case part: DicomSequenceItem => onItemStart(part)
    case part: DicomSequenceItemDelimitation => onItemEnd(part)
    case part: DicomFragmentsItem => onItemStart(part)
    case part => super.handlePart(part)
  }
}

trait TagPathTracking extends DicomFlow with ItemHandling {

  protected var tagPath: Option[_ <: TagPath] = None

  private val maybeUpdatePath: DicomPart => Unit = {
    case part: DicomHeader =>
      tagPath = tagPath.map {
        case t: TagPathTag => t.previous.map(_.thenTag(part.tag)).getOrElse(TagPath.fromTag(part.tag))
        case s: TagPathSequence => s.thenTag(part.tag)
      }.orElse(Some(TagPath.fromTag(part.tag)))
    case part: DicomValueChunk if part.last =>
      tagPath = tagPath.flatMap(_.previous)
    case part: DicomSequence =>
      tagPath = tagPath.map {
        case t: TagPathTag => t.previous.map(_.thenSequence(part.tag)).getOrElse(TagPath.fromSequence(part.tag))
        case s: TagPathSequence => s.thenSequence(part.tag)
      }.orElse(Some(TagPath.fromSequence(part.tag)))
    case _: DicomSequenceDelimitation =>
      tagPath = tagPath.flatMap {
        case t: TagPathTag => t.previous.flatMap(_.previous)
        case s: TagPathSequence => s.previous
      }
    case part: DicomFragments =>
      tagPath = tagPath.map {
        case t: TagPathTag => t.previous.map(_.thenTag(part.tag)).getOrElse(TagPath.fromTag(part.tag))
        case s: TagPathSequence => s.thenTag(part.tag)
      }.orElse(Some(TagPath.fromTag(part.tag)))
    case _: DicomFragmentsDelimitation =>
      tagPath = tagPath.flatMap {
        case t: TagPathTag => t.previous.flatMap(_.previous)
        case s: TagPathSequence => s.previous
      }
    case part: DicomSequenceItem =>
      tagPath = tagPath.map {
        case t: TagPathTag => t.previous
          .map(s => s.previous.map(_.thenSequence(s.tag, part.index)).getOrElse(TagPath.fromSequence(s.tag, part.index)))
          .getOrElse(t)
        case s: TagPathSequence => s.previous
          .map(_.thenSequence(s.tag, part.index))
          .getOrElse(TagPath.fromSequence(s.tag, part.index))
      }
    case _: DicomSequenceItemDelimitation =>
      tagPath = tagPath.flatMap {
        case t: TagPathTag => t.previous.map(s => s.previous.map(_.thenSequence(s.tag)).getOrElse(TagPath.fromSequence(s.tag)))
        case s: TagPathSequence => s.previous.map(_.thenSequence(s.tag)).orElse(Some(TagPath.fromSequence(s.tag)))
      }
    case _ =>
  }

  override private[streams] def handlePart(dicomPart: DicomPart): List[DicomPart] = {
    maybeUpdatePath(dicomPart)
    super.handlePart(dicomPart)
  }
}

trait DelimitationAlways extends DicomFlow with ItemHandling {
  var partStack: List[LengthPart] = Nil

  def subtractLength(seqsAndItems: List[DicomPart], part: DicomPart): List[LengthPart] =
    seqsAndItems.map {
      case item: DicomSequenceItem => item.copy(length = item.length - part.bytes.length)
      case sequence: DicomSequence => sequence.copy(length = sequence.length - part.bytes.length)
    }

  def maybeDelimit(): List[DicomPart] = {
    val delimits = partStack
      .filter(_.length <= 0) // find items and sequences that have ended
      .map { // create delimiters for those
      case item: DicomSequenceItem =>
        DicomSequenceItemDelimitation(item.index, item.bigEndian, ByteString.empty)
      case sequence: DicomSequence =>
        DicomSequenceDelimitation(sequence.bigEndian, ByteString.empty)
    }
    partStack = partStack.filter(_.length > 0) // only keep items and sequences with bytes left to subtract
    delimits // these will be returned and inserted in stream
  }

  override def onSequenceStart(part: DicomSequence): List[DicomPart] = {
    if (part.hasLength) {
      partStack = part +: subtractLength(partStack, part)
      part :: maybeDelimit()
    } else onPart(part)
  }

  override def onSequenceItemStart(part: DicomSequenceItem): List[DicomPart] = {
    if (part.hasLength) {
      partStack = part +: subtractLength(partStack, part)
      part :: maybeDelimit()
    } else onPart(part)
  }

  override def onSequenceEnd(part: DicomSequenceDelimitation): List[DicomPart] =
    if (part.bytes.isEmpty) Nil else super.onSequenceEnd(part)

  override def onSequenceItemEnd(part: DicomSequenceItemDelimitation): List[DicomPart] =
    if (part.bytes.isEmpty) Nil else super.onSequenceItemEnd(part)

  override def onPart(part: DicomPart): List[DicomPart] = {
    partStack = subtractLength(partStack, part)
    part :: maybeDelimit()
  }
}

object DicomFlowFactory {

  def create(flow: DicomFlow): Flow[DicomPart, DicomPart, NotUsed] = flow.baseFlow.mapConcat(flow.handlePart)

}
