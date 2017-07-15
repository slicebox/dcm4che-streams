package se.nimsa.dcm4che.streams

import scala.annotation.tailrec

sealed trait TagPath {

  import TagPath._

  val tag: Int
  val previous: Option[TagPathSequence]

  /**
    * `true` if this tag path points to the root dataset, at depth 0
    */
  val isRoot = previous.isEmpty

  /**
    * `true` if this tag path ends with a sequence
    */
  val isSequence = this.isInstanceOf[TagPathSequence]

  def toList: List[TagPath] = {
    @tailrec
    def toList(path: TagPath, tail: List[TagPath]): List[TagPath] =
      if (path.isRoot) path :: tail else toList(path.previous.get, path :: tail)
    toList(path = this, tail = Nil)
  }

  /**
    * Test if this tag path is less than the input path, comparing their parts pairwise according to the following rules
    *  (1) a is less than b if the a's tag number is less b's tag number
    *  (2) a is less than b if tag numbers are equal and a's item index is less than b's item index
    *  (3) a is less than b if tag numbers are equal, a points to an item index and b points to all indices (wildcard)
    *
    * @param that the tag path to compare with
    * @return `true` if this tag path is less than the input path
    */
  def <(that: TagPath): Boolean = {
    val thisList = this.toList
    val thatList = that.toList
    thisList.zip(thatList)
      .find {
        case (thisPath: TagPathSequence, thatPath: TagPathSequence) if thisPath.tag == thatPath.tag =>
          thisPath.item match {
            case Some(thisIndex) =>
              thatPath.item match {
                case Some(thatIndex) => thisIndex < thatIndex
                case None => true
              }
            case None =>
              thatPath.item match {
                case Some(_) => false
                case None => false
              }
          }
        case (thisPath, thatPath) =>
          thisPath.tag < thatPath.tag
      }
      .map(_ => true)
      .getOrElse(thisList.length < thatList.length)
  }

  /**
    * @param that tag path to test
    * @return `true` if the input tag path is part of this tag path, `false` otherwise.
    */
  def contains(that: TagPath): Boolean = {
    val thisList = this.toList
    val thatList = that.toList

    if (thisList.length >= thatList.length)
      thisList.zip(thatList).forall {
        case (thisSeq: TagPathSequence, thatSeq: TagPathSequence) =>
            thisSeq.tag == thatSeq.tag && (thisSeq.item.isEmpty || thatSeq.item.contains(thisSeq.item.get))
        case (thisTag: TagPathTag, thatTag: TagPathTag) =>
            thisTag.tag == thatTag.tag
        case _ =>
            false
      }
    else
      false
  }

  /**
    * Depth of this tag path. A tag path that points to a tag in a sequence in a sequence has depth 2. A tag path that
    * points to a tag in the root dataset has depth 0.
    *
    * @return the depth of this tag path, counting from 0
    */
  def depth: Int = {
    @tailrec
    def depth(path: TagPath, d: Int): Int = if (path.isRoot) d else depth(path.previous.get, d + 1)
    depth(this, 0)
  }

  override def toString = {
    @tailrec
    def toTagPathString(path: TagPath, tail: String): String = {
      val itemIndexSuffix = if (path.isSequence) s"[${path.asInstanceOf[TagPathSequence].item.map(_.toString).getOrElse("*")}]" else ""
      val head = DicomParsing.tagToString(path.tag) + itemIndexSuffix
      val part = head + tail
      if (path.isRoot) part else toTagPathString(path.previous.get, "." + part)
    }
    toTagPathString(path = this, tail = "")
  }
}

object TagPath {

  /**
    * A tag path that points to a non-sequence tag
    *
    * @param tag      the tag number
    * @param previous a link to the part of this tag part to the left of this tag
    */
  class TagPathTag private[TagPath](val tag: Int, val previous: Option[TagPathSequence]) extends TagPath {

    def canEqual(other: Any): Boolean = other.isInstanceOf[TagPathTag]

    override def equals(other: Any): Boolean = other match {
      case that: TagPathTag => (that canEqual this) &&
        tag == that.tag &&
        (previous.isEmpty && that.previous.isEmpty || previous.flatMap(p => that.previous.map(tp => p == tp)).getOrElse(false))
      case _ => false
    }

    override def hashCode(): Int = 31 * (31 * previous.map(_.hashCode()).getOrElse(0) + tag.hashCode())
  }

  /**
    * A tag path that points to a sequence
    *
    * @param tag      the sequence tag number
    * @param item     if defined, this defines the item index in the sequence. If not defined, this path points to all items in sequence
    * @param previous a link to the part of this tag part to the left of this tag
    */
  class TagPathSequence private[TagPath](val tag: Int, val item: Option[Int], val previous: Option[TagPathSequence]) extends TagPath {

    /**
      * Path to a specific tag
      *
      * @param tag tag number
      * @return the tag path
      */
    def thenTag(tag: Int) = new TagPathTag(tag, Some(this))

    /**
      * Path to all items in a sequence
      *
      * @param tag tag number
      * @return the tag path
      */
    def thenSequence(tag: Int) = new TagPathSequence(tag, None, Some(this))

    /**
      * Path to a specific item within a sequence
      *
      * @param tag  tag number
      * @param item item index
      * @return the tag path
      */
    def thenSequence(tag: Int, item: Int) = new TagPathSequence(tag, Some(item), Some(this))

    def canEqual(other: Any): Boolean = other.isInstanceOf[TagPathSequence]

    override def equals(other: Any): Boolean = other match {
      case that: TagPathSequence => (that canEqual this) &&
        tag == that.tag &&
        (item.isEmpty && that.item.isEmpty || item.flatMap(i => that.item.map(ti => i == ti)).getOrElse(false)) &&
        (previous.isEmpty && that.previous.isEmpty || previous.flatMap(p => that.previous.map(tp => p == tp)).getOrElse(false))
      case _ => false
    }

    override def hashCode(): Int = 31 * (31 * (31 * previous.map(_.hashCode()).getOrElse(0) + item.map(_.hashCode()).getOrElse(0)) + tag.hashCode())
  }

  /**
    * Create a path to a specific tag
    *
    * @param tag tag number
    * @return the tag path
    */
  def fromTag(tag: Int) = new TagPathTag(tag, None)

  /**
    * Create a path to all items in a sequence
    *
    * @param tag tag number
    * @return the tag path
    */
  def fromSequence(tag: Int) = new TagPathSequence(tag, None, None)

  /**
    * Create a path to a specific item within a sequence
    *
    * @param tag  tag number
    * @param item item index
    * @return the tag path
    */
  def fromSequence(tag: Int, item: Int) = new TagPathSequence(tag, Some(item), None)
}
