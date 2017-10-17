package se.nimsa.dcm4che.streams

import scala.annotation.tailrec

sealed trait TagPath {

  import TagPath._

  val tag: Int
  val previous: Option[TagPathSequence]

  /**
    * `true` if this tag path points to the root dataset, at depth 0
    */
  lazy val isRoot: Boolean = previous.isEmpty

  /**
    * `true` if this tag path ends with a (non-sequence) tag
    */
  lazy val isTag: Boolean = this.isInstanceOf[TagPathTag]

  /**
    * `true` if this tag path ends with a sequence
    */
  lazy val isSequence: Boolean = classOf[TagPathSequence].isAssignableFrom(getClass)

  /**
    * `true` if this tag path ends with a sequence pointing to any and all of its items
    */
  lazy val isSequenceAny: Boolean = this.isInstanceOf[TagPathSequenceAny]

  /**
    * `true` if this tag path ends with a sequence pointing to a specific item
    */
  lazy val isSequenceItem: Boolean = this.isInstanceOf[TagPathSequenceItem]

  def toList: List[TagPath] = {
    @tailrec
    def toList(path: TagPath, tail: List[TagPath]): List[TagPath] =
      if (path.isRoot) path :: tail else toList(path.previous.get, path :: tail)
    toList(path = this, tail = Nil)
  }

  /**
    * Test if this tag path is less than the input path, comparing their parts pairwise according to the following rules
    * (1) a is less than b if the a's tag number is less b's tag number
    * (2) a is less than b if tag numbers are equal and a's item index is less than b's item index
    * (3) a is less than b if tag numbers are equal, a points to an item index and b points to all indices (wildcard)
    * otherwise a is greater than or equal b
    *
    * @param that the tag path to compare with
    * @return `true` if this tag path is less than the input path
    */
  def <(that: TagPath): Boolean = {
    val thisList = this.toList
    val thatList = that.toList
    thisList.zip(thatList)
      .find {
        case (thisPath, thatPath) if thisPath.tag != thatPath.tag =>
          intToUnsignedLong(thisPath.tag) < intToUnsignedLong(thatPath.tag)
        case (thisPath: TagPathSequenceItem, thatPath: TagPathSequenceItem) => thisPath.item < thatPath.item
        case (_: TagPathSequenceItem, _: TagPathSequenceAny) => true
        case (_, _) => false
      }
      .map(_ => true)
      .getOrElse(thisList.length < thatList.length)
  }

  /**
    * @param that tag path to test
    * @return `true` if the input tag path is equal to this tag path. TagPath nodes are compared pairwise from the end
    *         towards the start of the paths. Node types, tag numbers as well as item indices where applicable must be
    *         equal. Paths of different lengths cannot be equal.
    * @example (0010,0010) == (0010,0010)
    * @example (0010,0010) != (0010,0020)
    * @example (0010,0010) != (0008,9215)[1].(0010,0010)
    * @example (0008,9215)[*].(0010,0010) != (0008,9215)[1].(0010,0010)
    * @example (0008,9215)[3].(0010,0010) == (0008,9215)[3].(0010,0010)
    */
  override def equals(that: Any): Boolean = {
    def tagEquals(t1: TagPath, t2: TagPath) = t1.tag == t2.tag &&
      (t1.previous.isEmpty && t2.previous.isEmpty || t1.previous.flatMap(p => t2.previous.map(p.equals)).getOrElse(false))
    (this, that) match {
      case (t1: TagPathTag, t2: TagPathTag) => tagEquals(t1, t2)
      case (s1: TagPathSequenceItem, s2: TagPathSequenceItem) => s1.item == s2.item && tagEquals(s1, s2)
      case (s1: TagPathSequenceAny, s2: TagPathSequenceAny) => tagEquals(s1, s2)
      case _ => false
    }
  }

  /**
    * @param tag tag number
    * @return `true` if this tag path contains the input tag number
    * @example (0008,9215)[*].(0010,0010) contains 0x00089215
    * @example (0008,9215)[*].(0010,0010) contains 0x00100010
    * @example (0008,9215)[*].(0010,0010) does not contain 0x00100020
    */
  def contains(tag: Int): Boolean = toList.map(_.tag).contains(tag)

  /**
    * A super-path of another path is a path of equal length with the same sequence of tag numbers. Differences are in the
    * specification of items. A path with a less restrictive specification of items (wildcard/all
    * items instead of item index) is said to be a super-path of the more general path.
    *
    * The input path is a super-path of this path if and only if this path is a sub-path of the input path.
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a super-path of this path
    * @example (0008,9215)[1].(0010,0010) is a super-path of (0008,9215)[1].(0010,0010)
    * @example (0008,9215)[*].(0010,0010) is a super-path of (0008,9215)[1].(0010,0010)
    * @example (0008,9215)[1].(0010,0010) is not a super-path of (0008,9215)[*].(0010,0010) (it is a sub-path)
    */
  def hasSuperPath(that: TagPath): Boolean = {
    def tagEquals(t1: TagPath, t2: TagPath) = t1.tag == t2.tag &&
      (t1.previous.isEmpty && t2.previous.isEmpty || t1.previous.flatMap(p => t2.previous.map(p.hasSuperPath)).getOrElse(false))
    (this, that) match {
      case (t1: TagPathTag, t2: TagPathTag) => tagEquals(t1, t2)
      case (s1: TagPathSequenceItem, s2: TagPathSequenceItem) => s1.item == s2.item && tagEquals(s1, s2)
      case (s1: TagPathSequenceItem, s2: TagPathSequenceAny) => tagEquals(s1, s2)
      case (s1: TagPathSequenceAny, s2: TagPathSequenceAny) => tagEquals(s1, s2)
      case _ => false
    }
  }

  /**
    * A sub-path of another path is a path of equal length with the same sequence of tag numbers. Differences are in the
    * specification of items. A path with a more restrictive specification of items (item index instead of wildcard/all
    * items) is said to be a sub-path of the more general path.
    *
    * The input path is a sub-path of this path if and only if this path is a super-path of the input path.
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a sub-path of this path
    * @example (0008,9215)[1].(0010,0010) is a sub-path of (0008,9215)[1].(0010,0010)
    * @example (0008,9215)[1].(0010,0010) is a sub-path of (0008,9215)[*].(0010,0010)
    * @example (0008,9215)[*].(0010,0010) is not a sub-path of (0008,9215)[1].(0010,0010) (it is a super-path)
    */
  def hasSubPath(that: TagPath): Boolean = that.hasSuperPath(this)

  private[TagPath] def startsWith(that: TagPath,
                                  f1: (TagPathSequenceItem, TagPathSequenceAny) => Boolean,
                                  f2: (TagPathSequenceAny, TagPathSequenceItem) => Boolean): Boolean = {
    if (this.depth >= that.depth)
      this.toList.zip(that.toList).forall {
        case (thisSeq: TagPathSequenceAny, thatSeq: TagPathSequenceAny) => thisSeq.tag == thatSeq.tag
        case (thisSeq: TagPathSequenceItem, thatSeq: TagPathSequenceAny) => f1(thisSeq, thatSeq)
        case (thisSeq: TagPathSequenceAny, thatSeq: TagPathSequenceItem) => f2(thisSeq, thatSeq)
        case (thisSeq: TagPathSequenceItem, thatSeq: TagPathSequenceItem) => thisSeq.tag == thatSeq.tag && thisSeq.item == thatSeq.item
        case (thisTag: TagPathTag, thatTag: TagPathTag) => thisTag.tag == thatTag.tag
        case _ => false
      }
    else
      false
  }
  /**
    * Tests if the n first nodes of this path is equal (see definition of `equels`) to the input path of depth n
    *
    * @param that tag path to test
    * @return `true` if the input tag path is equal to the the start of this tag path
    */
  def startsWith(that: TagPath): Boolean = startsWith(that, (_, _) => false, (_, _) => false)

  /**
    * Tests if the input path of depth n is a sub-path (see definition of `hasSubPath`) of the n first nodes of this path
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a sub-path of the start of this tag path
    */
  def startsWithSubPath(that: TagPath): Boolean = startsWith(that, (_, _) => false, (s1, s2) => s1.tag == s2.tag)

  /**
    * Tests if the input path of depth n is a super-path (see definition of `hasSuperPath`) of the n first nodes of this path
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a super-path of the start of this tag path
    */
  def startsWithSuperPath(that: TagPath): Boolean = startsWith(that, (s1, s2) => s1.tag == s2.tag, (_, _) => false)

  private[TagPath] def endsWith(that: TagPath,
                                f1: (TagPathSequenceItem, TagPathSequenceAny) => Boolean,
                                f2: (TagPathSequenceAny, TagPathSequenceItem) => Boolean): Boolean = {
    val matches = (this, that) match {
      case (thisSeq: TagPathSequenceAny, thatSeq: TagPathSequenceAny) => thisSeq.tag == thatSeq.tag
      case (thisSeq: TagPathSequenceItem, thatSeq: TagPathSequenceAny) => f1(thisSeq, thatSeq)
      case (thisSeq: TagPathSequenceAny, thatSeq: TagPathSequenceItem) => f2(thisSeq, thatSeq)
      case (thisSeq: TagPathSequenceItem, thatSeq: TagPathSequenceItem) => thisSeq.tag == thatSeq.tag
      case (thisTag: TagPathTag, thatTag: TagPathTag) => thisTag.tag == thatTag.tag
      case _ => false
    }
    (this.previous, that.previous) match {
      case _ if !matches => false
      case (None, None) => true
      case (Some(_), None) => true
      case (None, Some(_)) => false
      case (Some(thisPrev), Some(thatPrev)) => thisPrev.endsWith(thatPrev, f1, f2)
    }
  }

  /**
    * Tests if the n last nodes of this path is equal (see definition of `equels`) to the input path of depth n
    *
    * @param that tag path to test
    * @return `true` if the input tag path is equal to the end of this tag path
    */
  def endsWith(that: TagPath): Boolean = endsWith(that, (_, _) => false, (_, _) => false)

  /**
    * Tests if the input path of depth n is a sub-path (see definition of `hasSubPath`) of the n last nodes of this path
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a sub-path of the end of this tag path
    */
  def endsWithSubPath(that: TagPath): Boolean = endsWith(that, (_, _) => false, (s1, s2) => s1.tag == s2.tag)

  /**
    * Tests if the input path of depth n is a super-path (see definition of `hasSuperPath`) of the n last nodes of this path
    *
    * @param that tag path to test
    * @return `true` if the input tag path is a super-path of the end of this tag path
    */
  def endsWithSuperPath(that: TagPath): Boolean = endsWith(that, (s1, s2) => s1.tag == s2.tag, (_, _) => false)

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

  override def toString: String = {
    @tailrec
    def toTagPathString(path: TagPath, tail: String): String = {
      val itemIndexSuffix = path match {
        case _: TagPathSequenceAny => "[*]"
        case s: TagPathSequenceItem => s"[${s.item}]"
        case _ => ""
      }
      val head = tagToString(path.tag) + itemIndexSuffix
      val part = head + tail
      if (path.isRoot) part else toTagPathString(path.previous.get, "." + part)
    }
    toTagPathString(path = this, tail = "")
  }

  override def hashCode(): Int = this match {
    case s: TagPathSequenceItem => 31 * (31 * (31 * previous.map(_.hashCode()).getOrElse(0) + tag.hashCode()) * s.item.hashCode())
    case _ => 31 * (31 * previous.map(_.hashCode()).getOrElse(0) + tag.hashCode())
  }
}

object TagPath {

  /**
    * A tag path that points to a non-sequence tag
    *
    * @param tag      the tag number
    * @param previous a link to the part of this tag part to the left of this tag
    */
  class TagPathTag private[TagPath](val tag: Int, val previous: Option[TagPathSequence]) extends TagPath

  /**
    * A tag path that points to a sequence
    */
  trait TagPathSequence extends TagPath {

    val tag: Int
    val previous: Option[_ <: TagPathSequence]

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
    def thenSequence(tag: Int) = new TagPathSequenceAny(tag, Some(this))

    /**
      * Path to a specific item within a sequence
      *
      * @param tag  tag number
      * @param item item index
      * @return the tag path
      */
    def thenSequence(tag: Int, item: Int) = new TagPathSequenceItem(tag, item, Some(this))
  }

  /**
    * A tag path that points to all items of a sequence
    *
    * @param tag      the sequence tag number
    * @param previous a link to the part of this tag part to the left of this tag
    */
  class TagPathSequenceAny private[TagPath](val tag: Int, val previous: Option[_ <: TagPathSequence]) extends TagPathSequence

  /**
    * A tag path that points to an item in a sequence
    *
    * @param tag      the sequence tag number
    * @param item     defines the item index in the sequence
    * @param previous a link to the part of this tag part to the left of this tag
    */
  class TagPathSequenceItem private[TagPath](val tag: Int, val item: Int, val previous: Option[_ <: TagPathSequence]) extends TagPathSequence

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
  def fromSequence(tag: Int) = new TagPathSequenceAny(tag, None)

  /**
    * Create a path to a specific item within a sequence
    *
    * @param tag  tag number
    * @param item item index
    * @return the tag path
    */
  def fromSequence(tag: Int, item: Int) = new TagPathSequenceItem(tag, item, None)

  /**
    * Parse the string representation of a tag path into a tag path object.
    *
    * @param s string to parse
    * @return a tag path
    * @throws IllegalArgumentException for malformed input
    */
  def parse(s: String): TagPath = {
    def isSeq(s: String) = s.length > 11
    def parseTagNumber(s: String) = Integer.parseInt(s.substring(1, 5) + s.substring(6, 10), 16)
    def parseIndex(s: String) = if (s.charAt(12) == '*') None else Some(Integer.parseInt(s.substring(12, s.length - 1)))
    def createTag(s: String) = TagPath.fromTag(parseTagNumber(s))
    def createSeq(s: String) = parseIndex(s)
      .map(index => TagPath.fromSequence(parseTagNumber(s), index))
      .getOrElse(TagPath.fromSequence(parseTagNumber(s)))
    def addSeq(s: String, path: TagPathSequence) = parseIndex(s)
      .map(index => path.thenSequence(parseTagNumber(s), index))
      .getOrElse(path.thenSequence(parseTagNumber(s)))
    def addTag(s: String, path: TagPathSequence) = path.thenTag(parseTagNumber(s))

    val tags = if (s.indexOf('.') > 0) s.split("\\.").toList else List(s)
    val seqTags = if (tags.length > 1) tags.init else Nil // list of sequence tags, if any
    val lastTag = tags.last // tag or sequence
    try {
      seqTags.headOption.map(first => seqTags.tail.foldLeft(createSeq(first))((path, tag) => addSeq(tag, path))) match {
        case Some(path) => if (isSeq(lastTag)) addSeq(lastTag, path) else addTag(lastTag, path)
        case None => if (isSeq(lastTag)) createSeq(lastTag) else createTag(lastTag)
      }
    } catch {
      case e: Exception => throw new IllegalArgumentException("Tag path could not be parsed", e)
    }
  }

}
