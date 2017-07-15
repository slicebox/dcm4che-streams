package se.nimsa.dcm4che.streams

import org.dcm4che3.data.Tag
import org.scalatest.{FlatSpec, Matchers}

class TagPathTest extends FlatSpec with Matchers {

  "The tag path depth" should "be 0 when pointint to a tag in the root dataset" in {
    val path = TagPath.fromTag(Tag.PatientID)
    path.depth shouldBe 0
  }

  it should "be 3 when pointing to a tag in three levels of sequences" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence).thenSequence(Tag.DerivationCodeSequence, 3).thenSequence(Tag.DerivationCodeSequence).thenTag(Tag.PatientID)
    path.depth shouldBe 3
  }

  "A tag path" should "have a legible string representation" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence).thenSequence(Tag.DerivationCodeSequence, 3).thenSequence(Tag.DerivationCodeSequence).thenTag(Tag.PatientID)
    path.toString shouldBe "(0008,9215)[*].(0008,9215)[3].(0008,9215)[*].(0010,0020)"
  }

  it should "be root when pointing to root dataset" in {
    val path = TagPath.fromTag(Tag.PatientID)
    path.isRoot shouldBe true
  }

  it should "not be root when pointing to a tag in a sequence" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence).thenTag(Tag.PatientID)
    path.isRoot shouldBe false
  }

  it should "be a sequence when pointing to a sequence" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence)
    path.isSequence shouldBe true
  }

  it should "not be a sequence when pointing to a non-sequence tag" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence).thenTag(Tag.PatientID)
    path.isSequence shouldBe false
  }

  "A list representation of tag path tags" should "contain a single entry for a tag in the root dataset" in {
    val path = TagPath.fromTag(Tag.PatientID)
    path.toList shouldBe path :: Nil
  }

  it should "contain four entries for a path of depth 3" in {
    val path = TagPath.fromSequence(Tag.DerivationCodeSequence).thenSequence(Tag.DerivationCodeSequence, 3).thenSequence(Tag.DerivationCodeSequence).thenTag(Tag.PatientID)
    path.toList shouldBe path.previous.get.previous.get.previous.get :: path.previous.get.previous.get :: path.previous.get :: path :: Nil
  }

  "Comparing two tag paths" should "return false when comparing a tag path to itself" in {
    val path = TagPath.fromTag(1)
    path < path shouldBe false
  }

  it should "return false when comparing two equivalent tag paths" in {
    val aPath = TagPath.fromSequence(1).thenSequence(1, 3).thenSequence(1).thenTag(2)
    val bPath = TagPath.fromSequence(1).thenSequence(1, 3).thenSequence(1).thenTag(2)
    aPath < bPath shouldBe false
  }

  it should "sort them by tag number for depth 0 paths" in {
    val aPath = TagPath.fromTag(1)
    val bPath = TagPath.fromTag(2)
    aPath < bPath shouldBe true
  }

  it should "sort them by tag number for depth 1 paths" in {
    val aPath = TagPath.fromSequence(1).thenTag(2)
    val bPath = TagPath.fromSequence(1).thenTag(1)
    aPath < bPath shouldBe false
  }

  it should "sort by earliest difference in deep paths" in {
    val aPath = TagPath.fromSequence(1).thenSequence(1).thenTag(1)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenTag(1)
    aPath < bPath shouldBe true
  }

  it should "sort longer lists after shorter lists when otherwise equivalent" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3)
    aPath < bPath shouldBe false
  }

  it should "sort by item number in otherwise equivalent paths" in {
    val aPath = TagPath.fromSequence(1).thenSequence(1, 2).thenSequence(1).thenTag(2)
    val bPath = TagPath.fromSequence(1).thenSequence(1, 3).thenSequence(1).thenTag(2)
    aPath < bPath shouldBe true
  }

  it should "define wildcard items as both less than and greater than specific items" in {
    val aPath = TagPath.fromSequence(1).thenSequence(1).thenSequence(1).thenTag(2)
    val bPath = TagPath.fromSequence(1).thenSequence(1, 3).thenSequence(1).thenTag(2)
    aPath < bPath shouldBe true
    bPath < aPath shouldBe true
  }

  "Two tag paths" should "be equal if they point to the same path" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    aPath shouldBe bPath
  }

  it should "not be equal if item indices do not match" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2, 1).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2, 2).thenSequence(3).thenTag(4)
    aPath should not be bPath
  }

  it should "not be equal if they point to different tags" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(5)
    aPath should not be bPath
  }

  it should "not be equal if they have different depths" in {
    val aPath = TagPath.fromSequence(1).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    aPath should not be bPath
  }

  it should "not be equal if one points to all indices of a sequence and the other points to a specific index" in {
    val aPath = TagPath.fromSequence(1)
    val bPath = TagPath.fromSequence(1, 1)
    aPath should not be bPath
  }

  "The contains test" should "return true for equal paths" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    aPath.contains(bPath) shouldBe true
  }

  it should "return false when contained path is longer that enclosing path" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    aPath.contains(bPath) shouldBe false
  }

  it should "return true when paths involving item indices are equal" in {
    val aPath = TagPath.fromSequence(1, 4).thenSequence(2).thenSequence(3, 2).thenTag(4)
    val bPath = TagPath.fromSequence(1, 4).thenSequence(2).thenSequence(3, 2).thenTag(4)
    aPath.contains(bPath) shouldBe true
  }

  it should "return true when a path with wildcards contain a path with item indices" in {
    val aPath = TagPath.fromSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(2, 4).thenSequence(3, 66).thenTag(4)
    aPath.contains(bPath) shouldBe true
  }

  it should "return false when a path with wildcards contain a path with item indices" in {
    val aPath = TagPath.fromSequence(2, 4).thenSequence(3, 66).thenTag(4)
    val bPath = TagPath.fromSequence(2).thenSequence(3).thenTag(4)
    aPath.contains(bPath) shouldBe false
  }

  it should "return true when contained path is subset of enclosing path" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2).thenSequence(3)
    aPath.contains(bPath) shouldBe true
  }

  it should "return true when contained path is subset of enclosing path with item indices" in {
    val aPath = TagPath.fromSequence(1).thenSequence(2, 4).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1, 3).thenSequence(2, 4).thenSequence(3)
    aPath.contains(bPath) shouldBe true
  }

  it should "return false when contained path has wildcards enclosing path does not" in {
    val aPath = TagPath.fromSequence(1, 3).thenSequence(2, 4).thenSequence(3).thenTag(4)
    val bPath = TagPath.fromSequence(1).thenSequence(2, 4).thenSequence(3)
    aPath.contains(bPath) shouldBe false
  }
}
