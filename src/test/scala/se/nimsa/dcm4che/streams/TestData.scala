package se.nimsa.dcm4che.streams

import akka.util.ByteString
import se.nimsa.dicom.data._

object TestData {

  val pad0 = ByteString(0)

  val preamble: ByteString = ByteString.fromArray(new Array[Byte](128)) ++ ByteString("DICM")

  def fmiGroupLength(fmis: ByteString*): ByteString = tagToBytesLE(0x00020000) ++ ByteString("UL") ++ shortToBytesLE(0x0004) ++ intToBytesLE(fmis.map(_.length).sum)

  // File Meta Information Version
  val fmiVersion: ByteString = tagToBytesLE(0x00020001) ++ ByteString('O', 'B', 0x00, 0x00) ++ intToBytesLE(0x00000002) ++ ByteString(0x00, 0x01)
  // (not conforming to standard)
  val fmiVersionImplicitLE: ByteString = tagToBytesLE(0x00020001) ++ intToBytesLE(0x00000002) ++ ByteString(0x00, 0x01)

  val mediaStorageSOPClassUID: ByteString = tagToBytesLE(0x00020002) ++ ByteString("UI") ++ shortToBytesLE(0x001A) ++ ByteString(UID.CTImageStorage) ++ pad0
  val sopClassUID: ByteString = tagToBytesLE(0x00080016) ++ ByteString("UI") ++ shortToBytesLE(0x001A) ++ ByteString(UID.CTImageStorage) ++ pad0
  // (not conforming to standard)
  val mediaStorageSOPClassUIDImplicitLE: ByteString = tagToBytesLE(0x00020002) ++ intToBytesLE(0x0000001A) ++ ByteString(UID.CTImageStorage) ++ pad0
  val mediaStorageSOPInstanceUIDImplicitLE: ByteString = tagToBytesLE(0x00020003) ++ intToBytesLE(0x00000038) ++ ByteString("1.2.276.0.7230010.3.1.4.1536491920.17152.1480884676.735") ++ pad0

  val instanceCreatorUID: ByteString = tagToBytesLE(0x00080014) ++ ByteString("UI") ++ shortToBytesLE(0x0014) ++ ByteString("1.2.840.113619.6.184")

  val mediaStorageSOPInstanceUID: ByteString = tagToBytesLE(0x00020003) ++ ByteString("UI") ++ shortToBytesLE(0x0038) ++ ByteString("1.2.276.0.7230010.3.1.4.1536491920.17152.1480884676.735") ++ pad0

  // Transfer Syntax UIDs
  val tsuidExplicitLE: ByteString = tagToBytesLE(0x00020010) ++ ByteString("UI") ++ shortToBytesLE(0x0014) ++ ByteString(UID.ExplicitVRLittleEndian) ++ pad0
  val tsuidExplicitBE: ByteString = tagToBytesLE(0x00020010) ++ ByteString("UI") ++ shortToBytesLE(0x0014) ++ ByteString(UID.ExplicitVRBigEndianRetired) ++ pad0
  val tsuidImplicitLE: ByteString = tagToBytesLE(0x00020010) ++ ByteString("UI") ++ shortToBytesLE(0x0012) ++ ByteString(UID.ImplicitVRLittleEndian) ++ pad0
  val tsuidDeflatedExplicitLE: ByteString = tagToBytesLE(0x00020010) ++ ByteString("UI") ++ shortToBytesLE(0x0016) ++ ByteString(UID.DeflatedExplicitVRLittleEndian)
  // (not conforming to standard)
  val tsuidExplicitLEImplicitLE: ByteString = tagToBytesLE(0x00020010) ++ intToBytesLE(0x00000014) ++ ByteString(UID.ExplicitVRLittleEndian) ++ pad0

  val patientNameJohnDoe: ByteString = tagToBytesLE(0x00100010) ++ ByteString("PN") ++ shortToBytesLE(0x0008) ++ ByteString("John^Doe")
  val patientNameJohnDoeBE: ByteString = tagToBytesBE(0x00100010) ++ ByteString("PN") ++ shortToBytesBE(0x0008) ++ ByteString("John^Doe")
  val patientNameJohnDoeImplicit: ByteString = tagToBytesLE(0x00100010) ++ intToBytesLE(0x00000008) ++ ByteString("John^Doe")
  val emptyPatientName: ByteString = tagToBytesLE(0x00100010) ++ ByteString("PN") ++ shortToBytesLE(0x0000)
  val studyDate: ByteString = tagToBytesLE(0x00080020) ++ ByteString("DA") ++ shortToBytesLE(0x0008) ++ ByteString("19700101")

  def item(): ByteString = item(0xFFFFFFFF)
  def item(length: Int): ByteString = tagToBytesLE(0xFFFEE000) ++ intToBytesLE(length)
  def fragment(length: Int): ByteString = item(length)

  val itemEnd: ByteString = tagToBytesLE(0xFFFEE00D) ++ intToBytesLE(0x00000000)
  val sequenceEnd: ByteString = tagToBytesLE(0xFFFEE0DD) ++ intToBytesLE(0x00000000)
  val fragmentsEnd: ByteString = sequenceEnd
  val sequenceEndNonZeroLength: ByteString = tagToBytesLE(0xFFFEE0DD) ++ intToBytesLE(0x00000010)
  val pixeDataFragments: ByteString = tagToBytesLE(0x7FE00010) ++ ByteString('O', 'W', 0, 0) ++ intToBytesLE(0xFFFFFFFF)

  def sequence(tag: Int): ByteString = sequence(tag, 0xFFFFFFFF)
  def sequence(tag: Int, length: Int): ByteString = tagToBytesLE(tag) ++ ByteString('S', 'Q', 0, 0) ++ intToBytesLE(length)

  val waveformSeqStart: ByteString = tagToBytesLE(0x54000100) ++ ByteString('S', 'Q', 0, 0) ++ intToBytesLE(0xFFFFFFFF)

  def pixelData(length: Int): ByteString = tagToBytesLE(0x7FE00010) ++ ByteString('O', 'W', 0, 0) ++ intToBytes(length, bigEndian = false) ++ ByteString(new Array[Byte](length))
  def waveformData(length: Int): ByteString = tagToBytesLE(0x54001010) ++ ByteString('O', 'W', 0, 0) ++ intToBytes(length, bigEndian = false) ++ ByteString(new Array[Byte](length))

}
