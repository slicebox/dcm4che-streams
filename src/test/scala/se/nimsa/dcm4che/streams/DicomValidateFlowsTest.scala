package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.UID._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class DicomValidateFlowsTest extends TestKit(ActorSystem("DicomValidateFlowsSpec")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  import DicomData._
  import se.nimsa.dcm4che.streams.DicomFlows._

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  override def afterAll() = system.terminate()

  "The DICOM validation flow" should "accept a file consisting of a preamble only" in {
    val bytes = preamble

    val source = Source.single(bytes)
      .via(new Chunker(1000))
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(preamble)
      .expectComplete()
  }

  it should "accept a file consisting of a preamble only when it arrives in very small chunks" in {
    val bytes = preamble

    val source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(preamble)
      .expectComplete()
  }

  it should "accept a file with no preamble and which starts with an attribute header" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(patientNameJohnDoe)
      .expectComplete()
  }

  it should "not accept a file with a preamble followed by a corrupt attribute header" in {
    val bytes = preamble ++ ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val source = Source.single(bytes)
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()
  }


  it should "accept any file that starts like a DICOM file" in {
    val bytes = preamble ++ fmiGroupLength(tsuidExplicitLE) ++ ByteString.fromArray(new Array[Byte](1024))

    val source = Source.single(bytes)
      .via(new Chunker(10))
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes.take(140))
      .request(1)
      .expectNext(ByteString(28, 0, 0, 0, 0, 0, 0, 0, 0, 0))  // group length value (28, 0) + 8 zeros
      .request(1)
      .expectNext(ByteString(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  }

  "The DICOM validation flow with contexts" should "buffer first 512 bytes" in {

    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = preamble ++
      fmiGroupLength(tsuidExplicitLE) ++
      fmiVersion ++
      mediaStorageSOPClassUID ++
      mediaStorageSOPInstanceUID ++
      tsuidExplicitLE ++
      ByteString.fromArray(new Array[Byte](1024))

    val source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes.take(512))
      .request(1)
      .expectNext(ByteString(0))

  }

  it should "accept dicom data that corresponds to the given contexts" in {

    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = preamble ++
      fmiGroupLength(tsuidExplicitLE) ++
      fmiVersion ++
      mediaStorageSOPClassUID ++
      mediaStorageSOPInstanceUID ++
      tsuidExplicitLE

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(moreThan512Bytes.take(512))
      .request(1)
      .expectNext(ByteString(0))

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes)
      .expectComplete()

  }


  it should "not accept dicom data that does not correspond to the given contexts" in {

    val contexts = Seq(ValidationContext(CTImageStorage, "1.2.840.10008.1.2.2"))
    val bytes = preamble ++
      fmiGroupLength(tsuidExplicitLE) ++
      fmiVersion ++
      mediaStorageSOPClassUID ++
      mediaStorageSOPInstanceUID ++
      tsuidExplicitLE

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()

  }


  it should "be able to parse dicom file meta information with missing mandatory fields" in {

    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = preamble ++
      fmiVersion ++
      mediaStorageSOPClassUID ++
      tsuidExplicitLE

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(moreThan512Bytes.take(512))
      .request(1)
      .expectNext(ByteString(0))

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes)
      .expectComplete()
  }

  it should "be able to parse dicom file meta information with wrong transfer syntax" in {
    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))

    val bytes = preamble ++
      fmiVersionImplicitLE ++
      mediaStorageSOPClassUIDImplicitLE ++
      tsuidExplicitLEImplicitLE

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(moreThan512Bytes.take(512))
      .request(1)
      .expectNext(ByteString(0))

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(new Chunker(1))
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes)
      .expectComplete()
  }

  it should "accept a file with no preamble and which starts with an attribute header if no context is given" in {
    val bytes = patientNameJohnDoe

    val source = Source.single(bytes)
      .via(validateFlow)

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(patientNameJohnDoe)
      .expectComplete()
  }

  it should "not accept a file with no preamble and no SOPCLassUID if a context is given" in {
    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = instanceCreatorUID

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()
  }

  it should "not accept a file with no preamble and wrong order of DICOM fields if a context is given" in {
    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = patientNameJohnDoe ++
      sopClassUID

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()
  }

  it should "not accept a file with no preamble and SOPClassUID if not corrseponding to the given context" in {
    val contexts = Seq(ValidationContext(CTImageStorage, "1.2.840.10008.1.2.2"))
    val bytes = instanceCreatorUID ++
      sopClassUID ++
      patientNameJohnDoe

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectError()
  }

  it should "accept a file with no preamble and SOPClassUID if corrseponding to the given context" in {
    val contexts = Seq(ValidationContext(CTImageStorage, ExplicitVRLittleEndian))
    val bytes = instanceCreatorUID ++
      sopClassUID ++
      patientNameJohnDoe

    val moreThan512Bytes = bytes ++ ByteString.fromArray(new Array[Byte](1024))

    // test with more than 512 bytes
    var source = Source.single(moreThan512Bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(moreThan512Bytes)
      .expectComplete()

    // test with less than 512 bytes
    source = Source.single(bytes)
      .via(validateFlowWithContext(contexts))

    source.runWith(TestSink.probe[ByteString])
      .request(1)
      .expectNext(bytes)
      .expectComplete()
  }

}
