package se.nimsa.dcm4che.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.dcm4che3.data.Tag
import org.dcm4che3.io.DicomStreamException
import org.scalatest.{AsyncFlatSpecLike, Matchers}

class DicomSinksTest extends TestKit(ActorSystem("DicomFlowSpec")) with AsyncFlatSpecLike with Matchers {

  import DicomData._
  import se.nimsa.dcm4che.streams.DicomAttributesSink._
  import se.nimsa.dcm4che.streams.DicomSinks._

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  "The DICOM bytes and attributes sink" should "store bytes and attributes in separate sinks" in {
    val bytes = patientNameJohnDoe ++ studyDate
    val bytesSource = Source.single(bytes)

    val bytesSink = Sink.head[ByteString]
    val attrSink = attributesSink

    val testSink = bytesAndAttributesSink(bytesSink, attrSink, Seq(Tag.PatientName))
    bytesSource.runWith(testSink).map {
      case (storedBytes, (maybeFmi, maybeDataset)) =>
        storedBytes shouldBe bytes
        maybeFmi should not be defined
        maybeDataset shouldBe defined
        maybeDataset.get should have size 1
        maybeDataset.get.getString(Tag.PatientName) shouldBe "John^Doe"
    }
  }

  it should "fail for non-DICOM data" in {
    val bytes = ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val bytesSource = Source.single(bytes)

    val bytesSink = Sink.head[ByteString]
    val attrSink = attributesSink

    val testSink = bytesAndAttributesSink(bytesSink, attrSink, Seq(Tag.PatientName))

    recoverToSucceededIf[DicomStreamException] {
      bytesSource.runWith(testSink)
    }
  }

}
