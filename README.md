# dcm4che-streams

Service | Status | Description
------- | ------ | -----------
Travis            | [![Build Status](https://travis-ci.org/slicebox/dcm4che-streams.svg?branch=develop)](https://travis-ci.org/slicebox/dcm4che-streams.svg?branch=develop) | [Tests](https://travis-ci.org/slicebox/dcm4che-streams/)
Coveralls         | [![Coverage Status](https://coveralls.io/repos/github/slicebox/dcm4che-streams/badge.svg?branch=develop)](https://coveralls.io/github/slicebox/dcm4che-streams?branch=develop) | Code coverage


The purpose of this project is to integrate [akka-streams](http://doc.akka.io/docs/akka/current/scala/stream/index.html) 
with [dcm4che](https://github.com/dcm4che/dcm4che). Features will be added as needed (mainly in the 
[slicebox](https://github.com/slicebox/slicebox) project) and may include streaming reading and writing of DICOM data,
as well as streaming SCP and SCU capabilities.

Advantages of streaming DICOM data include better control over resource allocation such as memory via strict bounds on 
DICOM data chunk size and network utilization using back-pressure as specified in the 
[Reactive Streams](http://www.reactive-streams.org/) protocol.

### Usage

The following example reads a DICOM file from disk, validates that it is a DICOM file, discards all private attributes
and writes it to a new file.

```scala
import akka.stream.scaladsl.FileIO
import java.nio.file.Paths
import org.dcm4che3.data.Tag
import se.nimsa.dcm4che.streams.DicomFlows._
import se.nimsa.dcm4che.streams.DicomPartFlow._

FileIO.fromPath(Paths.get("source-file.dcm"))
  .via(validateFlow)
  .via(partFlow)
  .via(blacklistFilter(DicomParsing.isPrivateAttribute(_)))
  .map(_.bytes)
  .runWith(FileIO.toPath(Paths.get("target-file.dcm")))
```

Same result can be achieved with a whitelist filter instead, but we need to tell the filter
to keep the preamble:

```scala
import akka.stream.scaladsl.FileIO
import java.nio.file.Paths
import org.dcm4che3.data.Tag
import se.nimsa.dcm4che.streams.DicomFlows._
import se.nimsa.dcm4che.streams.DicomPartFlow._

FileIO.fromPath(Paths.get("source-file.dcm"))
  .via(validateFlow)
  .via(partFlow)
  .via(whitelistFilter(!DicomParsing.isPrivateAttribute(_), keepPreamble = true))
  .map(_.bytes)
  .runWith(FileIO.toPath(Paths.get("target-file.dcm")))
```


The next example materializes the above stream as dcm4che `Attributes` objects instead of writing data to disk.
`partFilter` is a convenient way to specify a whitelist by specifying a sequence of tags that are whitelisted.

```scala
import akka.stream.scaladsl.FileIO
import java.nio.file.Paths
import org.dcm4che3.data.{Attributes, Tag}
import scala.concurrent.Future
import se.nimsa.dcm4che.streams.DicomAttributesSink._
import se.nimsa.dcm4che.streams.DicomFlows._
import se.nimsa.dcm4che.streams.DicomPartFlow._

val futureAttributes: Future[(Option[Attributes], Option[Attributes])] =
  FileIO.fromPath(Paths.get("source-file.dcm"))
    .via(validateFlow)
    .via(partFlow)
    .via(partFilter(Seq(Tag.PatientName, Tag.PatientID)))
    .via(attributeFlow) // must turn headers + chunks into complete attributes before materializing
    .runWith(attributesSink)
    
futureAttributes.map {
  case (maybeMetaInformation, maybeDataset) => ??? // do something with attributes here
}
```