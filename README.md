# dcm4che-streams

Service | Status | Description
------- | ------ | -----------
Travis            | [![Build Status](https://travis-ci.org/slicebox/dcm4che-streams.svg?branch=master)](https://travis-ci.org/slicebox/dcm4che-streams.svg?branch=master) | [Tests](https://travis-ci.org/slicebox/dcm4che-streams/)
Coveralls         | [![Coverage Status](https://coveralls.io/repos/github/slicebox/dcm4che-streams/badge.svg?branch=master)](https://coveralls.io/github/slicebox/dcm4che-streams?branch=master) | Code coverage



### Migration to dicom-streams

This project has migrated to the project [dicom-streams](https://github.com/slicebox/dicom-streams) as is no longer maintained.
dicom-streams aims to become a standalone (partial) DICOM implementation with a fully streaming API.

### Introduction

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
FileIO.fromPath(Paths.get("source-file.dcm"))
  .via(validateFlow)
  .via(parseFlow)
  .via(tagFilter(_ => true)(tagPath => tagPath.toList.map(_.tag).exists(isPrivateAttribute))) // no private attributes anywhere on tag path
  .map(_.bytes)
  .runWith(FileIO.toPath(Paths.get("target-file.dcm")))
```

Care should be taken when modifying DICOM data so that the resulting data is still valid. For instance, group length
tags may need to be removed or updated after modifying attributes. Here is an example that modifies the `PatientName`
and `SOPInstanceUID` attributes. To ensure the resulting data is valid, group length tags in the dataset are removed and
the meta information group tag is updated.

```scala
val updatedSOPInstanceUID = padToEvenLength(ByteString(createUID()), VR.UI)

FileIO.fromPath(Paths.get("source-file.dcm"))
  .via(validateFlow)
  .via(parseFlow)
  .via(groupLengthDiscardFilter) // discard group length attributes in dataset
  .via(modifyFlow(
    TagModification.endsWith(TagPath.fromTag(Tag.PatientName), _ => padToEvenLength(ByteString("John Doe"), VR.PN), insert = false),
    TagModification.endsWith(TagPath.fromTag(Tag.MediaStorageSOPInstanceUID), _ => updatedSOPInstanceUID, insert = false),
    TagModification.endsWith(TagPath.fromTag(Tag.SOPInstanceUID), _ => updatedSOPInstanceUID, insert = true),
  ))
  .via(fmiGroupLengthFlow()) // update group length in meta information, if present
  .map(_.bytes)
  .runWith(FileIO.toPath(Paths.get("target-file.dcm")))
```

The next example materializes a stream as a dcm4che `Attributes` objects instead of writing data to disk.

```scala
val futureAttributes: Future[(Option[Attributes], Option[Attributes])] =
  FileIO.fromPath(Paths.get("source-file.dcm"))
    .via(validateFlow)
    .via(parseFlow)
    .via(attributeFlow) // must turn headers + chunks into complete attributes before materializing
    .runWith(attributesSink)
    
futureAttributes.map {
  case (maybeMetaInformation, maybeDataset) => ??? // do something with attributes here
}
```

New non-trivial DICOM flows can be built using a modular system of capabilities that are mixed in as appropriate with a 
core class implementing a common base interface. The base interface for DICOM flows is `DicomFlow` and new flows are 
created using the `DicomFlowFactory.create` method. The `DicomFlow` interface defines a series of events, one for each
type of `DicomPart` that is produced when parsing DICOM data with `DicomParseFlow`. The core events are:
```scala
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
```
Default behavior to these events are implemented in core classes. The most natural behavior is to simply pass parts on
down the stream, e.g. 
```scala
  def onPreamble(part: DicomPreamble): List[DicomPart] = part :: Nil
  def onHeader(part: DicomHeader): List[DicomPart] = part :: Nil
  ...
```
This behavior is implemented in the `IdentityFlow` core class. Another option is to defer handling to the `onPart` method
which is implemented in the `DeferToPartFlow` core class. This is appropriate for flows which define a common 
behavior for all part types. 

To give an example of a custom flow, here is the implementation of a filter that removes 
nested sequences from a dataset. We define a nested dataset as a sequence with `depth > 1` given that the root dataset 
has `depth = 0`.
```scala
  def nestedSequencesFilter() = DicomFlowFactory.create(new DeferToPartFlow with TagPathTracking {
    override def onPart(part: DicomPart): List[DicomPart] = if (tagPath.depth() > 1) Nil else part :: Nil
  })
```
In this example, we chose to use `DeferToPartFlow` as the core class and mixed in the `TagPathTracking` capability
which gives access to a `tagPath: TagPath` variable at all times which is automatically updated as the flow progresses.
Note that flows with internal state should be defined as functions (`def`) rather than constants/variables `val`/`var`
to avoid shared state within or between flows.