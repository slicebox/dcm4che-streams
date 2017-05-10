/*
 * Copyright 2017 Lars Edenbrandt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.nimsa.dcm4che.streams

import akka.stream.SinkShape
import akka.stream.scaladsl.{Broadcast, GraphDSL, Sink}
import akka.util.ByteString
import se.nimsa.dcm4che.streams.DicomParts.DicomPart

import scala.concurrent.Future

object DicomSinks {

  import DicomFlows.{attributeFlow, whitelistFilter, validateFlow}
  import DicomPartFlow._

  def bytesAndAttributesSink[A, B](bytesSink: Sink[ByteString, Future[A]],
                                   attributesSink: Sink[DicomPart, Future[B]],
                                   tagsWhiteList: Seq[Int]): Sink[ByteString, Future[(A, B)]] =
    Sink.fromGraph(GraphDSL.create(bytesSink, attributesSink)(_ zip _) {
      implicit builder =>
        (bytesConsumer, attributesConsumer) =>
          import GraphDSL.Implicits._

          // construct graph
          val validate = builder.add(validateFlow)
          val bcast = builder.add(Broadcast[ByteString](2))

          validate.out ~> bcast.in

          bcast.out(0) ~> partFlow ~> whitelistFilter(tagsWhiteList) ~> attributeFlow ~> attributesConsumer
          bcast.out(1) ~> bytesConsumer

          SinkShape(validate.in)
    })

}
