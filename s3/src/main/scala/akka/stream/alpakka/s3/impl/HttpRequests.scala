/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import scala.concurrent.{ ExecutionContext, Future }
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{ Host, RawHeader }
import akka.util.ByteString
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.scaladsl.Source
import akka.http.scaladsl.model.RequestEntity
import akka.http.scaladsl.model.RequestEntity
import akka.stream.alpakka.s3.scaladsl.ProxyTo

import scala.collection.immutable.Iterable
import scala.util.Random

private[alpakka] object HttpRequests {

  def s3Request(s3Location: S3Location,
                method: HttpMethod = HttpMethods.GET,
                uriFn: (Uri => Uri) = identity): HttpRequest =
    HttpRequest(method).withHeaders(Host(requestHost(s3Location.bucket))).withUri(uriFn(requestUri(s3Location)))

  def initiateMultipartUploadRequest(s3Location: S3Location,
                                     contentType: ContentType,
                                     cannedAcl: CannedAcl): HttpRequest =
    s3Request(s3Location, HttpMethods.POST, _.withQuery(Query("uploads")))
      .withDefaultHeaders(RawHeader("x-amz-acl", cannedAcl.value))
      .withEntity(HttpEntity.empty(contentType))

  def getRequest(s3Location: S3Location): HttpRequest =
    s3Request(s3Location)

  def download(s3Location: S3Location,
               region: String,
               proxyTo: Option[ProxyTo],
               method: HttpMethod = HttpMethods.GET,
               uriFn: (Uri => Uri) = identity): HttpRequest =
    HttpRequest(method)
      .withHeaders(Host(requestHost(s3Location.bucket)))
      .withUri(uriFn(requestUri(s"/${s3Location.key}", s3Location.bucket, proxyTo)))

  def listBucket(s3Bucket: S3Bucket,
                 region: String,
                 proxyTo: Option[ProxyTo],
                 prefix: Option[String],
                 maxKeys: Option[Int],
                 marker: Option[String]): HttpRequest =
    HttpRequest(HttpMethods.GET)
      .withHeaders(Host(requestHost(s3Bucket.bucket)))
      .withUri(listBucketUri(s3Bucket, region, proxyTo, prefix, maxKeys, marker))

  def uploadPartRequest(upload: MultipartUpload,
                        partNumber: Int,
                        payload: Source[ByteString, _],
                        payloadSize: Int): HttpRequest =
    s3Request(
      upload.s3Location,
      HttpMethods.PUT,
      _.withQuery(Query("partNumber" -> partNumber.toString, "uploadId" -> upload.uploadId))
    ).withEntity(HttpEntity(ContentTypes.`application/octet-stream`, payloadSize, payload))

  def completeMultipartUploadRequest(upload: MultipartUpload, parts: Seq[(Int, String)])(
      implicit ec: ExecutionContext): Future[HttpRequest] = {
    val payload = <CompleteMultipartUpload>
      {parts.map { case (partNumber, etag) => <Part>
        <PartNumber>
          {partNumber}
        </PartNumber> <ETag>
          {etag}
        </ETag>
      </Part>
      }}
    </CompleteMultipartUpload>
    for {
      entity <- Marshal(payload).to[RequestEntity]
    } yield {
      s3Request(
        upload.s3Location,
        HttpMethods.POST,
        _.withQuery(Query("uploadId" -> upload.uploadId))
      ).withEntity(entity)
    }
  }

  def requestHost(s3Bucket: String): Uri.Host = Uri.Host(s"${s3Bucket}.s3.amazonaws.com")
  //def requestHost(s3Location: S3Location): Uri.Host = Uri.Host(s"${s3Location.bucket}.s3.amazonaws.com")
  //def requestHost(region: String): Uri.Host = Uri.Host(s"s3-${region}.amazonaws.com")
  //def requestHost(): Uri.Host = Uri.Host("s3.amazonaws.com")

  def requestUri(s3Location: S3Location): Uri =
    Uri(s"/${s3Location.key}").withHost(requestHost(s3Location.bucket)).withScheme("https")

  def requestUri(path: String, bucket: String, proxyTo: Option[ProxyTo]): Uri =
    proxyTo match {
      case Some(proxyTo) =>
        // redirect all traffic to a proxy
        Uri(path).withHost(proxyTo.host).withPort(proxyTo.port).withScheme("http")
      case _ =>
        Uri(path).withHost(requestHost(bucket)).withScheme("https")
    }

  def listBucketUri(s3Bucket: S3Bucket,
                    region: String,
                    proxyTo: Option[ProxyTo],
                    prefix: Option[String],
                    maxKeys: Option[Int],
                    marker: Option[String]): Uri = {

    val prefixQuery: Map[String, String] = prefix match {
      case Some(p) => Map("prefix" -> p)
      case None => Map[String, String]().empty
    }

    val maxKeysQuery: Map[String, String] = maxKeys match {
      case Some(m) => Map("max-keys" -> m.toString)
      case _ => Map[String, String]().empty
    }

    val markerQuery: Map[String, String] = marker match {
      case Some(m) => Map("marker" -> m)
      case _ => Map[String, String]().empty
    }

    requestUri(s"/", region, proxyTo).withQuery(Uri.Query(prefixQuery ++ markerQuery ++ maxKeysQuery))
  }
}
