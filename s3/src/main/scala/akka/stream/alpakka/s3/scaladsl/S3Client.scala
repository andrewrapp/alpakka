/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.s3.impl.{ CompleteMultipartUploadResult, S3Bucket, S3Location, S3Stream }
import akka.stream.alpakka.s3.acl.CannedAcl
import akka.stream.alpakka.s3.auth.AWSCredentials
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.http.scaladsl.model.{ ContentType, ContentTypes, Uri }
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

final case class MultipartUploadResult(location: Uri, bucket: String, key: String, etag: String)

object MultipartUploadResult {
  def apply(r: CompleteMultipartUploadResult): MultipartUploadResult =
    new MultipartUploadResult(r.location, r.bucket, r.key, r.etag)
}

object S3Client {
  val MinChunkSize = 5242880
}

case class ProxyTo(host: String, port: Int)

final class S3Client(credentials: AWSCredentials, region: String, proxyTo: Option[ProxyTo])(
    implicit system: ActorSystem,
    mat: Materializer) {
  import S3Client._
  private val impl = new S3Stream(credentials, region)

  def download(bucket: String, key: String): Source[ByteString, NotUsed] =
    impl.download(S3Location(bucket, key), region, proxyTo)

  def listBucket(bucket: String,
                 prefix: Option[String],
                 maxKeys: Option[Int],
                 marker: Option[String]): Source[ByteString, NotUsed] =
    impl.listBucket(S3Bucket(bucket), region, proxyTo, prefix, maxKeys, marker)

  def multipartUpload(bucket: String,
                      key: String,
                      contentType: ContentType = ContentTypes.`application/octet-stream`,
                      cannedAcl: CannedAcl = CannedAcl.Private,
                      chunkSize: Int = MinChunkSize,
                      chunkingParallelism: Int = 4): Sink[ByteString, Future[MultipartUploadResult]] =
    impl
      .multipartUpload(S3Location(bucket, key), contentType, cannedAcl, chunkSize, chunkingParallelism)
      .mapMaterializedValue(_.map(MultipartUploadResult.apply)(system.dispatcher))
}
