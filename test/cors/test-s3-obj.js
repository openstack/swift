/* global PARAMS */

import {
  runTests,
  MakeRequest,
  HasStatus,
  HasHeaders,
  DoesNotHaveHeaders,
  HasNoBody,
  BodyHasLength,
  CorsBlocked
} from './harness.js'

import './vendor/aws-sdk-2.829.0.min.js'
const AWS = window.AWS

function CheckTransactionIdHeaders (resp) {
  return Promise.resolve(resp)
    .then(HasHeaders([
      'x-amz-request-id',
      'x-amz-id-2',
      'X-Openstack-Request-Id',
      'X-Trans-Id'
    ]))
    .then((resp) => {
      const txnId = resp.getResponseHeader('X-Openstack-Request-Id')
      return Promise.resolve(resp)
        .then(HasHeaders({
          'x-amz-request-id': txnId,
          'x-amz-id-2': txnId,
          'X-Trans-Id': txnId
        }))
    })
}
function CheckS3Headers (resp) {
  return Promise.resolve(resp)
    .then(HasHeaders([
      'Last-Modified',
      'Content-Type'
    ]))
    .then(CheckTransactionIdHeaders)
    .then(DoesNotHaveHeaders([
      'X-Timestamp',
      'Accept-Ranges',
      'Access-Control-Allow-Origin',
      'Access-Control-Expose-Headers',
      'Date',
      // Hmmm....
      'Content-Range',
      'X-Account-Bytes-Used',
      'X-Account-Container-Count',
      'X-Account-Object-Count',
      'X-Container-Bytes-Used',
      'X-Container-Object-Count'
    ]))
}

function MakeS3Request (service, operation, params) {
  return new Promise((resolve, reject) => {
    const s3req = service[operation](params)
    // Don't *actually* send it
    s3req.removeListener('send', AWS.EventListeners.Core.SEND)

    // Instead, copy method, path, headers over to a new test-harness request
    s3req.addListener('send', function () {
      const endpoint = s3req.httpRequest.endpoint
      const signedReq = s3req.httpRequest

      const filteredHeaders = {}
      for (const header of Object.keys(signedReq.headers)) {
        if (header === 'Host' || header === 'Content-Length') {
          continue // browser won't let you send these anyway
        }
        filteredHeaders[header] = signedReq.headers[header]
      }
      resolve(MakeRequest(
        signedReq.method,
        endpoint.protocol + '//' + endpoint.host + signedReq.path,
        filteredHeaders,
        signedReq.body
      ))
    })

    s3req.send()
  })
}

function makeTests (params) {
  const service = new AWS.S3(params)
  return [
    ['presigned GET, no CORS',
      () => MakeRequest('GET', service.getSignedUrl('getObject', {
        Bucket: 'public-no-cors',
        Key: 'obj'
      }))
        .then(CorsBlocked)],
    ['presigned HEAD, no CORS',
      () => MakeRequest('HEAD', service.getSignedUrl('headObject', {
        Bucket: 'public-no-cors',
        Key: 'obj'
      }))
        .then(CorsBlocked)],
    ['presigned GET, object exists',
      () => MakeRequest('GET', service.getSignedUrl('getObject', {
        Bucket: 'private-with-cors',
        Key: 'obj'
      }))
        .then(HasStatus(200, 'OK'))
        .then(CheckS3Headers)
        .then(HasHeaders(['x-amz-meta-mtime']))
        .then(DoesNotHaveHeaders(['X-Object-Meta-Mtime']))
        .then(HasHeaders({
          'Content-Type': 'application/octet-stream',
          Etag: '"0f343b0931126a20f133d67c2b018a3b"'
        }))
        .then(BodyHasLength(1024))],
    ['presigned HEAD, object exists',
      () => MakeRequest('HEAD', service.getSignedUrl('headObject', {
        Bucket: 'private-with-cors',
        Key: 'obj'
      }))
        .then(HasStatus(200, 'OK'))
        .then(CheckS3Headers)
        .then(HasHeaders(['x-amz-meta-mtime']))
        .then(DoesNotHaveHeaders(['X-Object-Meta-Mtime']))
        .then(HasHeaders({
          'Content-Type': 'application/octet-stream',
          Etag: '"0f343b0931126a20f133d67c2b018a3b"'
        }))
        .then(HasNoBody)],
    ['GET, object exists',
      () => MakeS3Request(service, 'getObject', {
        Bucket: 'private-with-cors',
        Key: 'obj'
      })
        .then(HasStatus(200, 'OK'))
        .then(CheckS3Headers)
        .then(HasHeaders(['x-amz-meta-mtime']))
        .then(DoesNotHaveHeaders(['X-Object-Meta-Mtime']))
        .then(HasHeaders({
          'Content-Type': 'application/octet-stream',
          Etag: '"0f343b0931126a20f133d67c2b018a3b"'
        }))
        .then(BodyHasLength(1024))],
    ['PUT then DELETE',
      () => Promise.resolve('put-target-' + Math.random()).then((objectName) => {
        return MakeS3Request(service, 'putObject', {
          Bucket: 'private-with-cors',
          Key: objectName,
          Body: 'test'
        })
          .then(HasStatus(200, 'OK'))
          .then(CheckS3Headers)
          .then(HasHeaders({
            'Content-Type': 'text/html; charset=UTF-8',
            Etag: '"098f6bcd4621d373cade4e832627b4f6"'
          }))
          .then(HasNoBody)
          .then((resp) => {
            return MakeS3Request(service, 'deleteObject', {
              Bucket: 'private-with-cors',
              Key: objectName
            })
          })
          .then(HasStatus(204, 'No Content'))
          .then(CheckTransactionIdHeaders)
          .then(HasNoBody)
      })],
    ['presigned PUT then DELETE',
      () => Promise.resolve('put-target-' + Math.random()).then((objectName) => {
        return MakeRequest('PUT', service.getSignedUrl('putObject', {
          Bucket: 'private-with-cors',
          Key: objectName,
          ContentType: 'application/octet-stream'
          // Consciously go for an unsigned payload
        }), {'Content-Type': 'application/octet-stream'}, 'test')
          .then(HasStatus(200, 'OK'))
          .then(CheckS3Headers)
          .then(HasHeaders({
            'Content-Type': 'text/html; charset=UTF-8',
            Etag: '"098f6bcd4621d373cade4e832627b4f6"'
          }))
          .then(HasNoBody)
          .then((resp) => {
            return MakeRequest('DELETE', service.getSignedUrl('deleteObject', {
              Bucket: 'private-with-cors',
              Key: objectName
            }))
          })
          .then(HasStatus(204, 'No Content'))
          .then(CheckTransactionIdHeaders)
          .then(HasNoBody)
      })],
    ['GET If-Match matching',
      () => MakeS3Request(service, 'getObject', {
        Bucket: 'private-with-cors',
        Key: 'obj',
        IfMatch: '0f343b0931126a20f133d67c2b018a3b'
      })
        .then(HasStatus(200, 'OK'))
        .then(CheckS3Headers)
        .then(HasHeaders(['x-amz-meta-mtime']))
        .then(DoesNotHaveHeaders(['X-Object-Meta-Mtime']))
        .then(HasHeaders({
          'Content-Type': 'application/octet-stream',
          Etag: '"0f343b0931126a20f133d67c2b018a3b"'
        }))
        .then(BodyHasLength(1024))],
    ['GET Range',
      () => MakeS3Request(service, 'getObject', {
        Bucket: 'private-with-cors',
        Key: 'obj',
        Range: 'bytes=100-199'
      })
        .then(HasStatus(206, 'Partial Content'))
        .then(CheckS3Headers)
        .then(HasHeaders(['x-amz-meta-mtime']))
        .then(DoesNotHaveHeaders(['X-Object-Meta-Mtime']))
        .then(HasHeaders({
          'Content-Type': 'application/octet-stream',
          Etag: '"0f343b0931126a20f133d67c2b018a3b"'
        }))
        .then(BodyHasLength(100))]
  ]
}

runTests('s3 obj (v2)', makeTests({
  endpoint: PARAMS.S3_ENDPOINT || 'http://localhost:8080',
  region: PARAMS.S3_REGION || 'us-east-1',
  accessKeyId: PARAMS.S3_USER || 'test:tester',
  secretAccessKey: PARAMS.S3_KEY || 'testing',
  s3ForcePathStyle: true,
  signatureVersion: 'v2'
}))

runTests('s3 obj (v4)', makeTests({
  endpoint: PARAMS.S3_ENDPOINT || 'http://localhost:8080',
  region: PARAMS.S3_REGION || 'us-east-1',
  accessKeyId: PARAMS.S3_USER || 'test:tester',
  secretAccessKey: PARAMS.S3_KEY || 'testing',
  s3ForcePathStyle: true,
  signatureVersion: 'v4'
}))
