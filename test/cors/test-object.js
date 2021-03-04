import {
  runTests,
  MakeRequest,
  HasStatus,
  HasHeaders,
  HasCommonResponseHeaders,
  HasNoBody,
  BodyHasLength,
  CorsBlocked,
  NotFound,
  Unauthorized
} from './harness.js'

runTests('object', [
  ['GET',
    () => MakeRequest('GET', 'public-with-cors/obj')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(1024))],
  ['HEAD',
    () => MakeRequest('HEAD', 'public-with-cors/obj')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({ 'Content-Type': 'application/octet-stream' }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(HasNoBody)],
  ['GET Range',
    () => MakeRequest('GET', 'public-with-cors/obj', { Range: 'bytes=100-199' })
      .then(HasStatus(206, 'Partial Content'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(100))],
  ['GET If-Match matching',
    () => MakeRequest('GET', 'public-with-cors/obj', { 'If-Match': '0f343b0931126a20f133d67c2b018a3b' })
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(1024))],
  ['GET If-Match not matching',
    () => MakeRequest('GET', 'public-with-cors/obj', { 'If-Match': 'something-else' })
      .then(HasStatus(412, 'Precondition Failed'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'text/html; charset=UTF-8',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(HasNoBody)],
  ['GET If-None-Match matching',
    () => MakeRequest('GET', 'public-with-cors/obj', { 'If-None-Match': '0f343b0931126a20f133d67c2b018a3b' })
      .then(HasStatus(304, 'Not Modified'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        // TODO: Content-Type can vary depending on storage policy type...
        // 'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['Content-Type', 'X-Object-Meta-Mtime']))
      .then(HasNoBody)],
  ['GET If-None-Match not matching',
    () => MakeRequest('GET', 'public-with-cors/obj', { 'If-None-Match': 'something-else' })
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(1024))],
  ['GET not found',
    () => MakeRequest('GET', 'public-with-cors/should-404')
      .then(NotFound)],
  ['POST',
    () => MakeRequest('POST', 'public-with-cors/obj')
      // No good way to make a container publicly-writable
      .then(Unauthorized)],
  ['POST with meta',
    () => MakeRequest('POST', 'public-with-cors/obj', { 'X-Object-Meta-Foo': 'bar' })
      // Still no good way to make a container publicly-writable, but notably,
      // *the POST goes through* and this isn't just CorsBlocked
      .then(Unauthorized)],
  ['GET no CORS, object exists',
    () => MakeRequest('GET', 'public-no-cors/obj')
      .then(CorsBlocked)], // But req 200s
  ['GET no CORS, object does not exist',
    () => MakeRequest('GET', 'public-no-cors/should-404')
      .then(CorsBlocked)], // But req 404s
  ['GET Range no CORS',
    () => MakeRequest('GET', 'public-no-cors/obj', { Range: 'bytes=100-199' })
      .then(CorsBlocked)], // preflight fails
  ['GET other-referrer, object exists',
    () => MakeRequest('GET', 'other-referrer-allowed/obj')
      .then(CorsBlocked)], // But req 401s
  ['GET other-referrer, object does not exist',
    () => MakeRequest('GET', 'other-referrer-allowed/should-404')
      .then(CorsBlocked)], // But req 401s
  ['GET Range other-referrer',
    () => MakeRequest('GET', 'other-referrer-allowed/obj', { Range: 'bytes=100-199' })
      .then(CorsBlocked)], // preflight fails
  ['GET other-referrer, attempt to spoof referer',
    () => MakeRequest('GET', 'other-referrer-allowed/obj', { Referer: 'https://other-host' })
      .then(CorsBlocked)], // new header gets ignored, req 401s with no allow-origin
  ['GET no ACL, object exists',
    () => MakeRequest('GET', 'private-with-cors/obj')
      .then(Unauthorized)],
  ['GET no ACL, object does not exist',
    () => MakeRequest('GET', 'private-with-cors/would-404')
      .then(Unauthorized)],
  ['GET completely private',
    () => MakeRequest('GET', 'private/obj')
      .then(CorsBlocked)],
  ['GET Range completely private',
    () => MakeRequest('GET', 'private/obj', { Range: 'bytes=100-199' })
      .then(CorsBlocked)],
  ['GET referrer allowed',
    () => MakeRequest('GET', 'referrer-allowed/obj')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(1024))],
  ['HEAD referrer allowed',
    () => MakeRequest('HEAD', 'referrer-allowed/obj')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(HasNoBody)],
  ['GET Range referrer allowed',
    () => MakeRequest('GET', 'referrer-allowed/obj', { Range: 'bytes=100-199' })
      .then(HasStatus(206, 'Partial Content'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(100))],
  ['GET attempt to spoof referer',
    () => MakeRequest('GET', 'referrer-allowed/obj', { Referer: 'https://other-host' })
      // new header gets ignored, no preflight, get succeeds
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(BodyHasLength(1024))]
])
