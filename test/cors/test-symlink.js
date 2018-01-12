import {
  runTests,
  MakeRequest,
  HasStatus,
  HasHeaders,
  HasCommonResponseHeaders,
  DoesNotHaveHeaders,
  HasNoBody,
  CorsBlocked,
  Skip
} from './harness.js'
import { GetClusterInfo } from './test-info.js'

function MakeSymlinkRequest () {
  return GetClusterInfo.then((clusterInfo) => {
    if (!('symlink' in clusterInfo)) {
      throw new Skip('Symlink is not enabled')
    }
    return MakeRequest(...arguments)
  })
}

runTests('symlink', [
  ['GET link to no CORS',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-no-cors')
      .then(CorsBlocked)],
  ['HEAD link to no CORS',
    () => MakeSymlinkRequest('HEAD', 'public-with-cors/symlink-to-public-no-cors')
      .then(CorsBlocked)],
  ['GET Range link to no CORS',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-no-cors', { Range: 'bytes=100-199' })
      .then(CorsBlocked)], // But preflight *succeeded*!

  ['GET link with CORS',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then((resp) => {
        if (resp.responseText.length !== 1024) {
          throw new Error('Expected body to have length 1024, got ' + resp.responseText.length)
        }
      })],
  ['HEAD link with CORS',
    () => MakeSymlinkRequest('HEAD', 'public-with-cors/symlink-to-public-with-cors')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then(HasNoBody)],
  ['GET Range link with CORS',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors', { Range: 'bytes=100-199' })
      .then(HasStatus(206, 'Partial Content'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then((resp) => {
        if (resp.responseText.length !== 100) {
          throw new Error('Expected body to have length 100, got ' + resp.responseText.length)
        }
      })],

  ['GET private',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-private')
      .then(CorsBlocked)], // TODO: maybe should be Unauthorized?
  ['HEAD private',
    () => MakeSymlinkRequest('HEAD', 'public-with-cors/symlink-to-private')
      .then(CorsBlocked)], // TODO: maybe should be Unauthorized?
  ['GET private Range',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-private', { Range: 'bytes=100-199' })
      .then(CorsBlocked)], // TODO: maybe should be Unauthorized?

  ['GET If-Match matching',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors', { 'If-Match': '0f343b0931126a20f133d67c2b018a3b' })
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then((resp) => {
        if (resp.responseText.length !== 1024) {
          throw new Error('Expected body to have length 1024, got ' + resp.responseText.length)
        }
      })],
  ['GET If-Match not matching',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors', { 'If-Match': 'something-else' })
      .then(HasStatus(412, 'Precondition Failed'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'text/html; charset=UTF-8',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then(HasNoBody)],
  ['GET If-None-Match matching',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors', { 'If-None-Match': '0f343b0931126a20f133d67c2b018a3b' })
      .then(HasStatus(304, 'Not Modified'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        // Content-Type can vary depending on storage policy type...
        // 'Content-Type': 'text/html; charset=UTF-8',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['Content-Type', 'X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then(HasNoBody)],
  ['GET If-None-Match not matching',
    () => MakeSymlinkRequest('GET', 'public-with-cors/symlink-to-public-with-cors', { 'If-None-Match': 'something-else' })
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        Etag: '0f343b0931126a20f133d67c2b018a3b'
      }))
      .then(HasHeaders(['X-Object-Meta-Mtime']))
      .then(DoesNotHaveHeaders(['Content-Location']))
      .then((resp) => {
        if (resp.responseText.length !== 1024) {
          throw new Error('Expected body to have length 1024, got ' + resp.responseText.length)
        }
      })]
])
