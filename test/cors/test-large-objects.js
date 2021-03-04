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

function MakeSloRequest () {
  return GetClusterInfo.then((clusterInfo) => {
    if (!('slo' in clusterInfo)) {
      throw new Skip('SLO is not enabled')
    }
    return MakeRequest(...arguments)
  })
}

runTests('large object', [
  ['GET DLO',
    () => MakeRequest('GET', 'public-with-cors/dlo')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        ETag: '"8d431e7531abb83a6cf67e56d91c6f74"'
      }))
      .then(DoesNotHaveHeaders(['X-Object-Manifest'])) // TODO: should maybe be exposed
      .then((resp) => {
        if (resp.responseText.length !== 10485760) {
          throw new Error('Expected body to have length 10485760, got ' + resp.responseText.length)
        }
      })],
  ['GET DLO with unlistable segments',
    () => MakeRequest('GET', 'public-with-cors/dlo-with-unlistable-segments')
      .then(CorsBlocked)], // TODO: should probably be Unauthorized
  ['GET SLO',
    () => MakeSloRequest('GET', 'public-with-cors/slo')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        ETag: '"8d431e7531abb83a6cf67e56d91c6f74"'
      }))
      .then(DoesNotHaveHeaders(['X-Static-Large-Object'])) // TODO: should maybe be exposed
      .then((resp) => {
        if (resp.responseText.length !== 10485760) {
          throw new Error('Expected body to have length 10485760, got ' + resp.responseText.length)
        }
      })],
  ['HEAD SLO',
    () => MakeSloRequest('HEAD', 'public-with-cors/slo')
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        ETag: '"8d431e7531abb83a6cf67e56d91c6f74"'
      }))
      .then(DoesNotHaveHeaders(['X-Static-Large-Object'])) // TODO: should maybe be exposed
      .then(HasNoBody)],
  ['GET SLO Range',
    () => MakeSloRequest('GET', 'public-with-cors/slo', { Range: 'bytes=100-199' })
      .then(HasStatus(206, 'Partial Content'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        ETag: '"8d431e7531abb83a6cf67e56d91c6f74"'
      }))
      .then(DoesNotHaveHeaders(['X-Static-Large-Object'])) // TODO: should maybe be exposed
      .then((resp) => {
        if (resp.responseText.length !== 100) {
          throw new Error('Expected body to have length 100, got ' + resp.responseText.length)
        }
      })],
  ['GET SLO Suffix Range',
    () => MakeSloRequest('GET', 'public-with-cors/slo', { Range: 'bytes=-100' })
      .then(HasStatus(206, 'Partial Content'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({
        'Content-Type': 'application/octet-stream',
        ETag: '"8d431e7531abb83a6cf67e56d91c6f74"'
      }))
      .then(DoesNotHaveHeaders(['X-Static-Large-Object'])) // TODO: should maybe be exposed
      .then((resp) => {
        if (resp.responseText.length !== 100) {
          throw new Error('Expected body to have length 100, got ' + resp.responseText.length)
        }
      })]
])
