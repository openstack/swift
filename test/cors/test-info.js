import {
  runTests,
  MakeRequest,
  HasStatus,
  HasHeaders,
  DoesNotHaveHeaders,
  HasNoBody,
  CorsBlocked
} from './harness.js'

function CheckInfoHeaders (resp) {
  return Promise.resolve(resp)
    .then(HasHeaders({ 'Content-Type': 'application/json; charset=UTF-8' }))
    .then(HasHeaders(['X-Trans-Id']))
    .then(DoesNotHaveHeaders([
      'X-Openstack-Request-Id', // TODO: this is blocked by CORS but almost certainly shouldn't
      'X-Timestamp',
      'Accept-Ranges',
      'Access-Control-Allow-Origin',
      'Access-Control-Expose-Headers',
      'Date',
      'Content-Range'
    ]))
}
function CheckInfoBody (resp) {
  const clusterInfo = JSON.parse(resp.responseText)
  if (!('swift' in clusterInfo)) {
    throw new Error('Expected to find "swift" in /info response; ' +
                    'got ' + JSON.stringify(clusterInfo))
  }
  if (!('version' in clusterInfo.swift)) {
    throw new Error('Expected to find "swift.version" in /info response; ' +
                    'got ' + JSON.stringify(clusterInfo.swift))
  }
  console.log('Tested against Swift version ' + clusterInfo.swift.version)
  return clusterInfo
}

export const GetClusterInfo = MakeRequest('GET', '/info')
  .then(HasStatus(200, 'OK'))
  .then(CheckInfoHeaders)
  .then(CheckInfoBody)

// TODO: /info should probably get an automatic access-control-allow-origin: *
runTests('cluster info', [
  ['GET', () => GetClusterInfo],
  ['GET with header', () => MakeRequest('GET', '/info', { 'X-Trans-Id-Extra': 'my-tracker' })
    // 200, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)],
  ['HEAD', () => MakeRequest('HEAD', '/info')
    .then(HasStatus(200, 'OK'))
    .then(CheckInfoHeaders)
    .then(HasNoBody)],
  ['OPTIONS', () => MakeRequest('OPTIONS', '/info')
    // 200, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)],
  ['POST', () => MakeRequest('POST', '/info')
    // 405, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)]
])
