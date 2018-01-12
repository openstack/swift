import { runTests, MakeRequest, CorsBlocked } from './harness.js'

runTests('account', [
  ['GET', () => MakeRequest('GET', '')
    // 200, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)],
  ['HEAD', () => MakeRequest('HEAD', '')
    // 200, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)],
  ['POST', () => MakeRequest('POST', '')
    // 200, but missing Access-Control-Allow-Origin
    .then(CorsBlocked)],
  ['POST with meta', () => MakeRequest('POST', '', { 'X-Account-Meta-Never-Makes-It': 'preflight failed' })
    // preflight 200s, but it's missing Access-Control-Allow-Origin
    .then(CorsBlocked)]
])
