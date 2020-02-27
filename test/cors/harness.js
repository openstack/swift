/* global PARAMS, XMLHttpRequest */

const STORAGE_URL = PARAMS.OS_STORAGE_URL || 'http://localhost:8080/v1/AUTH_test'

function makeUrl (path) {
  if (path.startsWith('http://') || path.startsWith('https://')) {
    return new URL(path)
  }
  if (!path.startsWith('/')) {
    return new URL(STORAGE_URL + '/' + path)
  }
  return new URL(STORAGE_URL.substr(0, STORAGE_URL.indexOf('/', 3 + STORAGE_URL.indexOf('://'))) + path)
}

export function MakeRequest (method, path, headers, body, params) {
  var url = makeUrl(path)
  headers = headers || {}
  params = params || {}
  if (!(
    url.searchParams.has('Signature') ||
    url.searchParams.has('X-Amz-Signature') ||
    'Authorization' in headers
  )) {
    // give each Swift request a unique query string to avoid ever fetching from cache
    params['cors-test-time'] = Date.now().toString()
    params['cors-test-random'] = Math.random().toString()
  }
  for (var key in params) {
      url.searchParams.append(key, params[key])
  }
  return new Promise((resolve, reject) => {
    const req = new XMLHttpRequest()
    req.addEventListener('readystatechange', function () {
      if (this.readyState === 4) {
        resolve(this)
      }
    })
    req.open(method, url.toString())
    if (headers) {
      for (const name of Object.keys(headers)) {
        req.setRequestHeader(name, headers[name])
      }
    }
    req.send(body)
  })
}

export function HasStatus (expectedStatus, expectedMessage) {
  return function (resp) {
    if (resp.status !== expectedStatus) {
      throw new Error('Expected status ' + expectedStatus + ', got ' + resp.status)
    }
    if (resp.statusText !== expectedMessage) {
      throw new Error('Expected status text ' + expectedMessage + ', got ' + resp.statusText)
    }
    return resp
  }
}

export function HasHeaders (headers) {
  if (headers instanceof Array) {
    return function (resp) {
      const missing = headers.filter((h) => !resp.getResponseHeader(h))
      if (missing.length) {
        throw new Error('Missing expected headers ' + JSON.stringify(missing) + ' in response: ' + resp.getAllResponseHeaders())
      }
      return resp
    }
  } else {
    return function (resp) {
      const names = Object.keys(headers)
      const missing = names.filter((h) => !resp.getResponseHeader(h))
      if (missing.length) {
        throw new Error('Missing expected headers ' + JSON.stringify(missing) + ' in response: ' + resp.getAllResponseHeaders())
      }
      for (const name of names) {
        const value = resp.getResponseHeader(name)
        if (name === 'Etag') {
          // special case for Etag which may or may not be quoted
          if ((value !== headers[name]) && (value !== "\"" + headers[name] + "\"")) {
            throw new Error('Expected header ' + name + ' to have value ' + headers[name] + ', got ' + value)
          }
        }
        else if (value !== headers[name]) {
          throw new Error('Expected header ' + name + ' to have value ' + headers[name] + ', got ' + value)
        }
      }
      return resp
    }
  }
}

export function HasCommonResponseHeaders (resp) {
  // These appear in most *all* responses, but have unpredictable values
  HasHeaders([
    'Last-Modified',
    'X-Openstack-Request-Id',
    'X-Timestamp',
    'X-Trans-Id',
    'Content-Type'
  ])(resp)
  // Save that trans-id and request-id are the same thing
  if (resp.getResponseHeader('X-Trans-Id') !== resp.getResponseHeader('X-Openstack-Request-Id')) {
    throw new Error('Expected X-Trans-Id and X-Openstack-Request-Id to match; got ' + resp.getAllResponseHeaders())
  }
  // These appear in most responses, but *aren't* (currently) exposed via CORS
  DoesNotHaveHeaders([
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
  ])(resp)
  return resp
}

export function DoesNotHaveHeaders (headers) {
  return function (resp) {
    const found = headers.filter((h) => resp.getResponseHeader(h))
    if (found.length) {
      throw new Error('Found unexpected headers ' + found + ' in response: ' + resp.getAllResponseHeaders())
    }
    return resp
  }
}

export function HasNoBody (resp) {
  if (resp.responseText !== '') {
    throw new Error('Expected no response body; got ' + resp.responseText)
  }
  return resp
}

export function BodyHasLength (expectedLength) {
  return (resp) => {
    if (resp.responseText.length !== expectedLength) {
      throw new Error('Expected body to have length ' + expectedLength + ', got ' + resp.responseText.length)
    }
    return resp
  }
}

export function CorsBlocked (resp) {
  // Yeah, there's *nothing* useful here -- gotta look at the browser's console if you want to see what happened
  HasStatus(0, '')(resp)
  const allHeaders = resp.getAllResponseHeaders()
  if (allHeaders !== '') {
    throw new Error('Expected no headers; got ' + allHeaders)
  }
  HasNoBody(resp)
  return resp
}

function _denial (status, text) {
  function Denial (resp) {
    HasStatus(status, text)(resp)
    const prefix = '<html><h1>' + text + '</h1>'
    if (!resp.responseText.startsWith(prefix)) {
      throw new Error('Expected body to start with ' + JSON.stringify(prefix) + '; got ' + JSON.stringify(resp.responseText))
    }

    HasHeaders({ 'Content-Type': 'text/html; charset=UTF-8' })(resp)
    HasHeaders([
      'X-Openstack-Request-Id',
      'X-Trans-Id',
      'Content-Type'
    ])(resp)
    if (resp.getResponseHeader('X-Trans-Id') !== resp.getResponseHeader('X-Openstack-Request-Id')) {
      throw new Error('Expected X-Trans-Id and X-Openstack-Request-Id to match; got ' + resp.getAllResponseHeaders())
    }
    DoesNotHaveHeaders([
      'X-Account-Bytes-Used',
      'X-Account-Container-Count',
      'X-Account-Object-Count',
      'X-Container-Bytes-Used',
      'X-Container-Object-Count',
      'Etag',
      'X-Object-Meta-Mtime',
      'Last-Modified',
      'X-Timestamp',
      'Accept-Ranges',
      'Access-Control-Allow-Origin',
      'Access-Control-Expose-Headers',
      'Date',
      // Hmmm....
      'Content-Range'
    ])(resp)
    return resp
  }
  return Denial
}
export const Unauthorized = _denial(401, 'Unauthorized')
export const NotFound = _denial(404, 'Not Found')

const $new = document.createElement.bind(document)

export function Skip (msg) {
  this.message = msg
}
Skip.prototype = new Error()

const testPromises = []
export function runTests (prefix, tests) {
  for (let i = 0; i < tests.length; ++i) {
    const [name, test] = tests[i]
    const fullName = prefix + ' - ' + name
    if ('test' in PARAMS && PARAMS['test'] !== fullName) {
      continue
    }
    const row = document.getElementById('results').appendChild($new('tr'))
    row.appendChild($new('td')).textContent = 'Queued'
    row.appendChild($new('td')).textContent = fullName
    row.appendChild($new('td'))
    testPromises.push(
      test().then((resp) => {
        row.childNodes[0].className = 'pass'
        row.childNodes[0].textContent = 'PASS'
      }).catch((reason) => {
        if (reason instanceof Skip) {
          row.childNodes[0].className = 'skip'
          row.childNodes[0].textContent = 'SKIP'
          row.childNodes[2].textContent = reason.message
        } else {
          row.childNodes[0].className = 'fail'
          row.childNodes[0].textContent = 'FAIL'
          row.childNodes[2].textContent = reason.message || reason
          if (reason.stack) {
            row.childNodes[2].textContent += '\n' + reason.stack
          }
          throw reason
        }
      })
    )
  }
}

window.addEventListener('load', function () {
  document.getElementById('status').textContent = 'Waiting for all ' + testPromises.length + ' tests to finish...'
  // Poor-man's version of something approximating
  //    Promise.allSettled(testPromises).then((results) => {
  // for Firefox < 71, Chrome < 76, etc.
  Promise.all(testPromises.map(x => x.then((x) => x, (x) => x))).then(() => {
    const resultTable = document.getElementById('results')
    document.getElementById('status').textContent = (
      'Complete.' +
      ' TESTS: ' + resultTable.childNodes.length +
      ' PASS: ' + resultTable.querySelectorAll('.pass').length +
      ' FAIL: ' + resultTable.querySelectorAll('.fail').length +
      ' SKIP: ' + resultTable.querySelectorAll('.skip').length
    )
  })
})
