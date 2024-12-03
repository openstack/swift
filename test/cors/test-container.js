import {
  runTests,
  MakeRequest,
  HasStatus,
  HasHeaders,
  HasCommonResponseHeaders,
  HasNoBody
} from './harness.js'

function CheckJsonListing (resp) {
  HasHeaders({ 'Content-Type': 'application/json; charset=utf-8' })(resp)
  const listing = JSON.parse(resp.responseText)
  for (const item of listing) {
    if ('subdir' in item) {
      if (Object.keys(item).length !== 1) {
        throw new Error('Expected subdir to be the only key, got ' + JSON.stringify(item))
      }
      continue
    }
    const missing = ['name', 'bytes', 'content_type', 'hash', 'last_modified'].filter((key) => !(key in item))
    if (missing.length) {
      throw new Error('Listing item is missing expected keys ' + JSON.stringify(missing) + '; got ' + JSON.stringify(item))
    }
  }
  return listing
}

function HasStatus200Or204 (resp) {
  if (resp.status === 200) {
    // NB: some browsers (like chrome) may serve HEADs from cached GETs, leading to the 200
    HasStatus(200, 'OK')(resp)
  } else {
    HasStatus(204, 'No Content')(resp)
  }
  return resp
}

const expectedListing = [
  'dlo',
  'dlo-with-unlistable-segments',
  'dlo/seg00',
  'dlo/seg01',
  'dlo/seg02',
  'dlo/seg03',
  'dlo/seg04',
  'dlo/seg05',
  'dlo/seg06',
  'dlo/seg07',
  'dlo/seg08',
  'dlo/seg09',
  'obj',
  'slo',
  'symlink-to-private',
  'symlink-to-public-no-cors',
  'symlink-to-public-with-cors'
]
const expectedWithDelimiter = [
  'dlo',
  'dlo-with-unlistable-segments',
  'dlo/',
  'obj',
  'slo',
  'symlink-to-private',
  'symlink-to-public-no-cors',
  'symlink-to-public-with-cors'
]

runTests('container', [
  ['GET format=plain',
    () => MakeRequest('GET', 'public-with-cors', {}, '', {'format': 'plain'})
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(HasHeaders({ 'Content-Type': 'text/plain; charset=utf-8' }))
      .then((resp) => {
        const names = resp.responseText.split('\n')
        if (!(names.length === expectedListing.length + 1 && names.every((name, i) => name === (i === expectedListing.length ? '' : expectedListing[i])))) {
          throw new Error('Expected listing to have items ' + JSON.stringify(expectedListing) + '; got ' + JSON.stringify(names))
        }
      })],
  ['GET format=json',
    () => MakeRequest('GET', 'public-with-cors', {}, '', {'format': 'json'})
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(CheckJsonListing)
      .then((listing) => {
        const names = listing.map((item) => 'subdir' in item ? item.subdir : item.name)
        if (!(names.length === expectedListing.length && names.every((name, i) => expectedListing[i] === name))) {
          throw new Error('Expected listing to have items ' + JSON.stringify(expectedListing) + '; got ' + JSON.stringify(names))
        }
      })],
  ['GET format=json&delimiter=/',
    () => MakeRequest('GET', 'public-with-cors', {}, '', {'format': 'json', 'delimiter': '/'})
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(CheckJsonListing)
      .then((listing) => {
        const names = listing.map((item) => 'subdir' in item ? item.subdir : item.name)
        if (!(names.length === expectedWithDelimiter.length && names.every((name, i) => expectedWithDelimiter[i] === name))) {
          throw new Error('Expected listing to have items ' + JSON.stringify(expectedWithDelimiter) + '; got ' + JSON.stringify(names))
        }
      })],
  ['GET format=xml',
    () => MakeRequest('GET', 'public-with-cors', {}, '', {'format': 'xml'})
      .then(HasStatus(200, 'OK'))
      .then(HasHeaders({ 'Content-Type': 'application/xml; charset=utf-8' }))
      .then((resp) => {
        const prefix = '<?xml version="1.0" encoding="UTF-8"?>\n<container name="public-with-cors">'
        if (resp.responseText.substr(0, prefix.length) !== prefix) {
          throw new Error('Expected response to start with ' + JSON.stringify(prefix) + '; got ' + resp.responseText)
        }
      })],
  ['GET Accept: json',
    () => MakeRequest('GET', 'public-with-cors', { Accept: 'application/json' })
      .then(HasStatus(200, 'OK'))
      .then(HasCommonResponseHeaders)
      .then(CheckJsonListing)
      .then((listing) => {
        if (listing.length !== 17) {
          throw new Error('Expected exactly 17 items in listing; got ' + listing.length)
        }
      })],
  ['GET Accept: xml',
    // NB: flakey on Safari -- sometimes it serves JSON from cache, *even with* a Vary: Accept header
    () => MakeRequest('GET', 'public-with-cors', { Accept: 'application/xml' })
      .then(HasStatus(200, 'OK'))
      .then(HasHeaders({ 'Content-Type': 'application/xml; charset=utf-8' }))
      .then((resp) => {
        const prefix = '<?xml version="1.0" encoding="UTF-8"?>\n<container name="public-with-cors">'
        if (resp.responseText.substr(0, prefix.length) !== prefix) {
          throw new Error('Expected response to start with ' + JSON.stringify(prefix) + '; got ' + resp.responseText)
        }
      })],
  ['HEAD format=plain',
    () => MakeRequest('HEAD', 'public-with-cors', {}, '', {'format': 'plain'})
      .then(HasStatus200Or204)
      .then(HasHeaders({ 'Content-Type': 'text/plain; charset=utf-8' }))
      .then(HasNoBody)],
  ['HEAD format=json',
    () => MakeRequest('HEAD', 'public-with-cors', {}, '', {'format': 'json'})
      .then(HasStatus200Or204)
      .then(HasHeaders({ 'Content-Type': 'application/json; charset=utf-8' }))
      .then(HasNoBody)],
  ['HEAD format=xml',
    () => MakeRequest('HEAD', 'public-with-cors', {}, '', {'format': 'xml'})
      .then(HasStatus200Or204)
      .then(HasHeaders({ 'Content-Type': 'application/xml; charset=utf-8' }))
      .then(HasNoBody)]
])
