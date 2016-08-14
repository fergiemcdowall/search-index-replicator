const JSONStream = require('JSONStream')
const SearchIndex = require('search-index')
const fs = require('fs')
const sandbox = process.env.SANDBOX || 'test/sandbox'
const test = require('tape')
const zlib = require('zlib')

const indexAddress = sandbox + '/replicate-test'

var replicator, replicatorTarget, replicatorTarget2

test('make a small search index', function (t) {
  t.plan(14)
  SearchIndex({
    indexPath: indexAddress
  }, function (err, si) {
    t.error(err)
    const filePath = './node_modules/reuters-21578-json/data/fullFileStream/justTen.str'
    fs.createReadStream(filePath)
      .pipe(JSONStream.parse())
      .pipe(si.createWriteStream())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        t.ok(true, ' stream ended')
        si.close(function (err) {
          t.error(err, ' index closed')
        })
      })
  })
})

test('initialise', function (t) {
  t.plan(1)
  require('../')({indexPath: sandbox + '/replicate-test'}, function (err, thisReplicator) {
    t.error(err)
    replicator = thisReplicator
  })
})

test('simple read from replicator (no ops)', function (t) {
  t.plan(1)
  var i = 0
  replicator.DBReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 2944)
    })
})

test('simple read from replicator (gzip: false)', function (t) {
  t.plan(1)
  var i = 0
  replicator.DBReadStream({gzip: false})
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 2944)
    })
})

test('simple read from replicator (gzip: true)', function (t) {
  t.plan(1)
  var i = 0
  replicator.DBReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 2944)
    })
})

test('initialise replication target', function (t) {
  t.plan(1)
  require('../')({indexPath: sandbox + '/replicate-test-target'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget = thisReplicator
  })
})

test('simple replication from one index to another', function (t) {
  t.plan(4)
  replicator.DBReadStream()
    .pipe(replicatorTarget.DBWriteStream({ merge: false }))
    .on('data', function (data) {
      t.ok(true, 'data event received')
    })
    .on('end', function () {
      t.ok(true, 'stream closed')
    })
})

test('simple read from replicated index (no ops)', function (t) {
  t.plan(1)
  var i = 0
  replicatorTarget.DBReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 2944)
    })
})

test('initialise replication target2', function (t) {
  t.plan(1)
  require('../')({indexPath: sandbox + '/replicate-test-target2'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget2 = thisReplicator
  })
})

test('gzipped replication from one index to another', function (t) {
  t.plan(1)
  replicator.DBReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .pipe(replicatorTarget2.DBWriteStream())
    .on('data', function (data) {
      // data
    })
    .on('end', function () {
      t.ok(true, 'stream closed')
    })
})

test('validate gzip replication', function (t) {
  t.plan(2)
  var i = 0
  replicatorTarget2.DBReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      replicatorTarget2.close(function (err) {
        t.error(err)
        t.equal(i, 2944)
      })
    })
})

test('confirm can search as normal in replicated index', function (t) {
  t.plan(3)
  SearchIndex({
    indexPath: sandbox + '/replicate-test-target2'
  }, function (err, si) {
    t.error(err)
    si.search({
      query: {
        AND: [{'*': ['*']}]
      }
    }, function (err, results) {
      t.error(err)
      t.looseEqual(
        results.hits.map(function (item) { return item.id }),
        [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ]
      )
    })
  })
})
