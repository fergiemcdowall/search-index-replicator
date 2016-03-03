const fs = require('fs')
const JSONStream = require('JSONStream')
const tape = require('tape')
const sandbox = 'test/sandbox'
const zlib = require('zlib')

var replicator

tape('initialise', function (t) {
  t.plan(1)
  require('../')({indexPath: sandbox + '/replicate-test'}, function (err, thisReplicator) {
    t.error(err)
    replicator = thisReplicator
  })
})

tape('simple write to replicator', function (t) {
  t.plan(2)
  fs.createReadStream('test/instructions.json')
    .pipe(JSONStream.parse())
    .pipe(replicator.writeStream())
    .on('close', function (err) {
      t.error(err)
      t.ok(true)
    })
})

tape('simple read from replicator', function (t) {
  t.plan(3)
  var arr = []
  replicator.readStream()
    .on('data', function (data) {
      arr.push(data)
    })
    .on('close', function (err) {
      t.error(err)
      t.equal(arr.length, 5)
      t.equal(arr[2].key, 'foo2')
    })
})

tape('get a gz snapshot and write it to a file', function (t) {
  t.plan(3)
  var arr = []
  replicator.gzReadStream()
    .pipe(fs.createWriteStream(sandbox + '/out.gz'))
    .on('close', function () {
      fs.createReadStream(sandbox + '/out.gz')
        .pipe(zlib.createGunzip())
        .pipe(JSONStream.parse())
        .on('data', function (data) {
          arr.push(data)
        })
        .on('close', function (err) {
          t.error(err)
          t.equal(arr.length, 5)
          t.equal(arr[2].key, 'foo2')
        })
    })
})

tape('write a gz snapshot to a new index', function (t) {
  t.plan(5)
  require('../')({indexPath: sandbox + '/replicate-test-two'}, function (err, newReplicator) {
    t.error(err)
    newReplicator.replicateFromSnapShotStream(fs.createReadStream(sandbox + '/out.gz'), function (errr) {
      t.error(errr)
      var arr = []
      newReplicator.readStream()
        .on('data', function (data) {
          arr.push(data)
        })
        .on('close', function (err) {
          t.error(err)
          t.equal(arr.length, 5)
          t.equal(arr[2].key, 'foo2')
        })
    })
  })
})
