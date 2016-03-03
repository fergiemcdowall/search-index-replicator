const levelws = require('level-ws')
const JSONStream = require('JSONStream')
const zlib = require('zlib')
const levelup = require('levelup')

var getOptions = function (givenOptions, callback) {
  if (!givenOptions.indexes) {
    levelup(givenOptions.indexPath || 'si', {
      valueEncoding: 'json'
    }, function (err, db) {
      givenOptions.indexes = db
      callback(err, givenOptions)
    })
  } else {
    callback(null, givenOptions)
  }
}

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    if (err) {
      return callback(err, null)
    }

    var indexes = options.indexes
    var Replicator = {}

    Replicator.readStream = function () {
      return indexes.createReadStream()
    }

    Replicator.writeStream = function () {
      var indexesws = levelws(indexes)
      return indexesws.createWriteStream()
    }

    Replicator.gzReadStream = function () {
      return indexes.createReadStream()
        .pipe(JSONStream.stringify('', '\n', ''))
        .pipe(zlib.createGzip())
    }

    Replicator.gzWriteStream = Replicator.replicateFromSnapShotStream
    Replicator.replicateFromSnapShot = Replicator.replicateFromSnapShotStream

    // ReplicateFromStream
    Replicator.replicateFromSnapShotStream = function (readStream, callback) {
      var indexesws = levelws(indexes)
      readStream.pipe(zlib.createGunzip())
        .pipe(JSONStream.parse())
        .pipe(indexesws.createWriteStream())
        .on('close', callback)
        .on('error', callback)
    }

    // createSnapShotForStream
    Replicator.createSnapShot = function (callback) {
      callback(indexes.createReadStream()
        .pipe(JSONStream.stringify('', '\n', ''))
        .pipe(zlib.createGzip())
      )
    }

    return callback(err, Replicator)
  })
}
