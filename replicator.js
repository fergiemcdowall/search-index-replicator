const levelws = require('level-ws')
const JSONStream = require('JSONStream')
const zlib = require('zlib')
const levelup = require('levelup')
const _defaults = require('lodash.defaults')

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

    var Replicator = {}

    Replicator.DBReadStream = function (ops) {
      ops = _defaults(ops || {}, {gzip: false})
      if (ops.gzip) {
        return options.indexes.createReadStream()
          .pipe(JSONStream.stringify('', '\n', ''))
          .pipe(zlib.createGzip())
      } else {
        return options.indexes.createReadStream()
      }
    }

    Replicator.DBWriteStream = function () {
      var indexesws = levelws(options.indexes)
      return indexesws.createWriteStream()
    }

    Replicator.close = function (callback) {
      options.indexes.close(function (err) {
        while (!options.indexes.isClosed()) {
          // closeing
        }
        if (options.indexes.isClosed()) {
          // closed
          callback(err)
        }
      })
    }

    return callback(err, Replicator)
  })
}
