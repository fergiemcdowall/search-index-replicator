const JSONStream = require('JSONStream')
const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const levelup = require('levelup')
const util = require('util')
const zlib = require('zlib')

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

    // TODO:
    // Make it possible to either MERGE or REPLACE
    Replicator.DBWriteStream = function (ops) {
      function DBWriteCleanStream () {
        this.batchSize = 1000
        this.currentBatch = []
        Transform.call(this, { objectMode: true })
      }
      function DBWriteMergeStream () {
        Transform.call(this, { objectMode: true })
      }
      ops = _defaults(ops || {}, {merge: true})
      if (ops.merge === true) {
        util.inherits(DBWriteMergeStream, Transform)
        DBWriteMergeStream.prototype._transform = function (data, encoding, end) {
          options.indexes.get(data.key, function (err, val) {
            if (err) {
              if (err.notFound) {
                // do something with this error
                err.notFound
                options.indexes.put(data.key, data.value, function (err) {
                  // do something with this error
                  err
                  end()
                })
              }
            } else {
              var newVal
              if (data.key.substring(0, 3) === 'TF￮') {
                newVal = data.value.concat(val).sort(function (a, b) {
                  return b[0] - a[0]
                })
              } else if (data.key.substring(0, 3) === 'DF￮') {
                newVal = data.value.concat(val).sort()
              } else if (data.key === 'DOCUMENT-COUNT') {
                newVal = (+val) + (+data.value)
              } else {
                newVal = data.value
              }
              options.indexes.put(data.key, newVal, function (err) {
                // do something with this err
                err
                end()
              })
            }
          })
        }
        return new DBWriteMergeStream()
      } else {
        util.inherits(DBWriteCleanStream, Transform)
        DBWriteCleanStream.prototype._transform = function (data, encoding, end) {
          var that = this
          this.currentBatch.push(data)
          if (this.currentBatch.length % this.batchSize === 0) {
            options.indexes.batch(this.currentBatch, function (err) {
              // TODO: some nice error handling if things go wrong
              err
              that.push('indexing batch')
              that.currentBatch = [] // reset batch
              end()
            })
          } else {
            end()
          }
        }
        DBWriteCleanStream.prototype._flush = function (end) {
          var that = this
          options.indexes.batch(this.currentBatch, function (err) {
            // TODO: some nice error handling if things go wrong
            err
            that.push('remaining data indexed')
            end()
          })
        }
        return new DBWriteCleanStream()
      }
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
