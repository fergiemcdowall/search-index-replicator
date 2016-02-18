var levelws = require('level-ws');
var JSONStream = require('JSONStream');
var skeleton = require('log-skeleton');
var zlib = require('zlib');

module.exports = function (givenOptions, callback) {

  getOptions(givenOptions, function(err, options) {

    var indexes = options.indexes;
    var log = skeleton((options) ? options.log : undefined);
    var Replicator = {};

    //ReplicateFromStream
    Replicator.replicateFromSnapShotStream = function (readStream, callback) {
      var indexesws = levelws(indexes);
      readStream.pipe(zlib.createGunzip())
        .pipe(JSONStream.parse().on('error', callback))
        .pipe(indexesws.createWriteStream())
        .on('close', callback)
        .on('error', function(err) {
          console.log(err)
        });
    };

    //ReplicateFromBatch
    Replicator.replicateFromSnapShotBatch = function (serializedDB, callback) {
      for (var i = 0; i < serializedDB.length; i++)
        serializedDB[i].type = 'put';
      indexes.batch(serializedDB, function (err) {
        if (err) return log.warn('Ooops!', err);
        log.info('Great success dear leader!');
        callback(err);
      });
    };

    //createSnapShotForStream
    Replicator.createSnapShot = function (callback) {
      callback(indexes.createReadStream()
               .pipe(JSONStream.stringify('', '\n', ''))
               .pipe(zlib.createGzip())
              );
    };

    //createSnapShotForBatch
    Replicator.createSnapShotBatch = function (callback) {
      //has to be like this in order for norch snapshotting to work
      callback
      (indexes.createReadStream()
       .pipe(JSONStream.stringify('[', ',', ']'))
       .pipe(zlib.createGzip())
      );
    };
    return callback(null, Replicator)
  })
}

var getOptions = function(givenOptions, callbacky) {
  const _ = require('lodash')
  const async = require('async')
  const bunyan = require('bunyan')
  const levelup = require('levelup')
  const leveldown = require('leveldown')
  const tv = require('term-vector')
  givenOptions = givenOptions || {}
  async.parallel([
    function(callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.fieldsToStore = 'all'
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = tv.getStopwords('en').sort()
      defaultOps.log = bunyan.createLogger({
        name: 'search-index',
        level: givenOptions.logLevel || defaultOps.logLevel
      })
      callback(null, defaultOps)
    },
    function(callback){
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function(err, db) {
          callback(null, db)          
        })
      }
      else {
        callback(null, null)
      }
    }
  ], function(err, results){
    var options = _.defaults(givenOptions, results[0])
    if (results[1] != null) {
      options = _.defaults(options, results[1])
    }
    return callbacky(err, options)
  })
}

