var levelws = require('level-ws');
var JSONStream = require('JSONStream');
var skeleton = require('log-skeleton');
var zlib = require('zlib');

module.exports = function (options) {
  var indexes = options.indexes;
  var log = skeleton((options) ? options.log : undefined);
  var replicator = {};

  //ReplicateFromStream
  replicator.replicateFromSnapShotStream = function (readStream, callback) {
    var indexesws = levelws(indexes);
    readStream.pipe(zlib.createGunzip())
      .pipe(JSONStream.parse())
      .pipe(indexesws.createWriteStream())
      .on('close', callback)
      .on('error', function(err) {
        console.log(err)
      });
  };

  //ReplicateFromBatch
  replicator.replicateFromSnapShotBatch = function (serializedDB, callback) {
    for (var i = 0; i < serializedDB.length; i++)
      serializedDB[i].type = 'put';
    indexes.batch(serializedDB, function (err) {
      if (err) return log.warn('Ooops!', err);
      log.info('Great success dear leader!');
      callback(err);
    });
  };

  //createSnapShotForStream
  replicator.createSnapShot = function (callback) {
    callback(indexes.createReadStream()
             .pipe(JSONStream.stringify('', '\n', ''))
             .pipe(zlib.createGzip())
            );
  };

  //createSnapShotForBatch
  replicator.createSnapShotBatch = function (callback) {
    //has to be like this in order for norch snapshotting to work
    callback
    (indexes.createReadStream()
     .pipe(JSONStream.stringify('[', ',', ']'))
     .pipe(zlib.createGzip())
    );
  };
  return replicator;
};
