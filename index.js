var host = process.env.RIAK_HOST||"192.168.1.254"
var db = require( "riak-js" ).getClient({host: host });
var sync = require( "couchdb-sync" );
var async = require( "async" );
var targetURL = process.env.TARGET_URL||"http://registry.oroszi.net:8008";
var request = require( "request" );
var registryUrl = process.env.REGISTRY_URL||"https://fullfatdb.npmjs.com/registry";
var maxQueueLen = 1000;
require( "http" ).globalAgent.maxSockets = Infinity;

var onError = function( prefix ){
  var args = [].slice.call( arguments, 1 );
  return function( err ){
    if( err ){
      console.error.apply( console, [ "[" + prefix + "]" ].concat( args ).concat( [].slice.call( arguments ) ) );
    }
  };
};


db.getBucket( "docs", function( err, bucket ){
  if( err ){
    throw err;
  }
  var seq = process.env.SEQ || bucket.seq || 0;
  var queue = async.queue(function( doc, done ){

    if( !doc.versions ){
      console.warn( "wtf it has no versinos: ", doc );
    }
    var files = Object.keys( doc.versions||{} ).map(function( version ){
      var obj = doc.versions[ version ];
      var currentTarball = obj.dist.tarball;
      var fullName = obj.name + "-" + obj.version + ".tgz"
      doc.versions[ version ].dist.tarball = targetURL + "/riak/attachments/" + fullName;
      return {
        name: fullName,
        url: currentTarball
      };
    });


    async.each( files, function( obj, cb ){
      request({
        url: obj.url,
        encoding: null
      }, function( err, _, body ){
        if( err ){
          return cb( err );
        }
        db.save( "attachments", obj.name, body, {
          contentType: "application/octet-stream"
        }, cb );
      });
    }, function( err ){
      if( err ){
        return done( err );
      }
      db.save( "docs", doc._id, doc, {}, done );
    });
  }, 10);

  queue.drain = function(){
    console.log( "resuming syncer" );
    syncer.feed.resume();
  }

  console.log( "start running on %s with seq: %s", registryUrl, seq );
  var syncer = sync( registryUrl, seq );
  var i = 0;

  syncer
    .on( "progress", function( val ){
      console.log( "progress: %s%, queue length: %s, docs: %s", (val * 100 ).toFixed(2), queue.length(), ++i );
    })
    .on( "data", function( data ){
      if( data.doc._deleted === true ){
        // we don't sync deleted packages
        return;
      }
      if( !data.doc.versions ){
        // wtf is this? in npm's couchdb there are some weird shit
        return;
      }
      queue.push( data.doc, onError( "message handler", data ) )
      if( queue.length() > maxQueueLen ){
        console.log( "queue length is more than %d, pausing", maxQueueLen );
        syncer.feed.pause();
      }
    })
    .on( "max", function( newSeq ){
      db.saveBucket( "docs", {
        seq: newSeq
      });
    })
    .on( "error", onError( "syncer", syncer ) );
});