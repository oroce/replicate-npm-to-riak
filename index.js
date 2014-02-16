var host = process.env.RIAK_HOST||"192.168.1.254"
var db = require( "riak-js" ).getClient({host: host });
var sync = require( "couchdb-sync" );
var async = require( "async" );
var targetURL = process.env.TARGET_URL||"http://registry.oroszi.net:8008";
var registryUrl = process.env.REGISTRY_URL||"https://fullfatdb.npmjs.com/registry";
var maxQueueLen = 1000;
var amqp = require( "amqp" );
var connection = amqp.createConnection({url: process.env.AMQP_URL});
require( "http" ).globalAgent.maxSockets = Infinity;

var onError = function( prefix ){
  var args = [].slice.call( arguments, 1 );
  return function( err ){
    if( err ){
      console.error.apply( console, [ "[" + prefix + "]" ].concat( args ).concat( [].slice.call( arguments ) ) );
    }
  };
};

var start = function(){
  console.log( "starting replication" );
  db.getBucket( "docs", function( err, bucket ){
    if( err ){
      throw err;
    }

    var seq = process.env.SEQ || bucket.seq || 0;
    var queue = async.queue(function( doc, done ){

      if( !doc.versions ){
        console.warn( "wtf it has no versinos: ", doc );
        return done();
      }
      Object.keys( doc.versions||{} ).map(function( version ){
        var obj = doc.versions[ version ];
        var currentTarball = obj.dist.tarball;
        var fullName = obj.name + "-" + obj.version + ".tgz"
        doc.versions[ version ].dist.tarball = targetURL + "/riak/attachments/" + fullName;
        connection.publish( "npm-download", {
          id: fullName,
          url: currentTarball
        }, {
          contentType: "application/json"
        }, console.log.bind(console, 'its published'));
      });

      db.save( "docs", doc._id, doc, {}, done );

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
        var push = function(){
          queue.push( data.doc, function( err ){
            console.log( "queue length is: ", queue.length() );
            if( err ){
              process.nextTick( push );
              return onError( "message handler", data )( err );
            }
          });
        }
        push();
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
};
connection.once( "ready", function(){
  connection.queue( "npm-download", {
    autoDelete: false,
    durable: true,
  }, function( q ){
    q.bind( "#" );
    start();
  });
}).on( "error", console.error.bind( console, "amqp error") );