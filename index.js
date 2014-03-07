require( "http" ).globalAgent.maxSockets = Infinity;
var host = process.env.RIAK_HOST||"192.168.1.254"
var db = require( "riak-js" ).getClient({host: host, api: process.env.RIAK_API });
var sync = require( "couchdb-sync" );
var async = require( "async" );
var targetURL = process.env.TARGET_URL||"http://registry.oroszi.net:8008";
var registryUrl = process.env.REGISTRY_URL||"https://fullfatdb.npmjs.com/registry";
var knox = require( "knox" );
var http = require( "http" );
var https = require( "https" );
var maxQueueLen = 0;
var client = knox.createClient({
  key: process.env.KEY,
  secret: process.env.SECRET,
  bucket: "attachments",
  endpoint: process.env.ENDPOINT||"127.0.0.1",
  secure: false,
  port: process.env.S3_PORT||8084,
  style: "path"
});
var createBar = require( "another-progress-bar" )
var bar = createBar( "syncing from " + registryUrl );
var onError = function( prefix ){
  var args = [].slice.call( arguments, 1 );
  return function( err ){
    if( err ){
      console.error.apply( console, [ "[" + prefix + "]" ].concat( args ).concat( [].slice.call( arguments ) ) );
    }
  };
};
var log = function( line ){
  process.stderr.write( line  + "\n" );
}
var start = function(){
  db.getBucket( "docs", function( err, bucket ){
    if( err ){
      throw err;
    }

    var seq = process.env.SEQ || bucket.seq || 0;
    var queue = async.queue(function( doc, done ){

      if( !doc.versions ){
        log( "wtf it has no versinos: ", doc );
        return done();
      }
      bar.label( doc.name + "(" + Object.keys(doc.versions).length + " versions)");
      var versions = [];
      Object.keys( doc.versions||{} ).map(function( version, i ){
        var obj = doc.versions[ version ];
        var currentTarball = obj.dist.tarball;
        var fullName = obj.name + "-" + obj.version + ".tgz"
        doc.versions[ version ].dist.tarball = targetURL + "/riak/attachments/" + fullName;
        versions.push({
          i: i + 1 ,
          id: fullName,
          url: currentTarball
        });
      });
      var saveVersions = function(err){
        if( err ){
          return done(err);
        }
        async.each( versions, function( message, cb ){
          bar.label( doc.name + "(" + message.i + " of " + versions.length + ")" );
          var put = function(){
            var m = (/^https/).test( message.url ) ? https : http;
            var port = (/^https/).test( message.url ) ? 443 : 80;
            var uri = url.parse( message.url );
            delete uri.host;
            delete uri.href;
            uri.port = port;
            message.newUrl = url.format( uri );
            console.error( "getting " + message.newUrl );
            var req = m.get(message.newUrl, function( res ){
              res.on( "error", function( err ){
                if( err ){
                  console.error( "response error:", message );
                }
                cb( err );
              });
              //console.log( res.headers );
              var k = client.putStream( res, message.id, {
                 "Content-Type": res.headers[ "content-type" ],
                 "Content-Length": res.headers[ "content-length" ],
                 "x-amz-acl": "public-read"
              }, cb );
            }).on( "error", function( err ){
              console.error( "http.get error", err );
              cb( err );
            });
            req.setTimeout(60*1000, function(){
              var err = new Error("ETIMEDOUT")
              err.code = "ETIMEDOUT"
              req.abort();
              cb( err );
            });
          };
          client.headFile( message.id, {
             "accept":"*/*"
          },function( err, res ){
            if( err ){
              return cb( err );
            }
            if( +res.statusCode === 200 ){
             console.error( "we've already got it: %s", message.id );
             return cb();
            }
            put();
          });
        }, done );
      };
      db.save( "docs", doc._id, doc, {}, saveVersions );

    }, 1);

    queue.drain = function(){
      console.error( "resuming syncer" );
      syncer.feed.resume();
    }

    console.error( "start running on %s with seq: %s", registryUrl, seq );
    var syncer = sync( registryUrl, seq );
    var i = 0;

    syncer
      .on( "progress", function( val ){
        //console.log( "progress: %s%, queue length: %s, docs: %s", (val * 100 ).toFixed(2), queue.length(), ++i );
        bar.progress( val*100, 100 );
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
          queue.unshift( data.doc, function( err ){
            console.error( "queue length is: ", queue.length() );
            if( err ){
              process.nextTick( push );
              return onError( "message handler", data )( err );
            }
          });
        }
        push();
        if( queue.length() > maxQueueLen ){
          console.error( "queue length is more than %d, pausing", maxQueueLen );
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
start();