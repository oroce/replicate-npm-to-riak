/* sample job
{
  "id": "debug-0.0.1.tar.gz",
  "url": "http://registry.npmjs.org/debug/-/debug-0.0.1.tgz"
}
Big file:
{
  "id": "tilemill-0.8.0.tar.gz"
  "url":"http://registry.npmjs.org/tilemill/-/tilemill-0.8.0.tgz"
}
*/
var amqp = require( "amqp" );
var connection = amqp.createConnection({ url: process.env.AMQP_URL });
var workerNums = process.env.FORKS||require("os").cpus().length;
var cluster = require( "cluster" );
var host = process.env.RIAK_HOST||"192.168.1.254"
var knox = require( "knox" );
var http = require( "http" );
var https = require( "https" );
var db = require( "riak-js" ).getClient({host: host});
var client = knox.createClient({
  key: process.env.KEY,
  secret: process.env.SECRET,
  bucket: "attachments",
  endpoint: process.env.ENDPOINT||"127.0.0.1",
  secure: false,
  port: process.env.S3_PORT||8084,
  style: "path"
});
var url = require("url");
var request = require( "request" );
var util = require( "util" );
function onMessage( message, headers, deliveryInfo, job ){
  
  //console.log( "job arrived", message.id, message.url );
  var cb = function( err ){
    if( err ){
      console.error( "JOB ERROR", message, err );
      job.reject( true );
      return;
    }
    console.log( "Job is finished: %s(%s)", message.id, message.url );
    job.acknowledge();
  }

  /*var s = request({
    url: message.url,
    encoding: null
  });
  s.on( "error", function( err ){
    return cb( err );
  });
  db.save( "attachments", message.id, s, {
    contentType: "application/octet-stream"
  },function(){
    console.log.apply( console, arguments );
  })*/
  /*var parts = url.parse( message.url ).pathname.split("/"); 
  var cdnUrl = ["http://registry.npmjs.org", parts[2], "-", parts[3] ].join( "/" );
  
  console.log( "%s transformed to %s", message.url, cdnUrl );*/
  var m = (/^https/).test( message.url ) ? https : http;
  var port = (/^https/).test( message.url ) ? 443 : 80;
  var uri = url.parse( message.url );
  delete uri.host;
  delete uri.href;
  uri.port = port;
  message.newUrl = url.format( uri );
  m.get( message.newUrl, function( res ){
    res.on( "error", function( err ){
      if( err ){
        console.log( "response error:", message );
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
    console.log( "http.get error", err );
    cb( err );
  });

}

function bindJob(){
  connection.queue( "npm-download", {
    autoDelete: false,
    durable: true,
  },function( q ){
    q.subscribe({
      ack: true,
      prefetchCount: 1
    }, onMessage );
  })
};


function start(){
  if( cluster.isMaster ){
    console.log( "starting master" );
    
    for( var i = 0; i < workerNums; i++ ){
      cluster.fork();
    }
    cluster.on( "exit", function(worker, code, signal) {
      console.log("worker " + worker.process.pid + " died with code %s because of signal: %s", code, signal );
    });
  }
  else{
    console.log( "starting worker: %s", process.pid );
    bindJob();
  }

}

connection.once( "ready", function(){
  start();
}).on( "error", console.error.bind( console, "amqp-error") );


