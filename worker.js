var amqp = require( "amqp" );
var connection = amqp.createConnection({ url: process.env.AMQP_URL });
var workerNums = process.env.FORKS||require("os").cpus().length;
var cluster = require( "cluster" );
var host = process.env.RIAK_HOST||"192.168.1.254"
var db = require( "riak-js" ).getClient({host: host });
var request = require( "request" );
function onMessage( message, headers, deliveryInfo, job ){
  console.log( "job arrived", message.id, message.url );
  var cb = function( err ){
    if( err ){
      console.error( "JOB ERROR", message, err );
      job.reject( true );
      return;
    }
    console.log( "Job is finished: %s(%s)", message.id, message.url );
    job.acknowledge();
  }

  request({
    url: message.url,
    encoding: null
  }, function( err, _, body ){
    if( err ){
      return cb( err );
    }
    db.save( "attachments", message.id, body, {
      contentType: "application/octet-stream"
    }, cb );
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
});


