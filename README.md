# Replicate npm to riak

Proof of concept project, NPM without CouchDB. :)

# Run

`REGISTRY_URL="npm registry url" TARGET_URL="your registry's frontend url" SEQ="for overwriting seq number" RIAK_HOST="your riak host's url" node index.js`

# Notes (2014-02-16)

I've started to use rabbitmq for messages, currently the downloads go to rabbitmq