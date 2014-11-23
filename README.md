gcloud-promise
==============

A promises based node interface to Google cloud services.

Currently, support is implemented for 
 - Google Cloud Storage and
 - Google Datastore.

Google provides a gcloud, a node binding which is similar in spirit.
This project grew out of a need/desire to use a promises based interface.
The code is much smaller, and IMHO, much easier to understand. YMMV.


## Installation
Simply,

    npm install gcloud-promise

## Get gcloud credentials
Follow instructions at:
   https://cloud.google.com/datastore/docs/activate

The Google cloud API provides 2 mechanisms of access:
1. From Google Compute Engine
2. From other platforms (like localhost, AWS etc).

At this point, only #2 has been tested with this library.
 - create a service account,
 - download the credentials to project_keyFile.json
   (keep this away from the main code directory and never
    commit it to the same VCS as your code)

## Setup environment variable to track the key file.

    export GCLOUD_KEYFILE=/path/to/project_keyFile.json

## Library initialization

    var acct = {
            keyFilename: process.env.GCLOUD_KEYFILE,
            project_id: ‘myProject’,
            cust_id: ‘cust1’
        },
        gcloud = require(‘gcloud-promise’)(acct.keyFilename);


## Google cloud storage
    var ds = gcloud.datastore(acct.project_id),
        data = [{
            name: ‘entity1’,
            prop1: ‘just a prop’,
            prop2: 23,
            prop3: 42.1,
            prop4: true
        }, {
            name: ‘entity2’,
            prop1: ‘just a prop again’,
            prop2: 76,
            prop3: 3.147,
            prop4: false
        }]
    ds.transaction().then(function(transaction) {
        /* Please understand datastore keys and entities.
           This loop converts a simple javascript object array
           into an entities array that the datastore can save, query and fetch
        */
        var entities = data.reduce(function(acc, datum) {
            var key = ds.key(acct.cust_id, [[‘EntityKind’, datum.name]]);
            acc.push(ds.entity.toProto(datum, key));
        });
        transaction
          .upsert(entities)
          .commit()
          .then(function(result) {
            if (result.statusCode === 200) {
                console.log(‘Commit succeeded:’ + result.statusCode + ‘ ‘ + JSON.stringify(result.body));
            } else {
                throw(‘Commit failure ‘ + result.statusCode + ‘ ‘ + JSON.stringify(result.errors));
            }
          });
    }).catch(function(err) {
        throw(‘Commit failed:’ + err);
    })
    ds.gql().



## Google storage
    var stor = gcloud.storage(acct.project_id),
        test = {
            bucket_name: acct.project_id + ‘-test-bucket-123456’,
            file_name: acct.project_id + ‘-test-file-123456’,
            data: ‘{“key1”: “val1”, “key2”: “val2”}’
        };
    stor.bucket.create(test.bucket_name).then(function(d){
        console.log('\n -> created bucket:' + JSON.stringify(d));
    }).then(function() {
        return stor.bucket.get(test.bucket_name).then(function(d){
            console.log('\n -> get bucket:' + JSON.stringify(d));
        });
    }).then(function(){
        return stor.bucket.list().then(function(d) {
            console.log('\n -> list buckets:' + JSON.stringify(d));
        });
    }).then(function() {
        return stor.file.create(test.bucket_name, test.file_name, test.data).then(function(d) {
            console.log('\n -> create file:' + JSON.stringify(d));
        });
    }).then(function() {
        return stor.file.create(test.bucket_name, test.file_name, test.data).then(function(d) {
            console.log('\n -> create2 file:' + JSON.stringify(d));
        });
    }).then(function() {
        return stor.file.get(test.bucket_name, test.file_name).then(function(d) {
            console.log('\n -> get file:' + JSON.stringify(d));
        });
    }).then(function() {
        return stor.file.delete(test.bucket_name, test.file_name).then(function(d) {
            console.log('\n -> delete file:' + JSON.stringify(d));
        });
    }).then(function() {
        return stor.bucket.delete(test.bucket_name).then(function(d) {
            console.log('\n -> deleted bucket:' + JSON.stringify(d));
        });
    });
