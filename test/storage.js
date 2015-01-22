if (process.env.GCLOUD_KEYFILE === undefined) {
    throw "Environment variable GCLOUD_KEYFILE is not defined";
} else {
    console.log("Using GCLOUD_KEYFILE " + process.env.GCLOUD_KEYFILE);
}

var assert = require("assert"),
    blanket = require("blanket"),
    fs = require('fs'),
    acct = {
        keyFilename: process.env.GCLOUD_KEYFILE,
        project_id: 's~dev-bydesign-2',
        cust_id: 'Company2'
    },
    gcloud = require('../lib/gcloud_promise.js')(acct.keyFilename),
    ds = gcloud.datastore(acct.project_id),
    data = [{
        Symbol: 'ATVI',
        Name: 'Activision Blizzard Inc',
        lastSale: 20.14,
        netChange: 0.24,
        pctChange: 1.21,
        shareVolume: 939808,
        Nasdaq100Points: 0.3
    }, {
        Symbol: 'EBAY',
        Name: 'eBay Inc',
        lastSale: 55.003,
        netChange: 0.16,
        pctChange: 0.33,
        shareVolume: 923661,
        Nasdaq100Points: 0.2
    }];

var project_id = acct.project_id.substring(2),
    storage = gcloud.storage(project_id),
    test_bkt = project_id + '-test-creation-12345',
    test_file = project_id + '-test-file-12345',
    test_data = '{"key1":"key1", "key2":"key2"}';

    console.log(test_bkt);

describe('gcloud-storage', function() {
    it('should create a bucket', function(done) {
        storage.bucket.create(test_bkt).then(function(res) {
            assert(res.statusCode, 200);
            assert(res.body.kind, 'storage#bucket');
            assert(res.body.id, test_bkt);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should get a bucket', function(done) {
        storage.bucket.get(test_bkt).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 200);
            assert(res.body.kind, 'storage#bucket');
            assert(res.body.id, test_bkt);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should list a bucket', function(done) {
        storage.bucket.list().then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 200);
            assert(res.body.length, 1);
            assert(res.body[0].kind, 'storage#bucket');
            assert(res.body[0].id, test_bkt);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should create a file', function(done) {
        storage.file.create(test_bkt, test_file, test_data).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 200);
            assert(res.body.kind, 'storage#object');
            assert(res.body.id, test_bkt + '/' + test_file);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should create the file again', function(done) {
        storage.file.create(test_bkt, test_file, test_data).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 200);
            assert(res.body.kind, 'storage#object');
            assert(res.body.id, test_bkt + '/' + test_file);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should get the file', function(done) {
        storage.file.get(test_bkt, test_file).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 200);
            assert(res.body.key1, test_data.key1);
            assert(res.body.key2, test_data.key2);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should delete the file', function(done) {
        storage.file.delete(test_bkt, test_file).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 204);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

    it('should delete a bucket', function(done) {
        storage.bucket.delete(test_bkt).then(function(res) {
            //console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 204);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

});
