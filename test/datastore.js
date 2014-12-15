
if (process.env.GCLOUD_KEYFILE === undefined) {
    throw "Environment variable GCLOUD_KEYFILE is not defined";
} else {
    console.log("Using GCLOUD_KEYFILE " + process.env.GCLOUD_KEYFILE);
}

var record = (process.env.NOCK_RECORDER !== undefined), // set to false when in playback mode.
    nock = require('nock');
if (record) {
    console.log("Running tests in replay mode");
    console.log("Running tests in recorder mode");
} else {
    console.log("Running tests in replay mode");
    var nocks = nock.load('./test/nock.json'),
        undone = 0;
    nocks.forEach(function(n) {
        if(!n.isDone()) {
            undone += 1;
        }
    });
    console.error('pending nocks at START: %j', undone);
}

var assert = require("assert"),
    fs = require('fs'),
    keyfile = process.env.GCLOUD_KEYFILE,
    acct = {
        keyFilename: keyfile,
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

if (record) {
    nock.recorder.rec({
        dont_print: true,
        output_objects: true,
        enable_reqheaders_recording: true
    });
}

describe('gcloud-datastore', function(){
    it('should save in data in a transaction', function(done) {
        return ds.transaction().then(function(t){
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                acc.push(ds.entity.toProto(obj, key));
                return acc;
            }, []);
            return t.upsert(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });

    it('gql queries should return results', function(done) {
        var gql = ds.gql(acct.cust_id).query('SELECT * FROM Nasdaq100 ORDER BY netChange');
        return gql.execute().then(function(res) {
            console.log(JSON.stringify(res));
            assert(res.length, 2);
            return done();
        });
    });

    it('should delete data in a transaction', function(done) {
        return ds.transaction().then(function(t) {
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                //acc.push(ds.entity.toProto(obj, key));
                acc.push(key);
                return acc;
            }, []);
            return t.delete(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });
    // we'll then try the insert
    it('should insert in data in a transaction', function(done) {
        return ds.transaction().then(function(t){
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                acc.push(ds.entity.toProto(obj, key));
                return acc;
            }, []);
            return t.insert(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });

    it('should update in data in a transaction', function(done) {
        return ds.transaction().then(function(t){
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                obj.Nasdaq100Points += 0.1;
                acc.push(ds.entity.toProto(obj, key));
                return acc;
            }, []);
            return t.update(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 8);
                done();
            });
        });
    });

    it('should update in data without modifications in a transaction', function(done) {
        return ds.transaction().then(function(t){
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                acc.push(ds.entity.toProto(obj, key));
                return acc;
            }, []);
            return t.update(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 0);
                done();
            });
        });
    });

    it('gql queries after insert+update should return results', function(done) {
        var gql = ds.gql(acct.cust_id).query('SELECT * FROM Nasdaq100 ORDER BY netChange');
        return gql.execute().then(function(res) {
            console.log(JSON.stringify(res));
            assert(res.length, 2);
            return done();
        });
    });

    it('should delete data after insert+update a transaction', function(done) {
        return ds.transaction().then(function(t) {
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                //acc.push(ds.entity.toProto(obj, key));
                acc.push(key);
                return acc;
            }, []);
            return t.delete(entities).commit().then(function(res) {
                console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });

    after(function()  {
        if(record) {
            var nockCalls = nock.recorder.play();
            fs.writeFileSync(__dirname + "/nock.json", JSON.stringify(nockCalls, null, 2));
        } else {
            console.error('pending nocks at END: %j', nocks.pendingMocks());
        }
    });
});

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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
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
            console.log(JSON.stringify(res, null, 2));
            assert(res.statusCode, 204);
            done();
        }).catch(function(err) {
            console.log("ERROR:\n" + JSON.stringify(err, null, 2));
            assert(err, null);
            done();
        });
    });

});
