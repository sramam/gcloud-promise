

if (process.env.GCLOUD_KEYFILE === undefined) {
    throw "Environment variable GCLOUD_KEYFILE is not defined";
} else {
    console.log("Using GCLOUD_KEYFILE " + process.env.GCLOUD_KEYFILE);
}

var assert = require("assert"),
    blanket = require("blanket"),
    fs = require('fs'),
    _ = require('lodash'),
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
    }],
    options =  {
        folders: {
            fixtures: "test/fixtures" // folder to find/store fixtures in
        },
        place_holders: {},
        enable_reqbody_recording: [{
            path: "/o/oauth2/token",
            enabled: false
        }]
    },
    nockturnal = require("nockturnal")("datastore", options);

describe('gcloud-datastore', function(){
    this.timeout(5000);
    before(function() {
        nockturnal.before();
    });
    it('should save in data in a transaction', function(done) {
        return ds.transaction().then(function(t){
            assert.notEqual(t, null);
            var entities = data.reduce(function(acc, obj) {
                var key = ds.key(acct.cust_id, [['Nasdaq100', obj.Symbol]]);
                acc.push(ds.entity.toProto(obj, key));
                return acc;
            }, []);
            return t.upsert(entities).commit().then(function(res) {
                //console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });
    it('gql queries should return results', function(done) {
        var gql = ds.gql(acct.cust_id).query('SELECT * FROM Nasdaq100 ORDER BY netChange');
        return gql.execute().then(function(res) {
            //console.log(JSON.stringify(res));
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
                //console.log(JSON.stringify(res, null, 2));
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
                //console.log(JSON.stringify(res, null, 2));
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
                //console.log(JSON.stringify(res, null, 2));
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
                //console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 0);
                done();
            });
        });
    });
    it('gql queries after insert+update should return results', function(done) {
        var gql = ds.gql(acct.cust_id).query('SELECT * FROM Nasdaq100 ORDER BY netChange');
        return gql.execute().then(function(res) {
            //console.log(JSON.stringify(res));
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
                //console.log(JSON.stringify(res, null, 2));
                assert.equal(res.statusCode, 200);
                assert.equal(res.body.mutationResult.indexUpdates, 30);
                done();
            });
        });
    });
    after(function(done) {
        nockturnal.after(done);
    });
});
