
var gcloud = require('./lib/gcloud_promise.js'),
    fs = require('fs'),
    keyfile = process.env.GCLOUD_KEYFILE,
    keyfile_valid = (keyfile !== undefined && fs.existsSync(keyfile)),
    acct = {
        keyFilename: process.env.GCLOUD_KEYFILE,
        project_id: 's~dev-bydesign-2',
    },
    gc = gcloud(acct.keyFilename),
    test_bkt = acct.project_id + '-test-creation-1234',
    test_file = acct.project_id + '-test-file-1234',
    test_data = '{"key1": "key1", "key2": "key2"}',
    cust_id = 'Company2';
    if (keyfile_valid !== true) {
        throw 'Keyfile is not valid "' + keyfile + '"';
    }
    var objarr = [{
        Symbol: 'ATVI',
        Name: 'Activision Blizzard Inc',
        lastSale: 20.14,
        netChange: 0.24,
        pctChange: 1.21,
        shareVolume: 939808,
        Nasdaq100Points: 0.2
    }, {
        Symbol: 'EBAY',
        Name: 'eBay Inc',
        lastSale: 55.003,
        netChange: 0.16,
        pctChange: 0.33,
        shareVolume: 923661,
        Nasdaq100Points: 0.2
    }];
    ds = gc.datastore(acct.project_id);
ds.transaction().then(function(t) {
    // t is the transaction
    // we convert js objects to storable protobuf entities.
    // TBD: Is doing this after starting the transaction a bad idea? 
    // TBD: Doesn't seem like it, to be verified.
    var entities = objarr.reduce(function(acc, obj) {
        var key = ds.key(cust_id, [['Nasdaq100', obj.Symbol]]);
        acc.push(ds.entity.toProto(obj, key));
        return acc;
    }, []);
    return t.upsert(entities).commit().then(function(res) {
        if (res.statusCode === 200) {
            console.log('commit succeeded:' + res.statusCode + ' ' + JSON.stringify(res.body));
            return res;
        } else {
            throw('commit failure ' + res.statusCode + ' ' + res.errors);
        }
    }).catch(function(err) {
        throw('commit failed: ' + err);
    });
}).then(function(d) {
    var gql = ds.gql(cust_id).query("SELECT * FROM Nasdaq100 ORDER BY netChange")
    return gql.execute().then(function(result) {
        console.log(JSON.stringify(result));
    });
});

if (false) {

stor = gc.storage(acct.project_id);
stor.bucket.create(test_bkt).then(function(d){
    console.log('\n*** created bucket:' + JSON.stringify(d));
}).then(function() {
    return stor.bucket.get(test_bkt).then(function(d){
        console.log('\n*** get bucket:' + JSON.stringify(d));
    });
}).then(function(){
    return stor.bucket.list().then(function(d) {
        console.log('\n*** list buckets:' + JSON.stringify(d));
    });
}).then(function() {
    return stor.file.create(test_bkt, test_file, test_data).then(function(d) {
        console.log('\n*** create file:' + JSON.stringify(d));
    });
}).then(function() {
    return stor.file.create(test_bkt, test_file, test_data).then(function(d) {
        console.log('\n*** create2 file:' + JSON.stringify(d));
    });
}).then(function() {
    return stor.file.get(test_bkt, test_file).then(function(d) {
        console.log('\n*** get file:' + JSON.stringify(d));
    });
}).then(function() {
    return stor.file.delete(test_bkt, test_file).then(function(d) {
        console.log('\n*** delete file:' + JSON.stringify(d));
    });
}).then(function() {
    return stor.bucket.delete(test_bkt).then(function(d) {
        console.log('\n*** deleted bucket:' + JSON.stringify(d));
    });
});
}
