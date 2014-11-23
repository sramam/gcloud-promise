/* @flow */
/* gcloud-promise. A promises based gcloud interface to Google Cloud Services.
 * (c) 2014 Shishir Ramam. MIT licensed.
 */

module.exports = function(keyFile, log) {
var Promise = require('bluebird'),
    request = Promise.promisify(require('request')),
    fs = require('fs'),
    jws = require('jws'),
    creds = JSON.parse(fs.readFileSync(keyFile, 'utf-8')),
    token = null,
    getToken = function(scope) {
        if (token && token.expiry && (new Date()).getTime() < token.expiry) {
            if(log) log.debug('Reusing token');
            return Promise.props(token);
        } else {
            if (typeof scope  === 'string') {
                scope = scope.split(' ');
            }
            scope = scope.join(' ');
            var iat = Math.floor(new Date().getTime() / 1000),
                payload = {
                    iss: creds.client_email,
                    scope: scope,
                    aud: 'https://accounts.google.com/o/oauth2/token',
                    exp: iat+3600,
                    iat: iat
                },
                signedJWT = jws.sign({
                    header: {alg: 'RS256', type: 'JWT'},
                    payload: payload,
                    secret: creds.private_key
                });
            return request({
                url: 'https://accounts.google.com/o/oauth2/token',
                method: 'POST',
                formData: {
                    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    assertion:   signedJWT
                },
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            }).then(function(d) {
                if (d[0].statusCode == '200') {
                    var t = JSON.parse(d[0].body);
                    if(log) log.debug('New token');
                    token = {
                        token: t.access_token,
                        valid_for: t.expires_in,
                        expiry: (new Date()).getTime() + t.expires_in*1000
                    };
                } else {
                    token = {
                        token: null,
                        valid_for: 0,
                        expiry: null
                    };
                }
                return token;
            });
        }
    },
    apiCall = function(params, scope, project_id) {
        return getToken(scope).then(function(tok) {
            params.headers = params.headers || {};
            params.headers['x-goog-api-version'] = 2;
            params.headers['x-goog-project-id'] = project_id;
            params.headers.Authorization = 'Bearer ' + tok.token;
            //console.log('apiCall Authorization:\n curl -X "POST" -H "Authorization:  Bearer ' + tok.token +' " -H "Content-Type:  application/json" -H "Content-length: 2" https://www.googleapis.com/datastore/v1beta2/datasets/dev-bydesign-2/beginTransaction -d "{}"\n');

            return request(params).then(function(d) {
                var resp = d[0].toJSON(),
                    statusCode = parseInt(d[0].statusCode),
                    body;
                try {
                    body = JSON.parse(resp.body);
                } catch (err) {
                    body = resp.body;
                }
                if (200 <= statusCode && statusCode<300) {
                    return {
                        body: body,
                        errors: null,
                        statusCode: d[0].statusCode
                    };
                } else if (body.error) {
                    return {
                        body: null,
                        errors: body.error.errors,
                        statusCode: d[0].statusCode
                    };
                } else {
                    return {
                        body: null,
                        errors: body,
                        statusCode: d[0].statusCode
                    };
                }
            });
        });
    },
    storage = function(project_id) {
        // Google API constants
        var STORAGE_BASE_URL = 'https://www.googleapis.com/storage/v1/b',
            STORAGE_UPLOAD_URL = 'https://www.googleapis.com/upload/storage/v1/b',
            scope = 'https://www.googleapis.com/auth/devstorage.read_write',
            api_call = function(params) {
                return apiCall(params, scope, project_id);
            },
            file = {
                create: function(bucket, file, data, content_encoding) {
                    contentEncoding = 'utf-8';
                    return api_call({
                        method: 'POST',
                        url: STORAGE_UPLOAD_URL + '/' + bucket + '/o',
                        qs: {
                            uploadType: 'media',
                            contentEncoding: contentEncoding,
                            name: file
                        },
                        body: data
                    });
                },
                delete: function(bucket, file) {
                    return api_call({
                        method: 'DELETE',
                        url: STORAGE_BASE_URL + '/' + bucket + '/o/' + file
                    });
                },
                get: function(bucket, file) {
                    return api_call({
                        method: 'GET',
                        url: STORAGE_BASE_URL + '/' + bucket + '/o/' + file,
                        qs: {
                            alt: 'media'
                        }
                    });
                },
                meta: function(bucket, file) {
                    return api_call({
                        method: 'GET',
                        url: STORAGE_BASE_URL + '/' + bucket + '/o/' + file,
                    });
                }
            },
            bucket = {
                create: function(bucket) {
                    return api_call({
                        method: 'POST',
                        url: STORAGE_BASE_URL,
                        qs: {
                            project: project_id
                        },
                        json: {
                            name: bucket
                        }
                    });
                },
                delete: function(bucket) {
                    return api_call({
                        method: 'DELETE',
                        url: STORAGE_BASE_URL + '/' + bucket
                    });
                },
                get: function(bucket) {
                    return api_call({
                        method: 'GET',
                        url: STORAGE_BASE_URL + '/' + bucket
                    });
                },
                list: function() {
                    return api_call({
                        method: 'GET',
                        url: STORAGE_BASE_URL,
                        qs: {
                            project: project_id
                        }
                    }).then(function(d) {
                        if (d.body) {
                            d.body = d.body.items;
                        }
                        return d;
                    });
                }
            };
        return {
            bucket: bucket,
            file: file
        };
    },
    datastore = function(project_id) {
        var base_url = 'https://www.googleapis.com/datastore/v1beta2/datasets/' + project_id,
            dataset_id = project_id,
            url = {
                allocateIds: base_url + '/allocateIds',
                beginTransaction: base_url + '/beginTransaction',
                commit: base_url + '/commit',
                lookup: base_url + '/lookup',
                rollback: base_url + '/rollback',
                runQuery: base_url + '/runQuery'
            },
            scope = [
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/datastore',
                'https://www.googleapis.com/auth/userinfo.email'
            ],
            key = function(cust_id, path, obj) {
                /*
                 * path is an array of 1- or 2- 3-tuple.
                 *    kind - kind of the entity. Mandatory. "__.*__" is reserved
                 *    name - name for the entity. String, <=500 chars. "__.*__" is reserved
                 *    id - long, greater than 0.
                 * the tuples are represented as arrays, [kind, name, id]
                 * Some examples of path:
                 * [['Country'], ['State'], ['Customer']]
                 * or
                 * [['Country', 'US'], ['State', 'California'], ['Customer', 'Google']]
                 * or
                 * [['Country', 'US', 1], ['State', 'California', 0], ['Customer', 'Google', 435]]
                 */
                var _path = path.reduce(function(acc, p, idx, arr) {
                        if(p.constructor !== Array) {
                            console.error('Invalid key format');
                            console.error('Paths are specified as arrays of arrays, one of');
                            console.error(" 1. [['kind1], ...]");
                            console.error(" 2. [['kind', 'name'], ...]");
                            console.error(" 3. [['kind', 'name', 'id'],...]");
                            throw("Paths not an array of arrays. (" + idx + ") " + JSON.stringify(arr));
                        }
                        var _p = { kind : p[0] };
                        if (2 <= p.length) {
                            _p.name = (typeof f === 'function') ? p[1](obj) : p[1];
                        }
                        if (3 === p.length) {
                            _p.id = (typeof f === 'function') ? p[2](obj): p[2];
                        }
                        acc.push(_p);
                        return acc;
                    }, []),
                    _key = {
                        partitionId: {
                            datasetId: project_id
                        },
                        path: _path
                    };
                if (cust_id) _key.partitionId.namespace = cust_id;
                return _key;
            },
            propertyToValue = function(property) {
                // convert from protobuf -> property
                if (exists(property.integer_value)) {
                    return parseInt(property.integer_value.toString(), 10);
                }
                if (exists(property.double_value)) {
                    return property.double_value;
                }
                if (exists(property.string_value)) {
                    return property.string_value;
                }
                if (exists(property.blob_value)) {
                    return new Buffer(property.blob_value, 'base64');
                }
                if (exists(property.timestamp_microseconds_value)) {
                    var microSecs = parseInt(
                        property.timestamp_microseconds_value.toString(), 10);
                        return new Date(microSecs/1000);
                }
                if (exists(property.key_value)) {
                    return keyFromKeyProto(property.key_value);
                }
                if (exists(property.entity_value)) {
                    return entityFromEntityProto(property.entity_value);
                }
                if (exists(property.boolean_value)) {
                    return property.boolean_value;
                }
                if (exists(property.list_value)) {
                    var list = [];
                    for (var i = 0; i < property.list_value.length; i++) {
                        list.push(propertyToValue(property.list_value[i]));
                    }
                    return list;
                }
            },
            valueToProperty = function(v) {
                // convert from property -> protobuf
                var p = {};
                if (v instanceof Boolean || typeof v === 'boolean') {
                    return { boolean_value: v };
                }
                if (v instanceof Number || typeof v === 'number') {
                    if (v % 1 === 0) {
                        return { integer_value: v };
                    } else {
                        return { double_value: v };
                    }
                }
                if (v instanceof Date) {
                    return { timestamp_microseconds_value: v.getTime()*1000 };
                }
                if (v instanceof String || typeof v === 'string') {
                    return { string_value: v };
                }
                if (v instanceof Buffer) {
                    return { blob_value: v };
                }
                if (v instanceof Array) {
                    p.list_value = v.map(function(item) {
                        return valueToProperty(item);
                    });
                    return p;
                }
                if (v instanceof Key) {
                    p.key_value = keyToKeyProto(v);
                    return p;
                }
                if (v instanceof Object && Object.keys(v).length > 0) {
                    var property = [];
                    Object.keys(v).forEach(function(k) {
                        property.push({
                            name: k,
                            value: valueToProperty(v[k])
                        });
                    });
                    p.entity_value = { property: property };
                    p.indexed = false;
                    return p;
                }
                throw new Error('Unsupported field value, ' + v + ', is provided.');
            },
            entity = function() {
                var toProto = function(obj, key) {
                        // object -> protofbuf
                        return {
                            key: key,
                            property: Object.keys(obj).map(function(key) {
                                return {
                                    name: key,
                                    value: valueToProperty(obj[key])
                                };
                            })
                        };
                    },
                    fromProto = function(proto) {
                        // protobuf -> object
                        var properties = proto.property || [];
                        return Object.keys(properties).reduce(function(acc, key) {
                            var property = properties[key];
                            acc[property.name] = propertyToValue(property.value);
                            return acc;
                        }, {});
                    };
                return {
                    toProto: toProto,
                    fromProto: fromProto
                };
            }(),
            api_call = function(params, operation) {
                return apiCall(params, scope, project_id).then(function(response) {
                    return response;
                });
            },
            getType = function(o) {
                if (o instanceof Buffer){
                    return 'Buffer';
                } else {
                    return Object.prototype.toString.call(o).match(/\s(\w+)\]/)[1];
                }
            },
            isOfType = function(o, type) {
                var o_type = getType(o).toLowerCase();
                return o_type === type.toLowerCase();
            },
            active_transaction = null,
            transaction = function(cust_id, isolationLevel) {
                var data = {
                        transaction: null,
                        mutation: {}
                    },
                    upsert = function(entities) {
                        data.mutation.upsert = data.mutation.upsert || [];
                        data.mutation.upsert = data.mutation.upsert.concat(entities);
                        return _t;
                    },
                    insert = function(entities, data) {
                        data.mutation.insert = data.mutation.insert || [];
                        data.mutation.insert.concat(entities);
                        return _t;
                    },
                    update = function(entities, data) {
                        data.mutation.update = data.mutation.update || [];
                        data.mutation.update.concact(entities);
                        return _t;
                    },
                    delete_ = function(entities, force, data) {
                        var e_ = entities.map(function(e) {
                            return e.key;
                        });
                        data.mutation.delete = data.mutation.delete || [];
                        data.mutation.delete(e_);
                        data.mutation.delete.force = (force===true);
                        return _t;
                    },
                    rollback = function(data) {
                        return api_cal({
                            method: 'POST',
                            url: url.rollback,
                            json: {
                                transaction: data.transaction
                            }
                        });
                    },
                    commit = function(transactional) {
                        data.mode = (!transactional) ? 'TRANSACTIONAL': 'NON_TRANSACTIONAL';
                        // do the commit here.
                        // console.log(JSON.stringify(data,null, 2))
                        return api_call({
                            method: 'POST',
                            url: url.commit,
                            encoding: null,
                            json: data
                        }, 'commit').then(function(result) {
                            // console.log('transaction was committed ' + JSON.stringify(result));
                            active_transaction = null;
                            data.transaction = null;
                            return result;
                        });
                    },
                    _t = {
                        upsert: upsert,
                        update: update,
                        insert: insert,
                        delete: delete_,
                        rollback: rollback,
                        commit: commit
                    };
                isolationLevel = isolationLevel || 'SERIALIZABLE';
                return api_call({
                    method: 'POST',
                    url: url.beginTransaction,
                    json: {
                        isolationLevel: isolationLevel
                    }
                }, 'beginTransaction').then(function(t) {
                    if (t.statusCode === 200) {
                        data.transaction = t.body.transaction;
                        active_transaction = data.transaction;
                        return _t;
                    } else {
                        throw "Failed to begin a transaction, " + t.body.statusCode + ", " + t.body.error
                    }
                });
            },
            qdata = {},
            q_response = {
                cursor: null
            },
            query = function(cust_id) {
                var limit = function(val) {
                        if(val) {
                            qdata.limit = limit;
                        }
                        return q;
                    },
                    offset = function(val) {
                        if(val) {
                            qdata.offset = limit;
                        }
                        return q;
                    },
                    startCursor = function(val) {
                        if(val) {
                            qdata.startCursor = val;
                        }
                        return q;
                    },
                    endCursor = function(val) {
                        if(val) {
                            qdata.endCursor = val;
                        }
                        return q;
                    },
                    groupBy = function(val) {
                        if(val) {
                            qdata.groupBy = qdata.groupBy || [];
                            qdata.groupBy.push({name: val});
                        }
                        return q;
                    },
                    order = function(name, direction) {
                        if (!(direction in ['ASCENDING', 'DESCENDING'])) {
                            throw "direction('"+direction+"') not one of ['ASCENDING', 'DESCENDING']";
                        }
                        if(name) {
                            qdata.order = qdata.order || [];
                            qdata.order({
                                property: {
                                    name: name
                                },
                                direction: direction
                            });
                        }
                        return q;
                    },
                    kinds = function(val) {
                        if(val) {
                            qdata.kinds = qdata.kinds || [];
                            qdata.kinds({name: val});
                        }
                        return q;
                    },
                    projection = function(name, aggregationFunction) {
                        if(name) {
                            qdata.projection = {
                                property: {
                                    name: name,
                                },
                                aggregationFunction: aggregationFunction
                            };
                        }
                        return q;
                    },
                    readConsistency = function(val) {
                        if(val in ['DEFAULT', 'EVENTUAL', 'STRONG']) {
                            gqldata.readOptions.readConsistency = val;
                        }
                        return q;
                    },
                    namespace = function(val) {
                        if(val) {
                            gqldata.partition_id = val;
                        }
                        return q;
                    },
                    transaction = function() {
                        // uses an active transaction if it exists.
                        if(active_transaction) {
                            gqldata.partition_id = active_transaction;
                        }
                        return q;
                    },
                    filter = function(name, op, value) {
                        if (qdata.filter.propertyFilter) {
                            qdata.filter.compositeFilter = {
                                operator: 'AND',
                                filters: []
                            };
                            qdata.filter.compositeFilter.filters.push(qdata.filter.propertyFilter);
                            delete qdata.filter.propertyFilter;
                        }
                        var f = {
                            property: {
                                name: name,
                            },
                            op: op,
                            value: entity.toProto(valus)
                        };
                        if (qdata.filter.compositeFilter) {
                            qdata.filter.compositeFilter.filters.push(f);
                        } else {
                            qdata.filter.propertyFilter = f;
                        }
                        return q;
                    },
                    execute = function() {
                        return api_call({
                            method: 'POST',
                            url: url.runQuery,
                            json: qdata
                        }, 'runQuery').then(function(result) {
                            q_response.cursor = result.endCursor;
                            q_response.moreResults = result.moreResults;
                            q_response.skippedResults = result.skippedResults;
                            return result;
                        });
                    },
                    next = function() {
                        // this continues to get the next match of results.
                        if (q_response.moreResults !== 'NO_MORE_RESULTS') {
                            startCursor(q_response.cursor);
                            return execute();
                        } else {
                            return Promise.all().then(function(d) {
                                return null;
                            });
                        }
                    },
                    q = {
                        limit: limit,
                        offset: offset,
                        startCursor: startCursor,
                        endCursor: endCursor,
                        groupBy: groupBy,
                        order: order,
                        kinds: kinds,
                        projection: projection,
                        readConsistency: readConsistency,
                        namespace: namespace,
                        transacction: transacction,
                        filter: filter,
                        execute: execute,
                        next: next
                    };
                return q;
            },
            gqldata = {},
            gql_response = {
                cursor: null
            },
            gql = function(use_transaction) {
                var query = function(qs) {
                        reset();
                        gqldata.queryString = qs;
                    },
                    readConsistency = function(val) {
                        if(val in ['DEFAULT', 'EVENTUAL', 'STRONG']) {
                            gqldata.readOptions.readConsistency = val;
                        }
                        return q;
                    },
                    namespace = function(val) {
                        if(val) {
                            gqldata.partition_id = val;
                        }
                        return q;
                    },
                    transaction = function() {
                        // uses an active transaction if it exists.
                        if(active_transaction) {
                            gqldata.partition_id = active_transaction;
                        }
                        return q;
                    },
                    nameArgs = function(args) {
                        gqldata.nameArgs = entity.toProto(args);
                        gqldata.numberArgs = [];
                        if (query_response.cursor) {
                            gqldata.nameArgs.forEach(function(el) {
                                el.cursor = query_response.cursor;
                            });
                        }
                        return q;
                    },
                    numberArgs = function(args) {
                        gqldata.numberArgs = entity.toProto(args);
                        gqldata.nameArgs = [];
                        return q;
                    },
                    reset = function() {
                        gqldata = {
                            partition_id: {
                                dataset_id: project_id
                            },
                            queryString: "",
                            nameArgs: [],
                            numberArgs: [],
                            cursor: ""
                        };
                        return q;
                    },
                    execute = function() {
                        return api_call({
                            method: 'POST',
                            url: url.runQuery
                        }, 'runQuery').then(function(result) {
                            query_response.cursor = result.endCursor;
                            query_response.moreResults = result.moreResults;
                            query_response.skippedResults = result.skippedResults;
                            return result;
                        });
                    },
                    next = function() {
                        // this continues to get the next match of results.
                        if (query_response.moreResults !== 'NO_MORE_RESULTS') {
                            if(gqldata.nameArgs !== []) {
                                if (query_response.cursor) {
                                    gqldata.nameArgs.forEach(function(el) {
                                        el.cursor = query_response.cursor;
                                    });
                                }
                            } else if(gqldata.numberArgs !== []) {
                                if (query_response.cursor) {
                                    gqldata.numberArgs.forEach(function(el) {
                                        el.cursor = query_response.cursor;
                                    });
                                }
                            }
                            return execute();
                        } else {
                            return Promise.all().then(function(d) {
                                return null;
                            });
                        }
                    },
                    q = {
                        query: query,
                        readConsistency: readConsistency,
                        namespace: namespace,
                        transaction: transaction,
                        nameArgs: nameArgs,
                        numberArgs: numberArgs,
                        reset: reset,
                        execut: execute,
                        next: next
                    };
                if (use_transaction) {
                    transaction();
                }
                return q;
            };
        return {
            key: key,
            entity: entity,
            transaction: transaction,
            query: query,
            gql: gql
        };
    };
    return {
        getToken: getToken,
        storage: storage,
        datastore: datastore,
    };
};