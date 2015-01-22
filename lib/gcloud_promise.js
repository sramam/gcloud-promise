/* @flow */
/*

  gcloud-promise. A promises based gcloud interface to Google Cloud Services.

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.

 */

module.exports = function(keyFile, log) {
winston = require('winston');
winston.addColors({
    error: 'red',
    warn: 'orange',
    http: 'blue'
});
log = log || winston;
log.transports.Console({level : 'invalid'});
var Promise = require('bluebird'),
    request = Promise.promisify(require('request')),
    fs = require('fs'),
    jws = require('jws'),
    creds = JSON.parse(fs.readFileSync(keyFile, 'utf-8')),
    token = null,
    getToken = function(scope) {
        if (token && token.expiry && (new Date()).getTime() < token.expiry) {
            log.debug('Reusing token');
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
                    log.debug('New token');
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
            log.verbose('apiCall Authorization:\n curl -X "POST" -H "Authorization:  Bearer ' + tok.token +' " -H "Content-Type:  application/json" -H "Content-length: 2" https://www.googleapis.com/datastore/v1beta2/datasets/dev-bydesign-2/beginTransaction -d "{}"\n');

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
                            log.error('Invalid key format');
                            log.error('Paths are specified as arrays of arrays, one of');
                            log.error(" 1. [['kind1], ...]");
                            log.error(" 2. [['kind', 'name'], ...]");
                            log.error(" 3. [['kind', 'name', 'id'],...]");
                            throw new Error("Paths not an array of arrays. (" + idx + ") " + JSON.stringify(arr));
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
                if ('integerValue' in property) {
                    return parseInt(property.integerValue.toString(), 10);
                }
                if ('doubleValue' in property) {
                    return property.doubleValue;
                }
                if ('stringValue' in property) {
                    return property.stringValue;
                }
                if ('blobValue' in property) {
                    return new Buffer(property.blobValue, 'base64');
                }
                if ('dateTimeValue' in property) {
                    return property.dateTimeValue;
                }
                if ('booleanValue' in property) {
                    return property.booleanValue;
                }
                if ('listValue' in property) {
                    var list = [];
                    for (var i = 0; i < property.listValue.length; i++) {
                        list.push(propertyToValue(property.listValue[i]));
                    }
                    return list;
                }
                throw new Error('Unhandled propery type ' + JSON.stringify(property));
            },
            valueToProperty = function(v) {
                // convert from property -> protobuf
                var p = {};
                if (v instanceof Boolean || typeof v === 'boolean') {
                    return { booleanValue: v };
                }
                if (v instanceof Number || typeof v === 'number') {
                    if (v % 1 === 0) {
                        return { integerValue: v };
                    } else {
                        return { doubleValue: v };
                    }
                }
                if (v instanceof Date) {
                    return { dateTimeValue: v.getTime()*1000 };
                }
                if (v instanceof String || typeof v === 'string') {
                    return { stringValue: v };
                }
                if (v instanceof Buffer) {
                    return { blobValue: v };
                }
                if (v instanceof Array) {
                    p.listValue = v.map(function(item) {
                        return valueToProperty(item);
                    });
                    return p;
                }
                throw new Error('Unsupported field value: ' + JSON.stringify(v));
            },
            entity = function() {
                var toProto = function(obj, key) {
                        // object -> protofbuf
                        return {
                            key: key,
                            properties: Object.keys(obj).reduce(function(accum, key) {
                                accum[key] = valueToProperty(obj[key]);
                                return accum;
                            }, {})
                        };
                    },
                    fromProto = function(proto) {
                        // protobuf -> object
                        var properties = proto.properties || [];
                        return Object.keys(properties).reduce(function(acc, key) {
                            var elem = properties[key];
                            acc[key] = propertyToValue(elem);
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
                        return _t; // allows chaining
                    },
                    insert = function(entities) {
                        data.mutation.insert = data.mutation.insert || [];
                        data.mutation.insert = data.mutation.insert.concat(entities);
                        return _t; // allows chaining
                    },
                    update = function(entities) {
                        data.mutation.update = data.mutation.update || [];
                        data.mutation.update = data.mutation.update.concat(entities);
                        return _t; // allows chaining
                    },
                    delete_ = function(entities, force) {
                        var e_ = entities.reduce(function(acc, obj) {
                            if ('key' in obj) {
                                acc.push(obj.key);
                            } else {
                                acc.push(obj);
                            }
                            return acc;
                        }, []);
                        data.mutation.delete = data.mutation.delete || e_;
                        data.mutation.delete.force = (force && force===true);
                        return _t; // allows chaining
                    },
                    rollback = function() {
                        return api_cal({
                            method: 'POST',
                            url: url.rollback,
                            json: {
                                transaction: data.transaction
                            }
                        });
                    },
                    commit = function(transactional) {
                        var start = process.hrtime();
                        data.mode = (!transactional) ? 'TRANSACTIONAL': 'NON_TRANSACTIONAL';
                        // do the commit here.
                        log.debug("commit data:" + JSON.stringify(data,null, 2));
                        return api_call({
                            method: 'POST',
                            url: url.commit,
                            encoding: null,
                            json: data
                        }, 'commit').then(function(result) {
                            var time_taken = process.hrtime(start);
                            //log.info('transaction committed:' + time_taken + ':' + JSON.stringify(result));
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
                var start = process.hrtime();
                return api_call({
                    method: 'POST',
                    url: url.beginTransaction,
                    json: {
                        isolationLevel: isolationLevel
                    }
                }, 'beginTransaction').then(function(t) {
                    var time_taken = process.hrtime(start);
                    if (t.statusCode === 200) {
                        //log.info('transaction started:' + time_taken + ':' + t.statusCode);
                        data.transaction = t.body.transaction;
                        active_transaction = data.transaction;
                        return _t;
                    } else {
                        throw new Error("Failed to begin a transaction, " + t.body.statusCode + ", " + t.body.error);
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
                            throw new Error("direction('"+direction+"') not one of ['ASCENDING', 'DESCENDING']");
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
                            qdata.readOptions.readConsistency = val;
                        }
                        return q;
                    },
                    namespace = function(val) {
                        if(val) {
                            qdata.partition_id = val;
                        }
                        return q;
                    },
                    transaction = function() {
                        // uses an active transaction if it exists.
                        if(active_transaction) {
                            qdata.partition_id = active_transaction;
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
                        log.debug(qdata);
                        var start = process.hrtime();
                        return api_call({
                            method: 'POST',
                            url: url.runQuery,
                            json: qdata
                        }, 'runQuery').then(function(result) {
                            var time_taken = process.hrtime(start);
                            q_response.cursor = result.endCursor;
                            q_response.moreResults = result.moreResults;
                            q_response.skippedResults = result.skippedResults;
                            //log.info('Query:' + time_taken);
                            //log.info('Query' + JSON.stringify(result));
                            var entityResults = result.body.batch.entityResults;
                            return result.body.batch.entityResults.reduce(function(acc, el) {
                                acc.push(entity.fromProto(el.entity));
                                return acc;
                            }, []);
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
            gql = function(cust_id, transaction) {
                var gqldata = {},
                    init = function() {
                        gqldata = {
                            readOptions : {
                                readConsistency: 'DEFAULT'
                            },
                            partitionId: {
                                datasetId: project_id
                            },
                            gqlQuery: {
                                queryString: "",
                                allowLiteral: true,
                                nameArgs: [],
                                numberArgs: []
                            }
                        };
                        if (cust_id) {
                            gqldata.partitionId.namespace = cust_id;
                        }
                        if (transaction) {
                            gqldata.readOptions.transaction = transaction;
                            transaction = null;
                        }
                        return gqldata;
                    },
                    gql_response = {
                        cursor: null
                    },
                    query = function(qs) {
                        init(); // reset params on new query
                        gqldata.gqlQuery.queryString = qs;
                        return q;
                    },
                    readConsistency = function(val) {
                        if(val in ['DEFAULT', 'EVENTUAL', 'STRONG']) {
                            gqldata.readOptions.readConsistency = val;
                        }
                        return q;
                    },
                    nameArgs = function(args) {
                        gqldata.gqlQuery.nameArgs = entity.toProto(args);
                        gqldata.gqlQuery.numberArgs = [];
                        if (query_response.cursor) {
                            gqldata.gqlQuery.nameArgs.forEach(function(el) {
                                el.cursor = query_response.cursor;
                            });
                        }
                        return q;
                    },
                    numberArgs = function(args) {
                        gqldata.gqlQuery.numberArgs = entity.toProto(args);
                        gqldata.gqlQuery.nameArgs = [];
                        if (query_response.cursor) {
                            gqldata.gqlQuery.numberArgs(function(el) {
                                el.cursor = query_response.cursor;
                            });
                        }
                        return q;
                    },
                    execute = function() {
                        log.debug(gqldata);
                        var start = process.hrtime();
                        return api_call({
                            method: 'POST',
                            url: url.runQuery,
                            json: gqldata
                        }, 'runQuery').then(function(result) {
                            var time_taken = process.hrtime(start);
                            gql_response.cursor =result.endCursor;
                            gql_response.moreResults = result.moreResults;
                            gql_response.skippedResults = result.skippedResults;
                            //log.info('GQL query:' + time_taken + ':' + gqldata.gqlQuery.queryString);
                            //log.info('GQL query' + JSON.stringify(result));
                            var entityResults = result.body.batch.entityResults;
                            return result.body.batch.entityResults.reduce(function(acc, el) {
                                acc.push(entity.fromProto(el.entity));
                                return acc;
                            }, []);
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
                        nameArgs: nameArgs,
                        numberArgs: numberArgs,
                        execute: execute,
                        next: next
                    };
                init();
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
