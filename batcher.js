/*jslint node: true */
"use strict";
var db = require('./db.js');
var kvstore = require('./kvstore.js');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var profiler = require('./profiler.js');

var conn;
var batch;

var bOngoingBatch = false;
var bOngoingSubBatch = false;
var bWaitingConnection = false;
var bCommittingBatch = false;

var bCommit = false;
var timerId;

var kvCachePut = {};
var kvCacheDel = {};
var kvSubCachePut = {};
var kvSubCacheDel = {};


var arrSingleQueries = [];
var arrSubBatches = [];


exports.query = function(sql, args, cb) { // execute a SQL query as soon as possible working with current uncommited data
	if (bOngoingBatch || bWaitingConnection){
		if (bWaitingConnection)
			return arrSingleQueries.push(executeQuery);
		else
			executeQuery();
		function executeQuery(){
		if (cb)
			conn.query(sql, args, cb);
		else
			conn.query(sql, args)
		}
	}
	else {
		if (cb)
			db.query(sql, args, cb);
		else
			db.query(sql, args);
	}
}

exports.startSubBatch = function(callback){ // start a subbatch as soon as a connection from pool is available

	if (!bOngoingBatch && !bWaitingConnection){
		console.log('start new batch ');
		bWaitingConnection = true;
		bCommit = false;
		bOngoingSubBatch = true;
		setTimeout(function(){
			bCommit = true;
		}, 1200);
		batch = kvstore.batch();
		db.takeConnectionFromPool(function (_conn) {
			conn = _conn;
			conn.query("BEGIN", function(){
				conn.query("SAVEPOINT spt_sub_batch", function(){
					bWaitingConnection = false;
					bOngoingBatch = true;
					arrSingleQueries.forEach(function(exeQuery){
						exeQuery();
					});
					arrSingleQueries = [];
					callback(createSubBatchfunctions());
				});
			});
		})
	} else if (!bOngoingSubBatch && !bWaitingConnection && !bCommittingBatch) {
		bOngoingSubBatch = true;
		console.log('already in batch, start new sub batch')
		conn.query("SAVEPOINT spt_sub_batch", function(){
			callback(createSubBatchfunctions());
		});
	} else {
		console.log('subbatch ongoing, will queue');
		arrSubBatches.push(callback);
	}

	function createSubBatchfunctions(){

		var objSubBatchFunctions = {
			release:function(cb){ // virtually commit a sub batch
				profiler.start();
				conn.query("RELEASE spt_sub_batch", function(){
					console.log("sub batch released")
					commitBatchIfNecessary(cb);
				})
			},
			rollback:function(cb){  // virtually rollback a sub batch
				profiler.start();
				conn.query("ROLLBACK TO spt_sub_batch", function(){
					console.log("sub batch rollbacked")
					commitBatchIfNecessary(cb);
				})
			},

			sql: {
				query: function(sql, args, cb){ // in sub batch query
					if (cb)
						conn.query(sql, args, cb)
					else
						conn.query(sql, args)
				},
				cquery:function(sql, args, cb){
					if (cb)
						conn.cquery(sql, args, cb)
					else
						conn.cquery(sql, args)
				}, 
				addQuery:conn.addQuery,
				getFromUnixTime: conn.getFromUnixTime,
				getIgnore: conn.getIgnore,
				escape: conn.escape,
				dropTemporaryTable: conn.dropTemporaryTable,
				getUnixTimestamp: conn.getUnixTimestamp
		
			},
			kv: {
				get: function(key, cb){ // get merged kv data from sub batch cache, batch cache and rocksdb
					console.log("look for key " + key);
					if (kvSubCachePut[key]){
						console.log(key + " found in kvSubCachePut")
						return cb(kvSubCachePut[key]);
					} else if (kvSubCacheDel[key]){
						console.log(key + " deleted in kvSubCachePut")
						return cb();
					} else if (kvCachePut[key]){
						console.log(key + " found in kvCachePut")
						return cb(kvCachePut[key]);
					}
					else if (kvCacheDel[key]){
						console.log(key + " deleted in kvCacheDel")
						return cb();
					}
					else {
						console.log("look for key " + key + " in store");
						return kvstore.get(key, cb);
					}
				},
				put: function(key, value){ // put kv data in sub batch cache
					console.log("put in subbatch " + key + " " + value)
					kvSubCachePut[key] = value;
					delete kvSubCacheDel[key];
				},
				del: function(key){ // delete kv data in sub batch cache
					kvSubCacheDel[key] = true;
					delete kvSubCachePut[key];
				},
				clear: function(){ // clear kv data in sub batch cache
					for (var key in kvSubCachePut)
						delete kvSubCachePut[key];
					for (var key in kvSubCacheDel)
						delete kvSubCacheDel[key];
				},
				write: function(cb){ // copy kv data from sub batch to batch cache
					console.log("write kv subbatch")
					for (var key in kvSubCacheDel){
						batch.del(key);
						kvCacheDel[key] = true;
					}
					for (var key in kvSubCachePut){
						batch.put(key, kvSubCachePut[key]);
						kvCachePut[key] = kvSubCachePut[key];
					}
					for (var key in kvSubCachePut) // clear sub batch cache
						delete kvSubCachePut[key];
					for (var key in kvSubCacheDel)
						delete kvSubCacheDel[key];
					if (cb)
						cb();
				}
			}
		}
		return objSubBatchFunctions;
	}
	
	function commitBatchIfNecessary(cb){ // commit the whole batch if time has elapsed
		console.log("commitBatchIfNecessary")
		if (bCommit){
			console.log('will commit batch')

			commitBatch(function(){
				profiler.stop('batch-commit');
				bOngoingBatch= false;
				conn.release();
				conn = null;
				if (cb)
					cb();
				processNextSubBatch();
				
			})
		}
		else {

	/*		clearTimeout(timerId);
			timerId = setTimeout(function(){
				if (bOngoingBatch && !bOngoingSubBatch){
					commitBatch();
				}
			}, 1000);*/
			profiler.stop('sub-batch-end');
			console.log('no batch commit yet')
			if (cb)
				cb();
			bOngoingSubBatch = false;
			processNextSubBatch();
		}

		function commitBatch(cb){ // first write kv batch cache to rocksdb then commit the SQL batch
			if (bCommittingBatch)
				return cb();
			bCommittingBatch = true;
			batch.write({sync: true}, function(err){
				if (err)
					throw Error("write batch failed " + err);

				for (var key in kvCachePut) // clear batch cache
					delete kvCachePut[key];
				for (var key in kvCacheDel)
					delete kvCacheDel[key];

				bOngoingSubBatch = false;
				bCommit = false;
				batch = kvstore.batch();
				conn.query("COMMIT", function(){
					console.log('batch committed')
					bCommittingBatch = false;
					if (cb)
						cb();
				});
			});
		}

		function processNextSubBatch(){
			if (arrSubBatches.length === 0)
				return;
			console.log("unqueue subbatch");
			exports.startSubBatch(arrSubBatches.shift());
		}
	}

}

var createReadStream = function(options){ // create a read stream merging kv batch cache (but not sub batch cache) and rocksdb
	var self = this;
	var arrResults = [];
	var bDestroyed = false;
	console.log(options);
	kvstore.createReadStream(options)
	.on('data', handleData)
	.on('end', onEnd);

	function handleData(data){
		var key;
		if (options.keys)
			key = data;
		else
			key = data.key;

		if (kvCacheDel[key] || kvCachePut[key])
			return console.log(key + ' overwritten by batch cache');
		arrResults.push(data);
	}

	function onEnd(){
		console.log("createReadStream batch cache state");
		for (var key in kvCachePut){
			if (options.gt && key <= options.gt){
				continue;
			}
			if (options.gte && key < options.gte){
				continue;
			}
			if (options.lt && key >= options.lt){
				continue;
			}
			if (options.lte && key > options.lte){
				continue;
			}
			console.log("found in cache "  + key + " " + kvCachePut[key]);
			if (options.keys)
				arrResults.push(key);
			else
				arrResults.push({key: key, value: kvCachePut[key]});
		}

		if (options.keys)
			arrResults.sort();
		else
			arrResults.sort((a,b)=>{return a.key > b.key ? 1 : -1});
		var limit;
		if (options.limit)
			limit = Math.min(options.limit, arrResults.length);
		else
			limit = arrResults.length;
		for (var i=0; i < limit && !bDestroyed; i++){
			self.emit('data', arrResults[i]);
		}
		if (!bDestroyed)
			self.emit('end');
	}

	this.destroy = function (){
		bDestroyed = true;
	}

	return this;
}

exports.createReadStream  = function(options){
	return new createReadStream(options);
}

exports.createKeyStream = function(options){
	options.keys = true;
	options.values = false;
	return new createReadStream(options);
}

exports.get = function(key, cb){ // read merged data from kv batch cache (but not sub batch cache) and rocksdb
	if (kvCachePut[key]){
		console.log(key + " found in kvCachePut")
		return cb(kvCachePut[key]);
	}
	else if (kvCacheDel[key]){
		console.log(key + " deleted in kvCacheDel")
		return cb();
	}
	else {
		console.log(key + " not found in kvCachePut")
		return kvstore.get(key, cb);
	}
},

util.inherits(createReadStream, EventEmitter);

exports.getUnixTimestamp = db.getUnixTimestamp;
