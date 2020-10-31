/*jslint node: true */
"use strict";
var db = require('./db.js');
var kvstore = require('./kvstore.js');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var savepoint_index = 0;
var conn;
var batch;

var bOngoingBatch = false;
var bOngoingSubBatch = false;
var waitingConnection = false;
var bCommittingBatch = false;

var bCommit = false;
var timerId;

var kvCachePut = {};
var kvCacheDel = {};
var kvSubCachePut = {};
var kvSubCacheDel = {};


var arrSingleQueries = [];
var arrSubBatches = [];

var rollbackeds = [];

exports.getUnixTimestamp = db.getUnixTimestamp;
exports.query = function(sql, args, cb) {
	if (bOngoingBatch || waitingConnection){
		if (waitingConnection)
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

exports.startSubBatch = function(callback){

	if (!bOngoingBatch && !waitingConnection){
		console.log('start new batch ');

		waitingConnection = true;
		savepoint_index = 0;
		bCommit = false;
	//	db.flagOnGoingBatch();
		bOngoingSubBatch = true;


		setTimeout(function(){
			bCommit = true;
		}, 1200);
		batch = kvstore.batch();
		db.takeConnectionFromPool(function (_conn) {
			conn = _conn;
			conn.query("BEGIN", function(){
				savepoint_index++;
				conn.query("SAVEPOINT spt_" +  savepoint_index, function(){
					console.log('put savepoint ' + savepoint_index);
					waitingConnection = false;
					bOngoingBatch = true;
					arrSingleQueries.forEach(function(exeQuery){
						exeQuery();
					});
					arrSingleQueries = [];
					console.log("createSubBatchfunctions " +  ' ' + bOngoingSubBatch)

					callback(createSubBatchfunctions());
				});
			});
		})
	} else if (!bOngoingSubBatch && !waitingConnection && !bCommittingBatch) {
		bOngoingSubBatch = true;

		console.log('already in batch')
		savepoint_index++;
		conn.query("SAVEPOINT spt_" +  savepoint_index, function(){
			console.log("put savepoint_index " + savepoint_index)
			callback(createSubBatchfunctions());
		});
	} else {
		console.log('subbatch ongoing, will queue');
		arrSubBatches.push(callback);

		//throw Error('on going sub batch');
	}

	function createSubBatchfunctions(){

		const subSavePointIndex = savepoint_index;

		var objSubBatchFunctions = {
			release:function(cb){
				conn.query("RELEASE spt_" + subSavePointIndex, function(){
					console.log("released savepoint_index " + subSavePointIndex)
					commitIfNecessary(cb);
				})
			},
			rollback:function(cb){
				if (rollbackeds.indexOf(subSavePointIndex) > -1)
					throw Error("already rollbacked")
				rollbackeds.push(subSavePointIndex);

				conn.query("ROLLBACK TO spt_" + subSavePointIndex, function(){
					console.log("rollbacked savepoint_index " + subSavePointIndex)
					commitIfNecessary(cb);
				})
			},

			sql: {
				query: function(sql, args, cb){
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
				get: function(key, cb){ 
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
				put: function(key, value){
					kvSubCachePut[key] = value;
					delete kvSubCacheDel[key];
				},
				del: function(key){
					kvSubCacheDel[key] = true;
					delete kvSubCachePut[key];
				},
				clear: function(){
					for (var key in kvSubCachePut)
						delete kvSubCachePut[key];
					for (var key in kvSubCacheDel)
						delete kvSubCacheDel[key];
				},
				write: function(cb){
					console.log("write kv subbatch")
					for (var key in kvSubCacheDel){
						batch.del(key);
						kvCacheDel[key] = true;
					}
					for (var key in kvSubCachePut){
						batch.put(key, kvSubCachePut[key]);
						kvCachePut[key] = kvSubCachePut[key];
					}
					for (var key in kvSubCachePut)
						delete kvSubCachePut[key];
					for (var key in kvSubCacheDel)
						delete kvSubCacheDel[key];
					cb();
				}
			}
		}
		return objSubBatchFunctions;
	}
	
	function commitIfNecessary(cb){
		console.log("commitIfNecessary")
		if (bCommit){
			console.log('will commit')

			commitBatch(function(){
				bOngoingBatch= false;
				conn.release();
				conn = null;
				if (cb)
					cb();
				nextSubatch();
				
			})
		}
		else {

	/*		clearTimeout(timerId);
			timerId = setTimeout(function(){
				if (bOngoingBatch && !bOngoingSubBatch){
					commitBatch();
				}
			}, 1000);*/

			console.log('not commit yet ' + bOngoingSubBatch)
			if (cb)
				cb();
			bOngoingSubBatch = false;
			nextSubatch();
		}

		function commitBatch(cb){
			if (bCommittingBatch)
				return cb();
			bCommittingBatch = true;
			batch.write({sync: true}, function(err){
				if (err)
					throw Error("write batch failed " + err);

				for (var key in kvCachePut)
					delete kvCachePut[key];
				for (var key in kvCacheDel)
				delete kvCacheDel[key];

				savepoint_index = 0;
				bOngoingSubBatch = false;
				rollbackeds = [];
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

		function nextSubatch(){
			if (arrSubBatches.length === 0)
				return;
			console.log("unqueue subbatch");
			exports.startSubBatch(arrSubBatches.shift());
		}
	}

}

var createReadStream = function(options){
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
			console.log("found in cache "  + key);
			if (options.keys)
				arrResults.push(key);
			else
				arrResults.push({key: key, value: kvCachePut[key]});
		}

		if (options.keys)
			arrResults.sort();
		else
			arrResults.sort((a,b)=>{a.key < b.key ? 1 : -1});

		console.log("createReadStream results");
		console.log(arrResults);
		console.log(arrResults.length);

		var limit;
		if (options.limit)
			limit = Math.min(options.limit, arrResults.length);
		else
			limit = arrResults.length;
		console.log("limit " + limit)
		for (var i=0; i < limit && !bDestroyed; i++){
			console.log('arrResults[i]')

			console.log(arrResults[i])
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


exports.get = function(key, cb){
	console.log('kvCachePut');
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


exports.hasOnGoingBatch = function() {return bOngoingBatch;};