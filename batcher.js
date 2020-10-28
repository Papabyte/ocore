/*jslint node: true */
"use strict";
var db = require('./db.js');
var kvstore = require('./kvstore.js');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');


var savepoint_index = 0;
var conn;
var batch;

var bOngoingBatch = false;
var bOngoingSubBatch = false;
var waitingConnection = false;

var bCommit = false;
var timerId;

var kvCachePut = {};
var kvCacheDel = {};
var kvSubCachePut = {};
var kvSubCacheDel = {};


var arrSingleQueries = [];
var arrSubBatches = [];

var rollbackeds = [];

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
	console.log('start new batch ' + bOngoingBatch + ' ' + bOngoingSubBatch + ' current index ' + savepoint_index)

	if (!bOngoingBatch && !waitingConnection){
		waitingConnection = true;
		savepoint_index = 0;
		bCommit = false;
	//	db.flagOnGoingBatch();
	bOngoingSubBatch = true;


		timerId = setTimeout(function(){
			bCommit = true;
		}, 1000);
		batch = kvstore.batch();
		db.takeConnectionFromPool(function (_conn) {
			conn = _conn;
			

			conn.query("BEGIN", function(){
				savepoint_index++;
				conn.query("SAVEPOINT spt_" +  savepoint_index, function(){
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
	} else if (!bOngoingSubBatch && !waitingConnection) {
		bOngoingSubBatch = true;

		console.log('already in batch')
		savepoint_index++;
		console.log("savepoint_index " + savepoint_index)
		conn.query("SAVEPOINT spt_" +  savepoint_index, function(){
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
				console.log("RELEASE savepoint_index " + subSavePointIndex)
				conn.query("RELEASE spt_" + subSavePointIndex, function(){
					commitIfNecessary(cb);
				})
			},
			rollback:function(cb){
				console.log("ROLLBACK savepoint_index " + subSavePointIndex)
				if (rollbackeds.indexOf(subSavePointIndex) > -1)
					throw Error("already rollbacked")
				rollbackeds.push(subSavePointIndex);

				conn.query("ROLLBACK TO spt_" + subSavePointIndex, function(){
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
				cquery: conn.cquery,
				addQuery: conn.addQuery,
				getFromUnixTime: conn.getFromUnixTime,
				getIgnore: conn.getIgnore,
				escape: conn.escape
		
			},
			kv: {
				get: function(key, cb){
					if (kvSubCachePut[key]){
						console.log(key + " found in kvSubCachePut")
						return cb(null, kvSubCachePut[key]);
					}
					else if (kvSubCacheDel[key]){
						console.log(key + " deleted in kvSubCachePut")
						return cb({notFound: true, type: 'NotFoundError'});
					}
					else {
						console.log(key + " not found in kvSubCachePut")
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
				createReadStream: function(options){
					return new createReadStream(this, options)
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

			batch.write({sync: true}, function(){
				kvSubCachePut = {};
				kvSubCacheDel = {};
				conn.query("COMMIT", function(){
					console.log('batch committed')
					savepoint_index = 0;
					rollbackeds = [];
					bCommit = false;
					bOngoingBatch= false;
					bOngoingSubBatch = false;

					//db.unflagOnGoingBatch();
					conn.release();
					conn = null;
					if (cb)
						cb();
					nextSubatch();
				});
			});
		}
		else {
			bOngoingSubBatch = false;
			console.log('not committed yet ' + bOngoingSubBatch)

			if (cb)
				cb();
			nextSubatch();
		}

		function nextSubatch(){

			if (arrSubBatches.length === 0)
				return;
			exports.startSubBatch(arrSubBatches.shift());
		}
	}

}

var createReadStream = function(options){
	this.assocResults = {};
	this.arrResults = [];
	kvstore.createReadStream(options)
	.on('data', handleData)
	.on('end', onEnd);

	function handleData(data){
		if (kvCacheDel[data.key] || kvCachePut[data.key])
			return;
		this.arrResults.push(data);
	}

	function onEnd(){
		for (var key in kvCachePut){
			if (options.gt && key <= options.gt){
				break;
			}
			if (options.gte && key < options.gte){
				break;
			}
			if (options.lt && key >= options.lt){
				break;
			}
			if (options.lte && key > options.lte){
				break;
			}
			this.arrResults.push({key: key, value: kvCachePut[key]});
		}
		this.arrResults.sort((a,b)=>{a.key > b.key ? 1 : -1});
		var limit = options.limit || this.arrResults.length;
		for (var i; i < limit; i++)
			this.emit('data', this.arrResults[i]);
		this.emit('end');
	}
	return this;
}

util.inherits(createReadStream, EventEmitter);

exports.hasOnGoingBatch = function() {return bOngoingBatch;};