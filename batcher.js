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

var bCommit = false;
var timerId;

var kvCachePut = {};
var kvCacheDel = {};

exports.query = function(sql, args, cb) {
	if (bOngoingBatch)
		conn.query(sql, args, cb);
	else
		db.query(sql, args, cb);
}

exports.startSubBatch = function(callback){
	var self = this;
	if (!bOngoingBatch){
		savepoint_index = 0;
		bCommit = false;
		db.flagOnGoingBatch();
		self.kvCachePut = {};
		self.kvCacheDel = {};

		timerId = setTimeout(function(){
			bCommit = true;
		}, 1000);
		batch = kvstore.batch();
		db.takeConnectionFromPool(function (_conn) {
			self.conn = _conn;
			conn = _conn;
			self.conn.query("BEGIN", function(){
				self.savepoint_index = savepoint_index;
				savepoint_index++;
				self.conn.query("SAVEPOINT spt_" +  self.savepoint_index, function(){
					callback(objSubBatchFunctions);
				});
			});
		})
	} else {
		self.conn = conn;
		self.savepoint_index = savepoint_index;
		savepoint_index++;
		self.conn.query("SAVEPOINT spt_" +  self.savepoint_index, function(){
			callback(objSubBatchFunctions);
		});
	}

	var objSubBatchFunctions = {
		release:function(cb){
			self.conn.query("RELEASE spt_" + self.savepoint_index, function(){
				commitIfNecessary(cb);
			})
		},
		rollback:function(cb){
			self.conn.query("ROLLBACK TO spt_" + self.savepoint_index, function(){
				commitIfNecessary(cb);
			})
		},

		sql: {
			query: self.conn.query,
			cquery: self.conn.cquery,
			addQuery: self.conn.addQuery,
			getFromUnixTime: self.conn.getFromUnixTime,
			getIgnore: self.conn.getIgnore
	
		},
		kv: {
			get: function(key, cb){
				if (self.kvCachePut[key])
					return cb(null, self.kvCachePut[key]);
				else if (self.kvCacheDel[key])
					return cb({notFound: true, type: 'NotFoundError'});
				else
					return kvstore.get(key, cb);
			},
			put: function(key, value){
				self.kvCachePut[key] = value;
				delete self.kvCacheDel[key];
			},
			del: function(key){
				self.kvCacheDel[key] = true;
				delete self.kvCachePut[key];
			},
			createReadStream: function(options){
				return new createReadStream(self, options)
			},
			write: function(cb){
				for (var key in self.kvCacheDel){
					batch.del(key);
					kvCacheDel[key] = true;
				}
				for (var key in self.kvCachePut){
					batch.put(key, self.kvCachePut[key]);
					kvCachePut[key] = self.kvCachePut[key];
				}
				self.kvCachePut = {};
				self.kvCacheDel = {};

				cb();
			}


		}

	}
	
	function commitIfNecessary(cb){
		if (bCommit){
			batch.write({sync: true}, function(){
				self.conn.query("COMMIT", function(){
					savepoint_index = 0;
					bCommit = false;
					bInBatch = false;
					db.unflagOnGoingBatch();
					self.conn.release();
					self.conn = null;
					cb();
				});
			});
		}
		else
			cb();
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