var path = require('path');
var shell = require('child_process').execSync;
var _ = require('lodash');
process.env.devnet = 1;
var constants = require("../constants.js");
var objectHash = require("../object_hash.js");
var desktop_app = require('../desktop_app.js');
desktop_app.getAppDataDir = function () { return __dirname + '/.testdata-' + path.basename(__filename); }

var dst_dir = __dirname + '/.testdata-' + path.basename(__filename);
shell('rm -rf ' + dst_dir);

var batcher = require('../batcher.js');
var db = require('../db.js');

var test = require('ava');


const batchPeriod = 1000;


var ops_1 = [
	{ type: 'put', key: 'son', value: 'Bart' },
	{ type: 'del', key: 'daughter', value: 'Lisa' },
	{ type: 'put', key: 'father', value: 'Homer' },
	{ type: 'put', key: 'mother', value: 'Karen' },
	{ type: 'put', key: 'mother', value: 'Marge' },
	{ type: 'put', key: 'neighbor', value: 'Karl' },
	{ type: 'del', key: 'neighbor' },
	{ type: 'put', key: 'daughter', value: 'Lisa' },
	{ type: 'put', key: 'baby', value:'Maggie'},
  ];


test.serial.cb('read uncommited then committed kv sub batch', t => {
	var results = [];

	batcher.startSubBatch(async function(subBatch){
		ops_1.forEach(function(op){
			if (op.type == 'put')
				subBatch.kv.put(op.key, op.value);
			if (op.type == 'del')
				subBatch.kv.del(op.key);
		})

		await subBatch.sql.query("INSERT INTO addresses (address) VALUES (?)", ["26XAPPPTTYRIOSYNCUV3NS2H57X5LZLJ"]);

		batcher.createReadStream()
		.on('data', function (data) {
			results.push(data)
		})
		.on('close', function () {
			console.log('Stream closed')
		})
		.on('error', function (error) {
			console.log('Stream error: ' + error)
		})
		.on('end', async function () {
			console.log('Stream ended')
			t.deepEqual(results,[]);
			batcher.query("SELECT address FROM addresses", function(rows){ // query from batcher can read uncommitted sub batch
				t.deepEqual(rows,[{address: "26XAPPPTTYRIOSYNCUV3NS2H57X5LZLJ"}]);
				subBatch.kv.write(function(){
					subBatch.release(function(){
						batcher.createReadStream()
						.on('data', function (data) {
							results.push(data)
						})
						.on('close', function () {
							console.log('Stream closed')
						})
						.on('error', function (error) {
							console.log('Stream error: ' + error)
						})
						.on('end', function () {
							console.log('Stream ended')
							console.log(results);
							t.deepEqual(results,[ 
								{ key: 'baby', value: 'Maggie' },
								{ key: 'daughter', value: 'Lisa' },
								{ key: 'father', value: 'Homer' },
								{ key: 'mother', value: 'Marge' },
								{ key: 'son', value: 'Bart' } 
							]);
							t.end();
						})
					})
				})
			});
		});
	})
});


test.serial.cb('read uncommited then committed sql sub batch', t => {
	var bQueryExecuted = false;
	db.query("SELECT address FROM addresses", function(rows){
		console.log("query executed")
		bQueryExecuted = true;
		t.deepEqual(rows,[{address: "26XAPPPTTYRIOSYNCUV3NS2H57X5LZLJ"}]);
	});

	setTimeout(function(){
		if (bQueryExecuted)
			t.fail();
	}, batchPeriod - 100);
	setTimeout(function(){
		if (!bQueryExecuted)
			t.fail();
		t.end();
	}, batchPeriod + 100);
});



process.on('unhandledRejection', up => { throw up; });
