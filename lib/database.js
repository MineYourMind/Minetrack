var util = require('./util');

exports.setup = function() {
	var useMongo = true;
	if (useMongo) {
		var mongoose = require('mongoose');
		mongoose.Promise = global.Promise;
		mongoose.connect('mongodb://localhost/minetrack');

		var Schema = mongoose.Schema;

		var pingSchema = new Schema({
			timestamp 	: Number,
			ip        	: String,
			playerCount	: Number,
		});

		var Ping = mongoose.model('Pings', pingSchema);


		exports.log = function(ip, timestamp, playerCount) {
			var ping = new Ping({
				timestamp: timestamp,
				ip: ip,
				playerCount: playerCount
			});

			ping.save(function(err) {
				if (err) throw err;
			});
		};

		exports.getTotalRecord = function(ip, callback) {
			Ping
				.findOne({ip: ip}, {playerCount: 1, _id: 0})
				.sort('-playerCount')
				.exec(function (err, data) {
					if (err) throw err;
					callback(data['playerCount']);
				});
		};

		exports.queryPings = function(duration, callback) {
			var currentTime = util.getCurrentTimeMs();
			Ping
				.find({timestamp: {
					$gt : currentTime - duration,
					$lt : currentTime
				}})
				.exec(function (err, data) {
					if (err) throw err;
					callback(data);
				});
		};

	} else {

		var sqlite = require('sqlite3');

		var db = new sqlite.Database('database.sql');

		db.serialize(function() {
			db.run('CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip TINYTEXT, playerCount MEDIUMINT)');
			db.run('CREATE INDEX IF NOT EXISTS ip_index ON pings (ip, playerCount)');
			db.run('CREATE INDEX IF NOT EXISTS timestamp_index on PINGS (timestamp)');
		});

		exports.log = function(ip, timestamp, playerCount) {
			var insertStatement = db.prepare('INSERT INTO pings (timestamp, ip, playerCount) VALUES (?, ?, ?)');

			db.serialize(function() {
				insertStatement.run(timestamp, ip, playerCount);
			});

			insertStatement.finalize();
		};

		exports.getTotalRecord = function(ip, callback) {
			db.all("SELECT MAX(playerCount) FROM pings WHERE ip = ?", [
				ip
			], function(err, data) {
				callback(data[0]['MAX(playerCount)']);
			});
		};

		exports.queryPings = function(duration, callback) {
			var currentTime = util.getCurrentTimeMs();

			db.all("SELECT * FROM pings WHERE timestamp >= ? AND timestamp <= ?", [
				currentTime - duration,
				currentTime
			], function(err, data) {
				callback(data);
			});
		};
	}
};
