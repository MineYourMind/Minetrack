var util = require('./util');

exports.setup = function() {
	var useMongo = true;
	if (useMongo) {
		var mongoose = require('mongoose');
		mongoose.Promise = global.Promise;
		mongoose.connect('mongodb://localhost/minetrack', {
			useMongoClient: true,
		});

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

		exports.getTotalRecord = function(ip, lastNDays, callback) {
			var match = {ip: ip};
			if (lastNDays) {
				match.timestamp = {$gte: new Date() - lastNDays * 24 * 60 * 60 * 1000};
			}
			Ping
				.findOne(match, {playerCount: 1, _id: 0})
				.sort('-playerCount')
				.limit(1)
				.exec(function (err, data) {
					if (err) throw err;
					callback(data ? data['playerCount'] : 0);
				});
		};

		exports.getAveragePlayers = function(ip, lastNDays, callback) {
			var match = {ip: ip, playerCount: {$gt: 0}};
			if (lastNDays) {
				match.timestamp = {$gte: new Date() - lastNDays * 24 * 60 * 60 * 1000};
			}
			Ping
				.aggregate([
					{$match: match},
					{$group: {_id: null, players: {$sum: "$playerCount"}, count: {$sum: 1}}}
				]).exec(function (err, data) {
					if (err) throw err;
					callback(data && data[0] ? data[0] : 0);
				});
		};

		exports.queryPings = function(duration, callback) {
			var currentTime = util.getCurrentTimeMs();
			/*Ping
				.find({timestamp: {
					$gt : currentTime - duration,
					$lt : currentTime
				}})
				.exec(function (err, data) {
					if (err) throw err;
					callback(data);
				});*/
			Ping
				.aggregate([
					{
						$match: {
							playerCount: {
								$gt: 0
							},
							timestamp: {
								$gt: currentTime - duration,
								$lt: currentTime
							}
						}
					},
					{
						$group : {
							_id: {
								ip: "$ip",
								hour: {
									$hour: {
										$add: [
											new Date(0),
											{ $multiply: [1, "$timestamp"] }
										]
									}
								},
								min: {
									$minute: {
										$add: [
											new Date(0),
											{ $multiply: [1, "$timestamp"] }
										]
									}
								}
							},
							timestamp: {$first: '$timestamp'},
							ip: {$first: '$ip'},
							playerCount: {$first: "$playerCount"},
							//playerCount: {$avg: "$playerCount"},
						}
					},
					{
						$sort : {
							timestamp : 1
						}
					},
					{
						$project : {
							_id : 0,
							timestamp: 1,
							ip : 1,
							playerCount: 1,
							//playerCount: { $trunc: "$playerCount" }
						}
					}
				])
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
