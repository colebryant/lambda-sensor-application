'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2])

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]), encoding: 'latin1'})

function counterToNumber(item) {
	if (item['column'].slice('-3') === 'Sum') {
		return Number(Buffer.from(item['$'], 'latin1').readBigInt64BE()) / 10000000;
	} else if (item['column'].slice('-5') === 'Total') {
		return Number(Buffer.from(item['$'], 'latin1').readBigInt64BE());
	} else {
		return item['$']
	}
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item)
	});
	return stats;
}

app.use(express.static('public'));
app.get('/sensors.html',function (req,res) {
    const sensor_id=req.query['sensor'];
	hclient.table('cwbryant_latest_readings_view').row(sensor_id).get(function (err, r_cells) {
		hclient.table('cwbryant_hourly_totals_view').row(sensor_id).get(function (err, t_cells) {
			const r_map = rowToMap(r_cells);
			const t_map = rowToMap(t_cells)

			function avg_water_level(hour) {
				var reading_total = t_map["totals:" + hour + "ReadingTotal"];
				var reading_sum = t_map["totals:" + hour + "ReadingSum"];
				if(reading_total === 0)
					return "-";
				return (reading_sum/reading_total).toFixed(3);
			}

			var template = filesystem.readFileSync("result.mustache").toString();
			var html = mustache.render(template,  {
				water_level : parseFloat(r_map['readings:WaterLevel']).toFixed(3),
				date : r_map['readings:ReadingDate'],
				hour : r_map['readings:ReadingHour'] + ':00',
				precipitation : r_map['readings:PRCP'],
				avg_temp : r_map['readings:TAVG'],
				high_temp : r_map['readings:TMAX'],
				low_temp : r_map['readings:TMIN'],
				avg_00 : avg_water_level('00'),
				avg_01 : avg_water_level('01'),
				avg_02 : avg_water_level('02'),
				avg_03 : avg_water_level('03'),
				avg_04 : avg_water_level('04'),
				avg_05 : avg_water_level('05'),
				avg_06 : avg_water_level('06'),
				avg_07 : avg_water_level('07'),
				avg_08 : avg_water_level('08'),
				avg_09 : avg_water_level('09'),
				avg_10 : avg_water_level('10'),
				avg_11 : avg_water_level('11'),
				avg_12 : avg_water_level('12'),
				avg_13 : avg_water_level('13'),
				avg_14 : avg_water_level('14'),
				avg_15 : avg_water_level('15'),
				avg_16 : avg_water_level('16'),
				avg_17 : avg_water_level('17'),
				avg_18 : avg_water_level('18'),
				avg_19 : avg_water_level('19'),
				avg_20 : avg_water_level('20'),
				avg_21 : avg_water_level('21'),
				avg_22 : avg_water_level('22'),
				avg_23 : avg_water_level('23')
			});
			res.send(html);
		});
	});
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
// var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaClient = new kafka.KafkaClient({kafkaHost: 'b-1.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092'});
var kafkaProducer = new Producer(kafkaClient);

app.get('/submit.html',function (req, res) {
	var sensor_id = req.query['sensor'];
	var water_level = req.query['water_level']

	// Get eastern time hour (sensors located in eastern timezone)
	const moment = require('moment-timezone')
	const hour_in_est = moment().tz('America/New_York').format('HH')
	var report = {
		sensor_id : sensor_id,
		water_level: water_level,
		reading_hour : hour_in_est
	};

	kafkaProducer.send([{ topic: 'cwbryant_readings', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(report);
			res.redirect('submit-reading.html');
		});
});

app.listen(port);
