'use strict';

const http = require('http'),
    request = require('request'),
    url = require('url'),
    sql = require('mssql'),
    moment = require('moment'),
    Log = require('log'),
    fs = require('fs'),
    dbConfig = {
        user: 'test',
        password: 'test',
        server: '127.0.0.1',
        database: 'test'
    },
    apiKey = 'test',
    port = 8000,
    log = new Log('info', fs.createWriteStream('log.log'));

function checkPreviousPoint(point) {
    let sqlRequest = new sql.Request();

    sqlRequest.input('unit_id', sql.Int, point.unit_id);
    sqlRequest.input('date_time', sql.VarChar, point.date_time);

    let stmt = `
		SELECT TOP 1 
			ignition_status
		FROM EVENTS_LKS
		WHERE unit_id = @unit_id
		AND date_time <= @date_time
		ORDER BY date_time DESC
	`;

    sqlRequest.query(stmt, function (err, result) {
        if (err) {
            log.error(err.message);
            return;
        }

        if (result.recordset.length > 0) {
            point.ignition_status = result.recordset[0].ignition_status;
        }

        insertPoint(point);
    });
}

function insertPoint(point) {
    let sqlRequest = new sql.Request();

    sqlRequest.input('unit_id', sql.Int, point.unit_id);
    sqlRequest.input('date_time', sql.VarChar, point.date_time);
    sqlRequest.input('northing', sql.Numeric(15, 7), point.northing);
    sqlRequest.input('easting', sql.Numeric(15, 7), point.easting);
    sqlRequest.input('speed', sql.Int, point.speed);
    sqlRequest.input('heading', sql.Int, point.heading);
    sqlRequest.input('ignition_status', sql.Bit, point.ignition_status);

    let stmt = `
		INSERT INTO EVENTS_LKS (
			unit_id,
			date_time,
			northing,
			easting,
			speed,
			heading,
			ignition_status,
			received_time
		) VALUES (
			@unit_id,
			@date_time,
			@northing,
			@easting,
			@speed,
			@heading,
			@ignition_status,
			GETDATE()
		)
	`;

    sqlRequest.query(stmt, function (err, result) {
        if (err) {
            log.error(err.message);
            return;
        }

        checkUnit(point);
    });
}

function checkUnit(point) {
    let sqlRequest = new sql.Request();

    sqlRequest.input('unit_id', sql.Int, point.unit_id);

    let stmt = `
		SELECT TOP 1 id
		FROM UNITS
		WHERE id = @unit_id
	`;

    sqlRequest.query(stmt, function (err, result) {
        if (err) {
            log.error(err.message);
            return;
        }

        if (result.recordset.length === 0) {
            fetchUnitData(point);
        }
    });
}

function fetchUnitData(point) {
    let api_url = 'https://mapon.com/api/v1/unit/list_one.json?unit_id=' + point.unit_id + '&key=' + apiKey;

    request(api_url, {json: true}, (err, result, body) => {
        if (err) {
            log.error(err.message);
            return;
        }

        if (body.data === undefined) {
            log.info('cannot fetch unit %s data from API', point.unit_id);
            return;
        }

        let unit = body.data;
        let sqlRequest = new sql.Request();

        sqlRequest.input('id', sql.Int, unit.unit_id);
        sqlRequest.input('sim_number', sql.NVarChar, unit.sim_number);
        sqlRequest.input('car_number', sql.NVarChar, unit.car_number);
        sqlRequest.input('car_label', sql.NVarChar, unit.car_label);
        sqlRequest.input('car_nickname', sql.NVarChar, unit.car_nickname);
        sqlRequest.input('depot', sql.NVarChar, unit.depot);
        sqlRequest.input('fuel_type', sql.NVarChar, unit.fuel_type);
        sqlRequest.input('fuel_tank', sql.Numeric(10, 6), unit.fuel_tank);
        sqlRequest.input('avg_fuel_consumption', sql.Numeric(10, 6), unit.avg_fuel_consumption);
        sqlRequest.input('custom_57116', sql.NVarChar, unit.custom_57116);
        sqlRequest.input('custom_57262', sql.NVarChar, unit.custom_57262);
        sqlRequest.input('custom_64464', sql.NVarChar, unit.custom_64464);
        sqlRequest.input('custom_81639', sql.NVarChar, unit.custom_81639);

        let stmt = `
			INSERT INTO UNITS (
				id,
				sim_number,
				car_number,
				car_label,
				car_nickname,
				depot,
				fuel_type,
				fuel_tank,
				avg_fuel_consumption,
				custom_57116,
				custom_57262,
                custom_64464,
                custom_81639
			) VALUES (
				@id,
				@sim_number,
				@car_number,
				@car_label,
				@car_nickname,
				@depot,
				@fuel_type,
				@fuel_tank,
				@avg_fuel_consumption,
				@custom_57116,
				@custom_57262,
                @custom_64464,
                @custom_81639
			)
        `;

        sqlRequest.query(stmt, ()=>{});
    });
}

const server = http.createServer(function (req, res) {
    let query = url.parse(req.url, true).query;

    if ((query.unit_id !== undefined) && (query.datetime !== undefined) && (query.n !== undefined) &&
        (query.e !== undefined) && (query.speed !== undefined) && (query.direction !== undefined)) {

        res.writeHead(200, {'Content-Type': 'text/plain; charset=UTF-8'});
        res.end('OK');

        if (query.direction === 'NULL') {
            return;
        }

        let point = {
            unit_id: query.unit_id,
            date_time: moment(query.datetime).add(moment().utcOffset(), 'minutes').format('YYYY-MM-DD HH:mm:ss'),
            northing: query.n,
            easting: query.e,
            speed: query.speed,
            heading: query.direction,
            ignition_status: query.ignition_status !== undefined ? query.ignition_status === '1' : undefined
        };

        if (point.ignition_status === undefined) {
            checkPreviousPoint(point);
        }
        else {
            insertPoint(point);
        }
    } else {
        let msg = "Incorrect parameters: '" + JSON.stringify(query) + "'";

        res.writeHead(422, {'Content-Type': 'text/plain; charset=UTF-8'});
        res.end(msg);
        log.info(msg);
    }
});

process.on('uncaughtException', function (err) {
    log.error(err.message);
});

sql.connect(dbConfig, function (err) {
    if (err) {
        log.error(err.message);
        process.exit();
    } else {
        server.listen(port);
        log.info('Server running at http://127.0.0.1:%d/', port)
    }
});
