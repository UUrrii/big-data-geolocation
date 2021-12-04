const express = require('express');
const app = express();

const mysql = require('mysql2/promise');

const ipRangeCheck = require('ip-range-check');
const path = require('path');
const PORT = 3000;
  
// Static Middleware 
app.use(express.static(path.join(__dirname, 'public')))
    
app.get('/', function(req, res) {
    let options = {
        root: path.join(__dirname + '/public')
    }
    res.sendFile(path.join(__dirname, 'index.html', options));
});

app.get('/ip/:ip', async function (req, res) {
    const mysqlConnection = await mysql.createConnection({
        host: 'smoothcloud.de',
        port: '3306',
        user: 'geo',
        password: 'geolite_pw',
        database: 'geolite'
    });

    const ip = req.params.ip;
    let geoname_id;

    const [ipv4_results] = await mysqlConnection.execute(`SELECT network, geoname_id FROM ipv4_table`);
    for (const ipv4_result of ipv4_results) {
        if (ipRangeCheck(ip, ipv4_result.network)) {
            geoname_id = ipv4_result.geoname_id;
            break;
        }
    }
    if (geoname_id) {
        const [data_result] = await mysqlConnection.execute(`SELECT continent_name, country_name, subdivision_1_name, city_name FROM data_table WHERE geoname_id = ${geoname_id}`);
        res.send(data_result[0]);
    } else {
        res.send({
            continent_name: 'not found',
            country_name: 'not found',
            subdivision_1_name: 'not found',
            city_name: 'not found'
        });
    }
});
  
app.listen(PORT, function(err){
    if (err) console.log(err);
    console.log("Server listening on PORT", PORT);
});

