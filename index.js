var express = require('express');

// var csv = require('csv');
var stringify = require('csv-stringify');
// Set the region 
var aws = require('aws-sdk')
// aws.config.update({region: 'Asia Pacific (Mumbai) ap-south-1'});
// // Create S3 service object
// s3 = new aws.S3({apiVersion: '2006-03-01'});
s3 = new aws.S3({
    accessKeyId: 'AKIAY3VA26JR7LH4FG5O',
    secretAccessKey: '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',
    region: 'us-east-1'
});
// const express = require('express');

var app = express();

app.listen(3000, () => {
    console.log('Server is up on port 3000');
});
//redshift.js
var Redshift = require('node-redshift');

var client = {
    user: 'awsuser',
    database: 'dev',
    password: 'Aws%2020',
    port: '5439',
    host: 'redshift-cluster-1.cxkpwzrvcnrm.us-east-1.redshift.amazonaws.com',
};

var redshiftClient = new Redshift(client);
// Parse URL-encoded bodies (as sent by HTML forms)
app.use(express.urlencoded());

// Parse JSON bodies (as sent by API clients)
app.use(express.json());

app.post('/', function (request, response) {
    console.log(request.body);
    // var body = [];
    // var body = JSON.stringify(request.body);
    // body.push(request.body.test) ;
    var file_name = 'items.txt';
    csvbody = [Object.values(request.body)];
    // console.log(body);
    console.log(csvbody);
    stringify(csvbody, function (err, data) {
        console.log(data);
        var params = {
            Bucket: 'aws-bucket-2021',
            Key: file_name,
            Body: data
        };

        var options = { partSize: 10 * 1024 * 1024, queueSize: 1 };

        s3.upload(params, options, function (s3err, result) {
            if (s3err) {
                //handle the error
                console.log('s3 error');
                console.log(s3err);
            }
            else {
                console.log('s3 done in ');
                //Upload successful. You can delete the S3 files
                var pg_query = "copy test1 from 's3://aws-bucket-2021/items.txt' " + "ACCESS_KEY_ID 'AKIAY3VA26JR7LH4FG5O' SECRET_ACCESS_KEY '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' escape delimiter as ',';";

                redshiftClient.query(pg_query, { raw: true }, function (err1, pgres) {
                    //query completed, we can close the connection
                    // release();
                    if (err1) {
                        console.log('error here');
                        console.error(err1);
                        response.send('error');
                    } else {
                        //upload successful
                        console.log('upload success');
                        response.send('success');

                    }
                });
                //  redshiftClient.close(); 
                console.log('outside');
            }
        });
    });

});