var express = require('express');
// var logger = require('morgan');
// var cookieParser = require('cookie-parser');
// var bodyParser = require('body-parser');
// var package = require("./package.json");
var fs = require('fs');
// var stringify = require('csv-stringify');

var http = require('http');
// var app = express();
var csv = require('csv');
var stringify = require('csv-stringify');
// Set the region 
var aws = require('aws-sdk')
// aws.config.update({region: 'Asia Pacific (Mumbai) ap-south-1'});
// // Create S3 service object
// s3 = new aws.S3({apiVersion: '2006-03-01'});
s3 = new aws.S3({
    accessKeyId: 'AKIAY3VA26JRQ5IMJ42W',
    secretAccessKey: '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',
    region :'us-east-1'
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

var redshiftClient = new Redshift(client, {rawConnection: true});
console.log('Here1');
console.log(redshiftClient);
// Parse URL-encoded bodies (as sent by HTML forms)
app.use(express.urlencoded());

// Parse JSON bodies (as sent by API clients)
app.use(express.json());

app.post('/', function(request, response){
    console.log(request.body);
    // var body = [];
    var body = JSON.stringify(request.body);
    // body.push(request.body.test) ;
    var file_name = 'items_'+ Date.now() + '.txt';
        
    // csv.stringify(body, {
    //     delimiter: '|'
    // }, function (err, data) {
    console.log(body);
    body ='2,test';
        var params = {
            Bucket: 'aws-bucket-2021',
            Key: file_name,
            Body: body
        };
    
        var options = {partSize: 10 * 1024 * 1024, queueSize: 1};
    
        s3.upload(params, options, function(s3err, result) {
            if (s3err) {
                //handle the error
                console.log('s3 error');
                console.log(s3err);
            }
            else {
                console.log('s3 done in ');
                //Upload successful. You can delete the S3 files
                // console.log('s3 done');
    var prefix = 'items_';

//     redshiftClient.rawQuery(`SELECT * FROM "test1"`, {raw: true})
// .then(function(data){
//   console.log(data); 
// })
// .catch(function(err){
//   console.log(err);
// });

    redshiftClient.connect(function(err){
                if (err) {
                    console.log('here error');
                    console.error(err);
                    // release();
                } else {
                console.log('Inside redshift');
                    var pg_query = "copy test1 from 's3://aws-bucket-2021/" + file_name + "ACCESS_KEY_ID 'AKIAY3VA26JRQ5IMJ42W' SECRET_ACCESS_KEY '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';";
                    redshiftClient.query(pg_query, function (err1, pgres) {
                        //query completed, we can close the connection
                        release();
                        if (err1) {
                            console.log('error here');
                            console.error(err1);
                        } else {
                            //upload successful
                        }
                    });
                    redshiftClient.close(); 
            }
        });
    }
    });

    return('done');

});
// });
// });
// http.createServer(function (req, res) {
//     console.log('Here');
//     // if (req.method === "GET") {
//     //     //not needed now
//     //     res.writeHead(200, { "Content-Type": "text/html" });
//     //     fs.createReadStream("./public/form.html", "UTF-8").pipe(res);
//     // } else 
//     if (req.method === "POST") {
//         console.log('body');
//         var body = [];
//         console.log(req);
//         req.on("data", function (chunk) {
//             console.log(chunk);
//             console.log('chunk');
//             body.push(chunk.toString()) ;
//         });

//         console.log(body);    
//         // stringify(body, {
//         //     header: true
//         // }, function (err, output) {
//         //     fs.writeFile(__dirname+'/new.csv', output);
//         // })
       
//         /*
//         the rest of your code
//         */
        
//         var file_name = 'items_'+ Date.now() + '.txt';
        
//         csv.stringify(body, {
//             delimiter: '|'
//         }, function (err, data) {
        
//             var params = {
//                 Bucket: 'my-bucket-2021',
//                 Key: file_name,
//                 Body: data
//             };
        
//             var options = {partSize: 10 * 1024 * 1024, queueSize: 1};
        
//             aws.s3.upload(params, options, function(s3err, result) {
//                 if (s3err) {
//                     //handle the error
//                 }
//                 else {
//                     //Upload successful. You can delete the S3 files
//                 }
//             });
//         });

//         var prefix = 'items_';
//         redshiftClient.connect(function(err){
//     if (err) {
//         console.error(err);
//         release();
//     } else {

//         var pg_query = "copy table_name from 's3://my-bucket-2021/" + prefix + "' credentials 'ACCESS_KEY_ID 'AKIAIOYG3QNKTNZUMXDQ' SECRET_ACCESS_KEY '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'' escape delimiter as '|' MAXERROR 10;";
//         redshiftClient.query(pg_query, function (err1, pgres) {
//             //query completed, we can close the connection
//             release();
//             if (err1) {
//                 console.error(err1);
//             } else {
//                 //upload successful
//             }
//         });

//     }
// });

//         req.on("end", function(){
//             res.writeHead(200, { "Content-Type": "text/html" });
//             res.end(body);
//         });
//     }

// }).listen(3000);
