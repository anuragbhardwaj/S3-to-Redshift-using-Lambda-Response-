
/****

********SunEdison*********

AUTHOR : Anurag Bhardwaj

****/

var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("AWS Lambda Redshift Database Loader using default region (For Response JSON) " + region);
}

//Requiring aws-sdk. 
var aws = require('aws-sdk');
aws.config.update({
	region : region
});

//Requiring S3 module. 
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});
//Requiring dynamoDB module. 
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});

//Requiring SNS module. 
var sns = new aws.SNS({
	apiVersion : '2010-03-31',
	region : region
});

require('./constants');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var pg = require('pg');
var upgrade = require('./upgrades');
var zlib = require('zlib');

//Connection string to connect to Redshift with username and password. 
var conString = "postgresql://abhardwaj:Master12@rs-instance.cysomezynckr.us-west-2.redshift.amazonaws.com:5439/mydb";

//Query string to insert data into Redshift. 
var queryTextInsert = 'INSERT INTO response (sunedcustid, pricingquoteid, customerleasepayments, downpayment, leaseterm, estimatedannualoutput, uniquefinancialrunid, terminationvalues, suned_timestamp, financialmodelversion, callversionid, guaranteedannualoutput) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)';

//Query string to fetch data from Redshift. 
var queryFetch = 'SELECT * from response where SunEdCustId = $1 and PricingQuoteId = $2';

// Main function for AWS Lambda
exports.handler = function(event, context) {

    // Get the object from the event and show its content type
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    s3.getObject({Bucket: bucket, Key: key}, function(err, data) {
        if (err) {
            console.log("Error getting object " + key + " from bucket " + bucket +
                ". Make sure they exist and your bucket is in the same region as this function.");
            context.fail ("Error getting file: " + err);      
        } else {
            console.log('CONTENT TYPE:', data.ContentType);
            var inbound_payload = JSON.parse(data.Body);

        	insertData(inbound_payload);
        }
    });

    //Method to insert data into Redshift. 
	var insertData = function(inbound_payload){

		//Local variables definition to get data from JSON file. 
		var SunEdCustId = inbound_payload.SunEdCustId;
        var PricingQuoteId = inbound_payload.PricingQuoteId;
        var DownPayment = inbound_payload.DownPayment;
        var LeaseTerm = inbound_payload.LeaseTerm; 
        var EstimatedAnnualOutput = inbound_payload.EstimatedAnnualOutput;
        var UniqueFinancialRunId = inbou.UniqueFinancialRunId;
        var Suned_Timestamp = inbound_payload.Timestamp;
        var FinancialModelVersion = inbound_payload.FinancialModelVersion;
        var CallVersionId = inbound_payload.CallVersionId; 	

		var suned_id = parseInt(SunEdCustId, 10);

		//Establishing connection to Redshift using postgres. 
		pg.connect(conString, function(err,client){
			if(err){
				return console.log("Connection error. ", err);
			}h");

            console.log("Connection Established under fetc
			//Querying redshift. 
			client.query(queryFetch, [SunEdCustId,PricingQuoteId], function(err,result){
				if(err){
					console.log("Error returning query", err);
					context.done("Fatal Error");
				}
				console.log("Number of rows: ", result.rows.length);
				console.log("Number of Arrays for CustomerLeasePayments in JSON: " + inbound_payload.CustomerLeasePayments.length);

                if(result.rows.length > 0){
                    context.done("Data already exisits for received customer ID.");
                }

				//Algorithm to check redundancy and add unique data into redshift. 
				for(var m=0;m<inbound_payload.LeaseTerm;m++){
                  	
                  	insertIntoRedshift(m);
                }
			});
		});

		//Method to run insert query to push data into redshift. 
		var insertIntoRedshift = function(m){
			pg.connect(conString, function(err,client){
        		if(err){
        			return console.log("Connection Error.", err);
       			}
       			console.log("Connection Established.");
       			client.query(queryTextInsert, [SunEdCustId, PricingQuoteId, inbound_payload.CustomerLeasePayments[m], DownPayment, LeaseTerm, EstimatedAnnualOutput, UniqueFinancialRunId, inbound_payload.TerminationValues[m], Suned_Timestamp, FinancialModelVersion, CallVersionId, inbound_payload.GuaranteedAnnualOutput[m]], function(err,result){
               		if(err){
                   		return console.log('Error returning query', err);
               		}
               		console.log('Row inserted. Go and check on Redshift: ' + result);
               		return client;
        		});
				
        	});	
		}
		
			
	}
};
