
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
var queryTextInsert = 'INSERT INTO suned_redshift (suned_cust_id, quote_system_size, quote_ef_cost_per_watt, quote_year1_production, cust_pre_payment, quote_master_lease_pay_esc_rate, quote_rebate, quote_hipbi_year1_value, quote_hipbi_tenure, quote_hipbi_annual_derate, quote_state_tax_rate, quote_current_utility_cost, quote_post_solar_utility_cost, quote_proposal_id, quote_call_version_id, quote_auth_code, system_module_id, system_module_quantity, system_inverter_id, system_inverter_quantity, system_mounting_type, contract_calcmap_current_date, contract_installer_client_name, contract_calcmap_dealer_name, contract_calcmap_howner_0_first_name, contract_calcmap_howner_0_last_name, contract_calcmap_howner_1_first_name, contract_calcmap_howner_1_last_name, contract_product_type, contract_calcmap_n_of_howners, contract_calcmap_howner_0_address, contract_calcmap_howner_0_city, contract_calcmap_howner_0_state, contract_calcmap_howner_0_zipcode, contract_calcmap_howner_0_phone, contract_calcmap_howner_0_email, contract_calcmap_howner_1_address, contract_calcmap_howner_1_city, contract_calcmap_howner_1_state, contract_calcmap_howner_1_zipcode, contract_calcmap_howner_1_phone, contract_calcmap_howner_1_email, contract_calcmap_howner_2_address, contract_calcmap_howner_2_city, contract_calcmap_howner_2_state, contract_calcmap_howner_2_zipcode, contract_calcmap_howner_2_phone, contract_calcmap_howner_2_email, contract_installer_client_phone, contract_production_0_col2, contract_calcmap_lifetime_kwh, array_number, module_type, module_quantity, shading, tilt, azimuth, orientation, monthly_production_values, degradation_rates) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60)';

//Query string to fetch data from Redshift. 
var queryFetch = 'SELECT * from suned_redshift where suned_cust_id = $1 order by array_number asc';

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
        var UniqueFinancialRunId = inbou.UniqueFinancialRunId;
        var Timestamp = inbound_payload.Timestamp;
        var FinancialModelVersion = inbound_payload.FinancialModelVersion;
        var CallVersionId = inbound_payload.CallVersionId; 	

		var suned_id = parseInt(SunEdCustId, 10);

		//Establishing connection to Redshift using postgres. 
		pg.connect(conString, function(err,client){
			if(err){
				return console.log("Connection error. ", err);
			}
			console.log("Connection Established under fetch");

			//Querying redshift. 
			client.query(queryFetch, [SunEdCustId], function(err,result){
				if(err){
					console.log("Error returning query", err);
					context.done("Fatal Error");
				}
				console.log("Number of rows: ", result.rows.length);
				console.log("Number of rows from JSON" + inbound_payload.Array.length);

				//Algorithm to check redundancy and add unique data into redshift. 
				for(var m=0;m<inbound_payload.Array.length;m++){
                  	
                  	//Insert all the data from JSON file if no data exists in Redshift. 
                  	if(result.rows.length == 0){
                        console.log("No records in Redshift");
                        insertIntoRedshift(m);
                  	}

                  	//Check for duplicacy and insert rows to redshift. 
                  	else{
                  		for(var k=0;k<result.rows;k++){
	                    	if(result.rows[k].suned_cust_id == SunEdCustId && result.rows[k].array_number == inbound_payload.Array[m].ArrayNumber){
	                        	console.log("Duplicate Row Exists.");
	                        	break;           
                        	}
	                    	else if(k == result.rows.length-1){
	                        	insertIntoRedshift(m);
	                    	} 
	                	}
                  	}   	
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
       			client.query(queryTextInsert, [suned_id, SystemSize, EFCostPerWatt, Year1Production, CustomerPrepayment, MasterLeasePaymentEscalationRate, Rebate, HIPBIYear1Value, HIPBITenure, HIPBIAnnualDerate, StateTaxRate, CurrentUtilityCost, PostSolarUtilityCost, ProposalID, CallVersionID, AuthorizationCode, ModuleId, ModuleQuantity, InverterId, InverterQuantity, MountingType, currentDate, installerClientName, dealerName, homeownerList_0_firstName, homeownerList_0_lastName, homeownerList_1_firstName, homeownerList_1_lastName, product_type, numberOfHomeowners, homeownerList_0_address, homeownerList_0_city, homeownerList_0_state, homeownerList_0_zipcode, homeownerList_0_phone, homeownerList_0_email, homeownerList_1_address, homeownerList_1_city, homeownerList_1_state, homeownerList_1_zipcode, homeownerList_1_phone, homeownerList_1_email, homeownerList_2_address,homeownerList_2_city, homeownerList_2_state, homeownerList_2_zipcode, homeownerList_2_phone, homeownerList_2_email, installerClientPhone, productionList_0_col2, lifeTimeKwh, inbound_payload.Array[m].ArrayNumber, inbound_payload.Array[m].ModuleType, inbound_payload.Array[m].ModuleQuantity, inbound_payload.Array[m].Shading, inbound_payload.Array[m].Tilt, inbound_payload.Array[m].Azimuth, inbound_payload.Array[m].Orientation, inbound_payload.Array[m].monthlyProductionValues, inbound_payload.Array[m].DegradationRate], function(err,result){
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
