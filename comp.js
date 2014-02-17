//Reconcilles report assets against our MongoHQ database.

var csv = require('ya-csv');
var util = require('util');
var fs = require('graceful-fs');
var async = require('async');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var Server = require('mongodb').Server;
var MONGOHQ_URL="mongodb://indmusic:247MCNetwork@zach.mongohq.com:10094/INDMUSIC_main";

var tSilo = [];
var aSilo = [];
var combined = [];
var matches = [];

MongoClient.connect(MONGOHQ_URL, function(err, db){

var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});

//push values from the first csv file to an array for later comparison


async.series([

	      function(callback){
		      getNotes(callback);
	      }
	      


//open the second csv file and compare a value to the entire length of the array and do an operaiton when theres a match
/*
function(callback){
reader.addListener('data', function(data){
    for(i=0;i<aSilo.length;i++){
	if(data['Asset ID'] === aSilo[i]){
	    console.log("Wrote AudioSocket Line");
	    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+",AUDIOSOCKET"+"\n");
	    break;
	}
    }
    });
callback();
},*/
/*
reader.addListener('end', function(){
    callback();
});*/

/*
function(callback){
reader.addListener('data', function(data){
    for(i=0;i<tSilo.length;i++){
	if(data['Asset ID'] === tSilo[i]){
	    console.log("Wrote TuneCore Line");
	    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+",TUNECORE"+"\n");
	    break;
	}
    }
});
callback();
}*/
/*
reader.addListener('end', function(){
    callback();
});
*/
/*
function(callback){
reader.addListener('data', function(data){
    for(i=0;i<combined.length;i++){
	if(data[0] === combined[i]){
	    console.log("MATCH!  DONT WRITE")
	    break;
	}

	else{
	    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+"\n");
	}
    }
});
callback();
}*/
]);

//FUNCTIONS ****

function getNotes(callback){
    reader.addListener('data', function(data){
        var silo = [];
        var assets = [];
	    silo.push(data['Asset ID'].split('|'));
        for(i in silo){
            for(j in silo[i]){
                //console.log(silo[i][j]);
	            var stream = db.collection('assets').find({"assetId":silo[i][j]}).stream();
                stream.on("data", function(item){
                console.log(item);
                if(item.notes != null){
                    console.log("I JUST WROTE A LINE WITH NOTES");
                    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+item.notes+"\n");
                }
                else if(item.policy === 'TuneCore Default Policy'){
                    console.log("TUNECORE MATCH POLICY");
                    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"TUNECORE"+"\n");
                }
                else{
                    console.log(item.assetId+" HAS NO NOTES");
                    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+"\n");
                }
                })
            }
        }
    })
    reader.addListener('end', function(data){
        callback();
    })
	}

    });