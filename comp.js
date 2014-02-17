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
        pusher(callback);
    },

    function(callback){
        tunecore(callback);
    },
    function(callback){
        audiosocket(callback)
    }
]);

//FUNCTIONS ****
    
function pusher(callback){
    reader.addListener('data', function(data){
        var silo = [];
        silo.push(data['Asset ID'].split('|'));
        for(i in silo){
            for(j in silo[i]){
                var stream = db.collection('assets').find({"assetId":silo[i][j]}).stream();
                stream.on("data", function(item){
                    console.log(item);
                    if(item.notes === "TUNECORE"){
                        tSilo.push(item.id);
                        combined.push(item.id);
                    }
                    else if(item.policy === "TuneCore Default Policy"){
                        tSilo.push(item.id);
                        combined.push(item.id);
                    }
                    else if(item.notes === "AUDIOSOCKET"){
                        aSilo.push(item.id);
                        combined.push(item.id);
                    }
                })
            }
        }
    })
    reader.addListener('end', function(){
        callback();
    })
}    


function tunecore(callback){
    reader.addListener('data', function(data){
        
        var silo = [];
	    silo.push(data['Asset ID'].split('|'));
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log(silo[i][j]);
                loop3:
                for(k in tSilo){
                    if(silo[i][j] === tSilo[k]){
                        console.log("found a match for TUNECORE");
                        fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"TUNECORE"+"\n");
                        break loop1;
                    }
                }
        /*        
		//var stream = db.collection('assets').find({"assetId":silo[i][j]}).stream();
            db.collection('assets').find({"assetId":silo[i][j]}), function(err, item){
                //console.log(err);
            //stream.on("data", function(item){
			console.log(item);
			if(item.notes != null){
			    console.log("I JUST WROTE A LINE WITH NOTES");
			    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+item.notes+"\n")
			}
			else if(item.policy === 'TuneCore Default Policy'){
                    console.log("TUNECORE MATCH POLICY");
                    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"TUNECORE"+"\n");
			}
                else{
                    console.log(item.assetId+" HAS NO NOTES");
                    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"\n");
                }
		    }
            }
        }
	})
	reader.addListener('end', function(data){
		callback();
	    })  */
	}

    }
    })
    
reader.addListener('end', function(){
    callback();
})

}
    
function audioSocket(callback){
    reader.addListener('data', function(data){
        var silo = [];
        silo.push(data['Asset ID']).split('|');
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log(silo[i][j]);
                loop3:
                for(k in aSilo){
                    if(silo[i][j] === aSilo[k]){
                        console.log("found a match for TUNECORE");
                        fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"AUDIOSOCKET"+"\n");
                        break loop1;
                    }
                }
            }
        }
    })
    reader.addListener('data', function(){
        callback();
    })
}
    
function combined(callback){
    reader.addListener('data', function(data){
        var silo = [];
        silo.push(data['Asset ID']).split('|');
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log(silo[i][j]);
                loop3:
                for(k in combined){
                    if(silo[i][j] === combined[k]){
                        console.log("MATCH!")
                        break loop1;
                    }
                    else{
                        fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+""+"\n");
                        break loop1;
                    }
                }
            }
        }
    })
    reader.addListener('end', function(){
        callback();
    })
}

});