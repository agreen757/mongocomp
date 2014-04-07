//Reconcilles report assets against our MongoHQ database.

var csv = require('ya-csv');
var util = require('util');
var fs = require('graceful-fs');
var async = require('async');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var Server = require('mongodb').Server;
var MONGOHQ_URL="MONGOHQURL";

var tSilo = [];
var aSilo = [];
var combined = [];
var matches = [];

MongoClient.connect(MONGOHQ_URL, function(err, db){

var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});

//push values from the first csv file to an array for later comparison


async.series([
    
    /*function(callback){
        pusher1(callback);
    },*/
    function(callback){
	pusher(callback);
    }
]);

//FUNCTIONS ****

function pusher2(callback){
    var reader = csv.createCsvFileReader('tunecore2.csv', {columnsFromHeader:false,'separator': ','});
    reader.addListener('data', function(data){
	console.log(data[0]);
	tSilo.push(data[0]);
	combined.push(data[0]);
    })
    reader.addListener('end', function(){
	callback();
    })
}

function pusher3(callback){
    var reader = csv.createCsvFileReader('audiosocket2.csv', {columnsFromHeader:false,'separator': ','});
    reader.addListener('data', function(data){
        console.log(data[0]);
        aSilo.push(data[0]);
	combined.push(data[0]);
    })
    reader.addListener('end', function(){
	callback();
    })
}

function pusher1(callback){
    var k = 0;
    var a = setTimeout(function(){
	var tstream = db.collection('assets').find({"notes":"TUNECORE"}).stream();
	tstream.on('data', function(item){
	    console.log(item);
	    fs.appendFileSync('tunecore.csv', item.assetId+'\n');
	    tSilo.push(item.assetId);  
	});
	var astream = db.collection('assets').find({"notes":"AUDIOSOCKET"}).stream();
	astream.on('data', function(item){
	    console.log(item);
	    fs.appendFileSync('audiosocket.csv', item.assetId+'\n');
	    aSilo.push(item.assetId);  
	})
    }, k +=50);
}

    
function pusher(callback){
    var k = 0;
    var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});
    reader.addListener('data', function(data){
        var silo = [];
	var foo = false;
	var a = setTimeout(function(){
        silo.push(data['Asset ID'].split('|'));
	loop1:
	for(i in silo){
	    if(silo[i] != null){
		//console.log(silo[i]);
		loop2:
		for(j=0;j<silo[i].length;j++){
		    var b = function(){
			//console.log("processing asset "+silo[i][j])
			db.collection('assets').findOne({assetId:silo[i][j]}, function(err,doc){
				if(err){
				    console.log(err);
				    return true;
				}
				if(doc != null){
				    if(doc.notes === "TUNECORE"){
					console.log("TuneCore Video ID "+data["Video ID"]);
					return true;
				    }
				    else if(doc.notes === "AUDIOSOCKET"){
					console.log("AudioSocket Video ID "+data["Video ID"]);
					return true;
				    }
				    else{
					console.log("not tc or as "+silo[i][j]);
					return false;
				    }
				}
				else{
				    console.log("NOT IN DB "+silo[i][j]);
				    return false;
				}
			    })
		    }
		    b();
		    if(b = true){
			console.log("broken");
			break loop1;
		    }
		}
	    }
	}
	  }, k+=100);
	})
	
	reader.addListener('end', function(){
		console.log("end of pusher")
		    callback();
	    })
	}    



function tunecore(callback){
    var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});
    //var tFile = csv.createCsvFileReader('tunecore.csv', {columnsFromHeader:false,'separator': ','});
    reader.addListener('data', function(data){
        
        var silo = [];
	silo.push(data['Asset ID'].split('|'));
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log("tunecore "+silo[i][j]);
                loop3:
                for(k in tSilo){
		  //console.log(tSilo[k]+" vs "+silo[i][j]);  
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
    var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});
    reader.addListener('data', function(data){
        var silo = [];
        silo.push(data['Asset ID'].split('|'));
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log("audiosocket "+silo[i][j]);
                loop3:
                for(k in aSilo){
                    if(silo[i][j] === aSilo[k]){
                        console.log("found a match for AUDIOSOCKET");
                        fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+"AUDIOSOCKET"+"\n");
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
    
function ender(callback){
    var reader = csv.createCsvFileReader('raw.csv', {columnsFromHeader:true,'separator': ','});
    reader.addListener('data', function(data){
        var silo = [];
	var flipper = false;
        silo.push(data['Asset ID'].split('|'));
        loop1:
        for(i=0;i<silo.length;i++){
            loop2:
            for(j=0;j<silo[i].length;j++){
                console.log("ender "+silo[i][j]);
                loop3:
                for(k in combined){
                    if(silo[i][j] === combined[k]){
                        console.log("MATCH!")
                        break loop1;
                    }
                }
            }
	    flipper = true;
	    break loop1;
        }
	if(flipper){
	    console.log("WRITING ENDER");
	    fs.appendFileSync('newRaw.csv', data['Video ID']+","+data['Content Type']+","+data['Policy']+","+data['Video Title']+","+data['Video Duration (sec)']+","+data['Username']+","+data['Uploader']+","+data['Channel Display Name']+","+data['Channel ID']+","+data['Claim Type']+","+data['Claim Origin']+","+data['Total Views']+","+data['Watch Page Views']+","+data['Embedded Player Views']+","+data['Channel Page Video Views']+","+data['Live Views']+","+data['Recorded Views']+","+data['Ad-Enabled Views']+","+data['Total Earnings']+","+data['Gross YouTube-sold Revenue']+","+data['Gross Partner-sold Revenue']+","+data['Gross AdSense-sold Revenue']+","+data['Estimated RPM']+","+data['Net YouTube-sold Revenue']+","+data['Net AdSense-sold Revenue']+","+data['Multiple Claims?']+","+data['Category']+","+data['Asset ID']+","+data['Channel']+","+data['Custom ID']+","+data['ISRC']+","+data['GRid']+","+data['UPC']+","+data['Artist']+","+data['Asset Title']+","+data['Album']+","+data['Label']+","+""+"\n");
	}
    })
    reader.addListener('end', function(){
        callback();
    })
}

});
