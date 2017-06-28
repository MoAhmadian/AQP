// Part 1 & 2

function SamplingWithReplacement(snumber,dnumber,cname) {
//	DBQuery.shellBatchSize = snumber*100;
	var samplename="s";

	for ( j = 1; j < snumber+1; ++j ) {
            var before = new Date()
	    samplename="s"+j;
	    print(samplename + " started");
            db[samplename].drop();
	    db[cname].aggregate([{$sample:{size: dnumber}}]).forEach(function(doc){ db[samplename].insert(doc);});
	    print("doc count in "+ samplename+ ":" +db[samplename].count());

var after = new Date()
execution_mills = after - before
print("Millis: "+execution_mills);
//sleep(1000);
}
}
//===========================================================================================================================================
function SamplingWithoutReplacement(snumber,dnumber,cname) {

	DBQuery.shellBatchSize = snumber*100;
	var samplename="s";

	for ( j = 1; j < snumber+1; ++j ) {
            var remain=0;
            var before = new Date()
	    samplename="s"+j;
	    print(samplename + " started");
            //db[samplename].drop();
	    var c=db[samplename].count();
            if(c==dnumber)continue;
            else{
            remain=dnumber-c;
            db[cname].aggregate([{$sample:{size: remain}}]).forEach(function(doc){ db[samplename].insert(doc);});
	    print("doc count in " +samplename+" is: "+db[samplename].count());
                       }
	    db[samplename].find({}).forEach( function(doc) {
	    db[cname].remove(doc);
});
var after = new Date()
execution_mills = after - before
print("Mills: "+execution_mills);
}
}

//===========================================================================================================================================
function percentileQuery(iteration,snumber){
var samplename;
print("{");
for ( j = 1; j < snumber+1; ++j ) {

	samplename="s"+j;
	print(',"' + samplename + '":');
        var nums;
       nums = db[samplename].count();
	for (i=0; i < iteration; i++) {
//	var before = new Date();
printjson(db[samplename].aggregate([
        { "$group": { "_id": {"clearance":  "$clearance"}, "count": { "$sum": 1 }}},    
        { "$project": { 
            "count": 1, 
            "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": [ { "$divide": [ "$count", {"$literal": nums }] }, 100 ] }, 0,6 ] }, "", "%" ]}
            }
        }
    ]
).toArray());

//	var after = new Date();
//	execution_mills = after - before;
//	print("Mills for "+ iteration + ":" +execution_mills);

} //internal for-loop

} //external for-loop
print("}");
	}

//===========================================================================================================================================
function QueryProcessingTime(iteration,snumber){
var samplename;
print("{");
for ( j = 1; j < snumber+1; ++j ) {

	samplename="s"+j;
	print('},"' + samplename + '":{');
        var nums;
       nums = db[samplename].count();
	var before = new Date();
	for (i=0; i < iteration; i++) {
db[samplename].aggregate([
        { "$group": { "_id": {"clearance":  "$clearance"}, "count": { "$sum": 1 }}},    
        { "$project": { 
            "count": 1, 
            "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": [ { "$divide": [ "$count", {"$literal": nums }] }, 100 ] }, 0,6 ] }, "", "%" ]}
            }
        }
    ]
);

} //internal for-loop
	var after = new Date();
	execution_mills = after - before;
print('"time":'+execution_mills);
} //external for-loop
print("}");
	}

//===========================================================================================================================================
function RepopulateDB(){
db.back.aggregate([ { $match: {} }, { $out: "people" } ])
}

//===========================================================================================================================================

function DellAllSamples(){
for ( j = 1; j < 101; ++j ) { samplename="s"+j;  db[samplename].drop();}
}
//=============================================================================================================================================
//full data time
function TetaOverD(){
        nums=db.people.count();
	db.setProfilingLevel(0);
	db.system.profile.drop();
	db.setProfilingLevel(2);
db.people.aggregate([
        { "$group": { "_id": {"clearance":  "$clearance"}, "count": { "$sum": 1 }}},    
        { "$project": { 
            "count": 1, 
            "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": [ { "$divide": [ "$count", {"$literal": nums }] }, 100 ] }, 0,4 ] }, "", "%" ]}
            }
        }
    ]
);
db.system.profile.find({},{millis:1,totalDocsExamined:1,nReturned:1});
}

//===================================================================================================================================================
function TetaOverD(){
        nums=db.people.count();
	db.setProfilingLevel(0);
	db.system.profile.drop();
	db.setProfilingLevel(2);
db.people.aggregate([
        { "$group": { "_id": {"clearance":  "$clearance"}, "count": { "$sum": 1 }}},    
        { "$project": { 
            "count": 1, 
            "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": [ { "$divide": [ "$count", {"$literal": nums }] }, 100 ] }, 0,4 ] }, "", "%" ]}
            }
        }
    ]
);
db.system.profile.find({},{millis:1,totalDocsExamined:1,nReturned:1});
}

//======================================================================================================================================================
use hr2;
function runQueryOverSamples(iteration,snumber){
var samplename;
DBQuery.shellBatchSize = iteration;
for ( j = 1; j < snumber+1; ++j ) {

	samplename="s"+j;
        print("s"+j);
	db.setProfilingLevel(0);
	db.system.profile.drop();
	db.setProfilingLevel(2);
	var before = new Date() 
	for (i=0; i < iteration; i++) {
		db.setProfilingLevel(0);
		db[samplename].find({salary:{ $gt:40000 }});
		db.setProfilingLevel(2);
		var nums = db[samplename].count();
printjson(db[samplename].aggregate([
        { "$group": { "_id": {"clearance":  "$clearance"}, "count": { "$sum": 1 }}},    
        { "$project": { 
            "count": 1, 
            "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": [ { "$divide": [ "$count", {"$literal": nums }] }, 100 ] }, 0,4 ] }, "", "%" ]}
            }
        }
    ]
).toArray());
} //internal for-loop

	var after = new Date()
	execution_mills = after - before
	print("Mills for all queries iteration: "+execution_mills);
} //external for-loop
	db.system.profile.find({},{millis:1,totalDocsExamined:1,nReturned:1});
	}



runQueryOverSamples(10,100);
//===================================================



SamplingWithReplacement(100,100000,'people');
SamplingWithoutReplacement(100,10000,'people');








// Part 3 & 4

//=========================================================================================================================
use lp;
function CrossCorrelationAnalysis(source,destination,numberOfSamples,localfield, foreignfield) {
         var correlation     = [];
         var sourceInit      = source.charAt(0);
	 var destinationInit = destination.charAt(0);
	for ( j = 1; j < numberOfSamples+1; ++j ) {

	    samplename       = sourceInit+j;
            resCollection   = samplename+destinationInit;
            db[resCollection].drop();
            print("Number of documents in "+samplename+ " is: " + db[samplename].count());
            var before       = new Date()	    
db[samplename].aggregate([{$lookup:{from:destination,localField:localfield,foreignField:foreignfield,as:"corr" }},{$match:{"corr":{$ne:[]}}},{ $out : resCollection }]);

            var after = new Date()
            correlation[j]=db[resCollection].count();
	    execution_mills = after - before
            print("Mills: "+execution_mills);
						}
	for ( j = 1; j < numberOfSamples+1; ++j ) {	   
 print("correlation["+j+"] is "+ correlation[j]);
                       }
print("============================");
}
//============================================================================================================================
CrossCorrelationAnalysis('health','phone',10,'phone','phone');
CrossCorrelationAnalysis('health','finance',10,'ssn','ssn');
CrossCorrelationAnalysis('social','phone',10,'email','email');
