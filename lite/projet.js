r = require('rethinkdb')
var baseDb='basedb';
function callback(err, result) {
    if (err) throw err;
    console.log(JSON.stringify(result, null, 2));
};
function callbackNoError(err, result) {
    console.log(JSON.stringify(result, null, 2));
};

r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
 	var containsDb=r.dbCreate(baseDb).run(conn,callbackNoError);
	console.log("e "+containsDb);
	
});

  
