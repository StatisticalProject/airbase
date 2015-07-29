r = require('rethinkdb')
var baseDb='basedb';
function callback(err, result) {
    if (err) throw err;
    console.log(JSON.stringify(result, null, 2));
};
function callbackNoError(err, result) {
    console.log(JSON.stringify(result, null, 2));
};

console.log("Création de la base");
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
	r.dbDrop(baseDb).run(conn,callbackNoError);
 	var containsDb=r.dbCreate(baseDb).run(conn,callbackNoError);
	console.log(containsDb);
	
});

console.log("Import des données");
var exec = require('child_process').exec;
var cmd = 'rethinkdb import -f data/EE_meta.xml.json --table '+baseDb+'.origin --force';
exec(cmd, function(error, stdout, stderr) {
  console.log("Import Data "+error+" "+stderr+" "+stdout);
});
var exec = require('child_process').exec;
var cmd = 'rethinkdb import -f data/IS_meta.xml.json --table '+baseDb+'.origin --force';
exec(cmd, function(error, stdout, stderr) {
  console.log("Import Data "+error+" "+stderr+" "+stdout);
});

console.log("Création des Tables");
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
 	r.db(baseDb).tableDrop('countries').run(conn,callbackNoError);
	r.db(baseDb).tableDrop('stations').run(conn,callbackNoError);
	r.db(baseDb).tableDrop('measures').run(conn,callbackNoError);

	r.db(baseDb).tableCreate('countries').run(conn,callbackNoError);
	r.db(baseDb).tableCreate('stations').run(conn,callbackNoError);
	r.db(baseDb).tableCreate('measures').run(conn,callbackNoError);

	console.log(r.db(baseDb).tableList()).run(conn,callbackNoError);
});

console.log("Insertion des pays");
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
	//On récupère la description des pays
	var countryList= r.db(baseDb).table('origin')('airbase')('country') ;
	//On convertit le document
	var countries= countryList.map(function ( doc) {
	//Suppression de la partie station du document et ajout d’un attribut “id”
	  return doc.without('station').merge({id: doc('country_iso_code')});
	});
	//ajout dans la table countries
	var table= r.db(baseDb).table('countries');
	console.log(table.insert(countries,{conflict:'replace'}).run(conn,callbackNoError));
});


console.log("Insertion des stations");
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
	//On liste chaque station de chaque pays de la base airbase
	var stLt= r.db('airbase').table('origin')('airbase')('country')('station');
	//On concatene toutes les listes de station
	var stations= stLt.concatMap(
	function ( doc) {
	  //on supprime la partie concernant les mesures 
	  return doc.without('measurement_configuration');
	}).distinct().map(function (doc){ //On ajoute ensuite l'identifiant
	  return doc.merge({id:doc('@Id')});
	});
	//on sauvegarde dans la table stations
	var table= r.db('airbase').table('stations') ;
	console.log(table.insert(stations,{conflict:'replace'}).run(conn,callbackNoError));

});
  
