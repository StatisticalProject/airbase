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

	console.log(r.db(baseDb).tableList().run(conn,callbackNoError));
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
	var stLt= r.db(baseDb).table('origin')('airbase')('country')('station');
	//On concatene toutes les listes de station
	var stations= stLt.concatMap(
	function ( doc) {
	  //on supprime la partie concernant les mesures 
	  return doc.without('measurement_configuration');
	}).distinct().map(function (doc){ //On ajoute ensuite l'identifiant
	  return doc.merge({id:doc('@Id')});
	});
	//on sauvegarde dans la table stations
	var table= r.db(baseDb).table('stations') ;
	console.log(table.insert(stations,{conflict:'replace'}).run(conn,callbackNoError));

});
  
console.log("Insertion des mesures");
r.connect( {host: 'localhost', port: 28015}, function(err, conn) {
    if (err) throw err;
	var stations= db(baseDb).table('origin')('airbase')('country')('station') ;
	// Première partie : les tableaux de mesure
	var arrayMeasure=stations.map( function (station){
	  return station.
		//filtre sur les tableaux et contenant une configuration de mesure
	  filter(
		function (doc){ 
			 return doc.hasFields('measurement_configuration').
		  and(doc('measurement_configuration').typeOf().eq('ARRAY'))}
		
		).//on concatène les mesures des stations
		concatMap(function (element){
			return element('measurement_configuration').map(
			function (configM){
			  //On récupère la mesure et on lui ajoute les informations de station
			  //On ajoute un identifiant m »langeant id de stations 
			  //et le nom de la composante
			  return configM.merge(element.without('measurement_configuration')).
				merge({id:element('@Id').add('-').add(configM('component_caption'))});
			});
		  });
	  });
	//Seconde partie: les mesures uniques
	var objectMeasure=stations.map( function (station){
	  //filtre les données avec mesure et qui ne sont pas des tableaux
	  return station.filter(
		function (doc){ 
		  return doc.hasFields('measurement_configuration').and(doc('measurement_configuration').typeOf().eq('OBJECT'))
		})// On transforme les documents
		.map(
		function (element){
		  //On récupère la mesure et on lui ajoute les informations de station
		  //On ajoute un identifiant mélangeant id de stations 
		  //et le nom de la composante          
			return element('measurement_configuration').
			merge(element.without('measurement_configuration')).
			merge({id:element('@Id').add('-').add(element('measurement_configuration')('component_caption'))});
		});
	});
	  //On réalise l'union des deux séquences
	var measures=arrayMeasure.union(objectMeasure).concatMap(function (doc){return doc;});
	//On sauvegarde dans la table des mesures
	var table=r.db(baseDb).table('measures');
	console.log(table.insert(measures,{conflict:'replace'}).run(conn,callbackNoError));

});  
