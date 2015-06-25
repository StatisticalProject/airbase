r.tableDrop('countries');
r.tableDrop('stations');
r.tableDrop('measures');


r.tableCreate('countries');
r.tableCreate('stations');
r.tableCreate('measures');

var countries=r.table('users').map(function ( doc) {
  return {id: doc('airbase').getField('country').getField('country_iso_code'),country:doc('airbase').getField('country').without('station')};
});


var stations=r.table('users').concatMap(function ( doc) {
               return doc('airbase')('country')('station').without('measurement_configuration').map(function(station) {
                 return station.merge({id:station('@Id')});
                                                                                                    });
             });

var measures=r.table('users').concatMap(function ( doc) {
  return doc('airbase')('country')('station').filter(function (row ){return row.hasFields('measurement_configuration')}).map(function(station) {
    return {id:station('@Id'),mesure:station('measurement_configuration')};

});
});

r.table('countries').insert(countries,{conflict:'replace'});
r.table('stations').insert(stations,{conflict:'replace'});
r.table('measures').insert(measures,{conflict:'replace'});


r.table('countries').count();
r.table('stations').count();
r.table('measures').count();

r.table('users').map(function ( doc) {
  return {station: doc('airbase').getField('country').getField('station').without('measurement_configuration')};
})

r.table('users').concatMap(function ( doc) {
  return doc('airbase')('country')('station')('measurement_configuration');
})


