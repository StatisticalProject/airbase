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
    return {id:station('@Id'),measure:station('measurement_configuration')};

});
});

var statistics=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function (doc){
  return {stats:doc('measure')('statistics'),id:doc('id')};
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


r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function (doc){
return doc('measure')('statistics');
});

r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function (doc){
  return {measure:statistics:doc('measure').map(function (stat){return {stats:stat('statistics'),stats:('measure')('component_caption')})},id:doc('id')};
});



r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function (doc){
  return {statistics:doc('measure').filter(function (doc){return doc('measure').hasFields('statistics');}).map(
  function (stati){
    return stati('statistics').merge(comp:stati('component_caption')
  }),id:doc('id')};
});

r.row('foo').filter({id: 'id1'})