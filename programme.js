r.tableDrop('countries');
r.tableDrop('stations');
r.tableDrop('measures');
r.tableDrop('stats');


r.tableCreate('countries');
r.tableCreate('stations');
r.tableCreate('measures');
r.tableCreate('stats');

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
    return {id:station('@Id'),latitude:station('station_info')('station_latitude_decimal_degrees'),longitude:station('station_info')('station_longitude_decimal_degrees'),measure:station('measurement_configuration')};
});
});



r.table('countries').insert(countries,{conflict:'replace'});
r.table('stations').insert(stations,{conflict:'replace'});
r.table('measures').insert(measures,{conflict:'replace'});

var stats=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {
                          return
                            doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id'),latitude:doc('latitude').coerceTo('number'),longitude:doc('longitude').coerceTo('number')})})})
                          ;
                        }).concatMap(function (comp){
            return comp.pluck('statistics','idStation','component_caption','latitude','longitude').coerceTo('array');
          });

r.table('stats').insert(stats,{conflict:'replace'});



r.table('countries').count();
r.table('stations').count();
r.table('measures').count();




//pour sauvegarde
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


r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {
  return {id: doc('id'),latitude:doc('station_latitude_decimal_degrees'),longitude:doc('station_longitude_decimal_degrees'),comp:doc('measure').getField('component_caption')}};
})

var stats=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {

      return
        doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id'),latitude:doc('latitude'),longitude:doc('longitude')})})})
      ;


    }).map(function (comp){
      return comp.pluck('statistics','idStation','component_caption','latitude','longitude') ;

    })

r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {
  return {id: doc('id'),comp:doc('measure')};
}).map(function ( doc) {

  return {
    	id: doc('id'),
    stat:doc('comp').getField('''').append(doc('comp').getField('statistics'))
  };

  r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {

    return
      doc('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id')})})})
    ;


  })

    r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {

      return
        doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id')})})})
      ;


    }).map(function (comp){
      de=comp('idStation');
      return comp.getField('statistics').do(function (stat) { return stat.map(function (ab){return ab.merge({id:de,component:comp('component_caption')});})
        });

    })


})

    var stats=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {

      return
        doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id'),latitude:doc('latitude'),longitude:doc('longitude')})})})
      ;


    }).map(function (comp){
      return comp.pluck('statistics','idStation','component_caption','latitude','longitude') ;

    })


var statistics=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function (doc){
  return {stats:doc('measure')('statistics'),id:doc('id')};
});

r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {
                return
                  doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id'),latitude:doc('latitude').coerceTo('number'),longitude:doc('longitude').coerceTo('number')})})})
                ;
              }).map(function (comp){
                return comp.pluck('statistics','idStation','component_caption','latitude','longitude') ;
}).concatMap(function (doc){return doc.coerceTo('array');})
  .filter(function (doc){return doc.hasFields('statistics');})
  .
  map(function (doc){
  return {statistic:doc('statistics'),idStation:('idStation'),component_caption:doc('component_caption'),latitude:doc('latitude'),longitude:doc('longitude')};
})