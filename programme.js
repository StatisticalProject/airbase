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
r.table('countries').insert(countries,{conflict:'replace'});


var stations=r.table('users').concatMap(function ( doc) {
               return doc('airbase')('country')('station').without('measurement_configuration').map(function(station) {
                 return station.merge({id:station('@Id')});
                                                                                                    });
             });

r.table('stations').insert(stations,{conflict:'replace'});


var measures=r.table('users').concatMap(function ( doc) {
  return doc('airbase')('country')('station').filter(function (row ){return row.hasFields('measurement_configuration')}).map(function(station) {
    return {id:station('@Id'),latitude:station('station_info')('station_latitude_decimal_degrees'),longitude:station('station_info')('station_longitude_decimal_degrees'),measure:station('measurement_configuration')};
});
});

r.table('measures').insert(measures,{conflict:'replace'});

var stats=r.table('measures').coerceTo('array').concatMap(function(measure){ return measure('measure').coerceTo('array').filter(function (year){return year.typeOf().eq('OBJECT');}).map(function(compl){ return compl.merge(measure.without('measure'))});}).coerceTo('array').filter(function (doc){return doc.hasFields('statistics')}).concatMap(function (configMeasure){ 
  return configMeasure('statistics').coerceTo('array').filter(function (year){return year.typeOf().eq('OBJECT');}).map(function (stat){return {id:configMeasure('id').add('-').add(stat('@Year')),st:stat,con:configMeasure.without('statistics')};});
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
var stats=r.table('measures').filter(function (doc){return doc('measure').hasFields('statistics');}).map(function ( doc) {
                          return
                            doc.getField('measure').do(function (oo){return oo.do(function (dd){return dd.merge({idStation:doc('id'),latitude:doc('latitude').coerceTo('number'),longitude:doc('longitude').coerceTo('number')})})})
                          ;
                        }).concatMap(function (comp){
  return comp.pluck('statistics','idStation','component_caption','latitude','longitude').coerceTo('array');
          });
stats.coerceTo('array').map(function(forId){return forId.merge({id:forId('idStation').add(' - ').add(forId('component_caption'))});}).limit(10);


Map reduce compte les mesures par type
r.table('stats').coerceTo('array')('con').group('component_caption').map( function (t){return 1;}).reduce(function (a,b){return a.add(b);})
r.table('stats').coerceTo('array')('con').group('component_code').count()
compte les mesures par stations
r.table('stats').coerceTo('array')('con').eqJoin("id", r.table("stations"))('right').group('station_european_code').count();
par pays
r.table('stats').coerceTo('array')('con').eqJoin("id", r.table("stations"))('right').map(function (po){return po.merge({station_european_code:po('station_european_code').coerceTo('binary').slice(0,2).coerceTo('string')})}).group('station_european_code').count()
compte par anneee pour NO2
r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');})('st').group('@Year').count()
compte sur tous les NO par caption
r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');})('con').group('component_caption').count()
tous de 2005 sur NO avec toutes les coordonnees
r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');}).map(function (yui){return yui('st').merge(yui('con'));}).filter(function(fre){return fre.hasFields('@Year')&&fre('@Year').eq('2005')}).concatMap(function (values){return values('statistics_average_group').merge(values.without('statistics_average_group'));}).filter(function (prim){ return prim('@value').eq('day')}).map(function (set){return set('statistic_set').merge(set.without('statistic_set'));}).concatMap(function (res){ return res('statistic_result').merge(res.without('statistic_result'));}).filter( function (meaner){return meaner('statistic_shortname').eq('Mean')})

distance max pour 2005 NO en sample
var tryo=r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');}).map(function (yui){return yui('st').merge(yui('con'));}).filter(function(fre){return fre.hasFields('@Year')&&fre('@Year').eq('2005')}).concatMap(function (values){return values('statistics_average_group').merge(values.without('statistics_average_group'));}).filter(function (prim){ return prim('@value').eq('day')}).map(function (set){return set('statistic_set').merge(set.without('statistic_set'));}).concatMap(function (res){ return res('statistic_result').merge(res.without('statistic_result'));}).filter( function (meaner){return meaner('statistic_shortname').eq('Mean')});
var extra=tryo.map(function (max){ return {point:r.point(max('longitude').coerceTo('number'),max('latitude').coerceTo('number')),mean:max('statistic_value').coerceTo('number')};}).sample(10);
extra.concatMap(function (left){return extra.map(function (right){return r.distance(left('point'),right('point'),{unit: 'km'})})}).max()

vrac
var tryo=r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');}).map(function (yui){return yui('st').merge(yui('con'));}).filter(function(fre){return fre.hasFields('@Year')&&fre('@Year').eq('2005')}).concatMap(function (values){return values('statistics_average_group').merge(values.without('statistics_average_group'));}).filter(function (prim){ return prim('@value').eq('day')}).map(function (set){return set('statistic_set').merge(set.without('statistic_set'));}).concatMap(function (res){ return res('statistic_result').merge(res.without('statistic_result'));}).filter( function (meaner){return meaner('statistic_shortname').eq('Mean')});
tryo.map(function (max){ return {point:r.point(max('longitude').coerceTo('number'),max('latitude').coerceTo('number')),mean:max('statistic_value').coerceTo('number')};})    


var tryo=r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');}).map(function (yui){return yui('st').merge(yui('con'));}).filter(function(fre){return fre.hasFields('@Year')&&fre('@Year').eq('2005')}).concatMap(function (values){return values('statistics_average_group').merge(values.without('statistics_average_group'));}).filter(function (prim){ return prim('@value').eq('day')}).map(function (set){return set('statistic_set').merge(set.without('statistic_set'));}).concatMap(function (res){ return res('statistic_result').merge(res.without('statistic_result'));}).filter( function (meaner){return meaner('statistic_shortname').eq('Mean')});
var extra=tryo.map(function (max){ return {point:r.point(max('longitude').coerceTo('number'),max('latitude').coerceTo('number')),mean:max('statistic_value').coerceTo('number')};});
extra.map(function (left){return extra.map(function (right){return r.distance(left('point'),right('point'))});})

var tryo=r.table('stats').coerceTo('array').filter(function (pomo){ return pomo('con')('component_code').eq('38');}).map(function (yui){return yui('st').merge(yui('con'));}).filter(function(fre){return fre.hasFields('@Year')&&fre('@Year').eq('2005')}).concatMap(function (values){return values('statistics_average_group').merge(values.without('statistics_average_group'));}).filter(function (prim){ return prim('@value').eq('day')}).map(function (set){return set('statistic_set').merge(set.without('statistic_set'));}).concatMap(function (res){ return res('statistic_result').merge(res.without('statistic_result'));}).filter( function (meaner){return meaner('statistic_shortname').eq('Mean')});
var extra=tryo.map(function (max){ return {point:r.point(max('longitude').coerceTo('number'),max('latitude').coerceTo('number')),mean:max('statistic_value').coerceTo('number')};}).sample(10);
extra.concatMap(function (left){return extra.map(function (right){return r.distance(left('point'),right('point'))})});
