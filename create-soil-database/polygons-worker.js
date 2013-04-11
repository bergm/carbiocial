importScripts("https://maps.googleapis.com/maps/api/js?v=3.exp&sensor=false");

importScripts("regions-and-profiles.js");

onmessage = function(event){
    
  postMessage({"type": "debug", "out": "test"})
  
  for(var spi in soilProfiles){
    var lat10000 = soilProfiles[spi].lat10000;
    var lng10000 = soilProfiles[spi].lng10000;
    var spCoord = soilProfiles[spi].coord;
    
    postMessage({"type": "debug", "lat": lat10000, "lng": lng10000});
    
    for(var ri in regions){
      var rId = regions[ri].id;
      var rPoly = regions[ri].poly;
            
      var p = new google.maps.Polygon();
      p.setPath(rPoly);
      
      if(google.maps.geometry.poly.containsLocation(spCoord, p))
        postMessage({"type": "result", "rid": rId, "lat": lat10000, "lng": lng10000});
        
    }
    
   //postMessage({"type": "debug", "lat": lat10000, "lng": lng10000}); 
  }
  
};
