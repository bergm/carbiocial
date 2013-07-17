/**
Authors: 
Michael Berg <michael.berg@zalf.de>

Maintainers: 
Currently maintained by the authors.

This file is part of the MONICA model. 
Copyright (C) 2007-2013, Leibniz Centre for Agricultural Landscape Research (ZALF)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "boost/foreach.hpp"

#include "tools/use-stl-algo-boost-lambda.h"
#include "db/db.h"
#include "tools/coord-trans.h"
#include "grid/grid+.h"

using namespace std;
using namespace Db;
using namespace Tools;
using namespace Grids;

int main(int argc, char** argv)
{
	GridPPtr solos(new GridP("solos", GridP::ASCII, "../solos_brazil_900.asc", UTM21S_EPSG32721));
				
	vector<LatLngCoord> tlTrBrBlLatLngBounds = RC2latLng(solos->rcRect().toTlTrBrBlVector());

	DBPtr con(new SqliteDB("../carbiocial.sqlite"));
	
	//get all the profiles in a region into an easy to access map
	ostringstream s;
	s << 
		"select distinct lat_times_10000, lng_times_10000 "
		"from soil_profile_data "
		"order by lat_times_10000, lng_times_10000";
	con->select(s.str().c_str());

	vector<LatLngCoord> regionLatLngCoords;
	DBRow row;
	while (!(row = con->getRow()).empty())
	{
		double lat = double(satoi(row[0])/10000.);
		double lng = double(satoi(row[1])/10000.);
		LatLngCoord llc(lat, lng);
		regionLatLngCoords.push_back(llc);
	}

	map<int, vector<int> > regionId2profileId;
	map<int, RectCoord> profileId2rectCoord;
	map<int, LatLngCoord> profileId2LatLngCoord;
	int profileIdCount = 0;

	vector<RectCoord> rcs = latLng2RC(regionLatLngCoords, UTM21S_EPSG32721);
	for(int i = 0, size = rcs.size(); i < size; i++)
	{
		LatLngCoord llc = regionLatLngCoords.at(i);
		RectCoord rc = rcs.at(i);

		int regionId = int(solos->dataAt(rc));
		if(regionId == solos->noDataValue())
			continue;
		profileId2rectCoord[profileIdCount]  = rc;
		profileId2LatLngCoord[profileIdCount] = llc;
		regionId2profileId[regionId].push_back(profileIdCount);
		profileIdCount++;
	}

	//*
	con->freeResultSet();
	typedef pair<int, LatLngCoord> P;
	BOOST_FOREACH(P p, profileId2LatLngCoord)
	{
		ostringstream s2;
		s2 <<
			"update soil_profile_data"
			" set id = " << p.first << 
			" where lat_times_10000 = " << int(p.second.lat*10000) << 
			" and lng_times_10000 = " << int(p.second.lng*10000); 
		con->update(s2.str().c_str());
		cout << "done profile " << p.first << " from " << profileId2LatLngCoord.size() << endl;
	}
	//*/

	//create a voronoi grid of sub-regions which belong to just one profile
	GridPPtr voronoiProfiles(solos->clone());
	for(int r = 0, rs = voronoiProfiles->rows(); r < rs; r++)
	{
		for(int c = 0, cs = voronoiProfiles->cols(); c < cs; c++)
		{
			if(voronoiProfiles->isDataField(r, c))
			{
				RectCoord cellRc = voronoiProfiles->rcCoordAtCenter(r, c);

				int regionId = int(voronoiProfiles->dataAt(r, c));
				vector<int> profileIds = regionId2profileId[regionId];
				double closestProfileId = -1;
				double closestDistanceSoFar = -1;

				BOOST_FOREACH(int profileId, profileIds)
				{
					RectCoord currentRc = profileId2rectCoord[profileId];
					double currentDistance = cellRc.distanceTo(currentRc);
					if(closestDistanceSoFar == -1 || currentDistance < closestDistanceSoFar)
					{
						closestProfileId = profileId;
						closestDistanceSoFar = currentDistance;
					}
					//cout << "r: " << r << " c: " << c <<
					//	" cur: |id: " << profileId << " -> " << currentDistance <<
					//	"| close: |id: " << closestProfileId << " -> " << closestDistanceSoFar <<
					//	"| curRc: |r: " << currentRc.r << " h: " << currentRc.h << "|" <<
					//	"| cellRc: |r: " << cellRc.r << " h: " << cellRc.h << "|" << endl;
				}

				voronoiProfiles->setDataAt(r, c, closestDistanceSoFar == -1
					? voronoiProfiles->noDataValue()
					: closestProfileId);
			}
		}
		cout << "done row: " << r << endl;
	}
			
	voronoiProfiles->writeAscii<int>("../solos-profiles_brazil_900.asc");

	exit(0);
}
