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
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_set>

#include "boost/foreach.hpp"

#include "tools/use-stl-algo-boost-lambda.h"
#include "db/db.h"
#include "tools/coord-trans.h"
#include "grid/grid+.h"
#include "carbiocial.h"

using namespace std;
using namespace Db;
using namespace Tools;
using namespace Grids;
using namespace Carbiocial;

pair<int, int> roundTo5(double value)
{
	auto v = div(int(value), 10);
	switch(v.rem)
	{
	case 0: case 1: case 2: return make_pair(v.quot*10, v.rem);
	case 3: case 4: case 5: case 6: case 7: return make_pair(v.quot*10 + 5, 5 - v.rem);
	case 8: case 9: return make_pair(v.quot*10 + 10, 10 - v.rem); 
	default: ;
	}
}

enum RegionId { cuiaba = 1, sinop = 2, novoProgresso = 3, campoVerde = 4 };

void sectorizeSubSolosGrid(string pathToSoilClassIdsGrid, string outputPathToSectorsFile, int roundToDigits,
													 RegionId regionId)
{
	GridPPtr solos(new GridP("solos-soil-class-ids", GridP::ASCII, pathToSoilClassIdsGrid, UTM21S_EPSG32721));
	
	typedef int SoilId;

	unordered_set<string> s;
	map<string, vector<int>> s2sectorIds;
	map<string, map<SoilId, int>> s2soilClassDistribution;
	map<int, pair<int, int>> sectorId2topLeft;

	int sectorId = 0;
	int maxRows = solos->rows();
	int maxCols = solos->cols();

	for(int rowSize = maxRows; rowSize > 0; rowSize--)
	{
		int noOfSectorsPerRow = maxRows/rowSize - 1;
		int rowRest = maxRows%rowSize;
		if(rowRest > 0) 
			noOfSectorsPerRow++;

		for(int colSize = maxCols; colSize > 0; colSize--)
		{
			int noOfSectorsPerCol = maxCols/colSize - 1;
			int colRest = maxCols%colSize;
			if(colRest > 0) 
				noOfSectorsPerCol++;
			
			for(int top = 0; top <= noOfSectorsPerRow; top++)
			{
				for(int left = 0; left <= noOfSectorsPerCol; left++)
				{
					int cols = left == noOfSectorsPerCol && colRest > 0 ? colRest : colSize;
					int rows = top == noOfSectorsPerRow && rowRest > 0 ? rowRest : rowSize;
					GridPPtr sg = GridPPtr(solos->subGridClone(top, left, rows, cols));

					auto rsf = roundedSoilFrequency(sg.get(), roundToDigits);

					ostringstream os;
					for_each(rsf.begin(), rsf.end(), [&](pair<SoilId, int> p)
					{
						os << int(p.first) << " = " << p.second << "%" << endl;
					});
					string distributionAsString = os.str();

					if(!distributionAsString.empty())
					{
						s.insert(distributionAsString);
						s2sectorIds[distributionAsString].push_back(sectorId);
						if(s2soilClassDistribution.find(distributionAsString) == s2soilClassDistribution.end())
							s2soilClassDistribution[distributionAsString] = rsf;
					}

					//cout << "[" << top << "|" << left << "] ";
					//cout << "sector id: " << sectorId << " -> " << os.str() << endl << endl;
					sectorId2topLeft[sectorId] = make_pair(top, left);

					sectorId++;
				}
				//cout << endl;
			}
			//cout << endl;
			//cout << "rowSize: " << rowSize << " colSize: " << colSize << endl;
		}
		cout << "rowSize: " << rowSize << endl;
	}
		
	//*
	//write to database
	DBPtr con(new MysqlDB("144.41.15.33", "mberg", "ZaLf2013", "carbiocial"));
	
	string insert = 
		"insert into tbl_unique_sectors_in_municipality (municip_id, sector_id, soil_type_id, percentage) "
		"values ";

	int uniqueSectorId = 0;
	for_each(s2soilClassDistribution.begin(), s2soilClassDistribution.end(), 
		[&](const pair<string, map<SoilId, int>>& p)
	{
		ostringstream inserts;
		inserts << insert;

		bool firstElement = true;

		for_each(p.second.begin(), p.second.end(), [&](pair<SoilId, int> p2)
		{
			inserts 
				<< (firstElement ? "" : ", ") << "(" 
				<< int(regionId) << ", " << uniqueSectorId << ", " 
				<< p2.first << ", " << p2.second << ")";

			firstElement = false;
		});
	
		cout << inserts.str() << endl; 

		con->insert(inserts.str());

		uniqueSectorId++;
	});
	
	//*/
	
	ofstream ofs(outputPathToSectorsFile);

	/*
	ofs << "sectorId ---> [top, left]" << endl << "--------------------------------------" << endl;
	for_each(sectorId2topLeft.begin(), sectorId2topLeft.end(), [&ofs](pair<int, pair<int, int>> p)
	{
		ofs << p.first << " ---> [" << p.second.first << ", " << p.second.second << "]" << endl;
	});
	ofs << endl << endl;
	//*/

	ofs << "number of distinct sectors: " << s.size() << endl << endl;

	ofs << "sector soil class area ---> sectorId1 sectorId2 ....." << endl << "------------------------------------" << endl;
	for_each(s2sectorIds.begin(), s2sectorIds.end(), [&](pair<string, vector<int>> p)
	{
		//if(p.first.substr(0, 11) == "-9999 = 100")
		//	ofs << "-9999 = 100 -> " << p.second.size() << " sectors" << endl << endl;
		//else
		//{
			ofs << p.first << "sectors ---> ";
			for_each(p.second.begin(), p.second.end(), [&](int v){ ofs << v << " "; });
			ofs << endl << endl;
		//}
	});
}

void sectorizeFullSolosGrid(string pathToSoilClassIdsGrid, string outputPathToSectorsFile)
{
	GridPPtr solos(new GridP("solos-soil-class-ids", GridP::ASCII, pathToSoilClassIdsGrid, UTM21S_EPSG32721));
	
	unordered_set<string> s;
	map<string, vector<int>> s2sectorIds;
	map<int, pair<int, int>> sectorId2topLeft;

	int sectorId = 0;
	for(int top = 0, tops = 254; top <= tops; top++)
	{
		for(int left = 0, lefts = 192; left <= lefts; left++)
		{
			int cols = left == 192 ? 8 : 10;
			int rows = top == 254 ? 5 : 10;
			GridPPtr sg = GridPPtr(solos->subGridClone(top, left, rows, cols));
			
			auto f = sg->frequencyRev<int>(true, 0, 0);
			ostringstream os;
			for_each(f.begin(), f.end(), [&os](pair<double, double> p)
			{ 
				int roundedPercentage = Tools::roundRT<int>(p.first, -1);
				//if(int((double(roundedPercentage)/10.0 - int(double(roundedPercentage)/10.0)) * 10) != 5)
				//	roundedPercentage = int(Tools::round(p.first, 1, false));
				//else
				//	cout << "blal";
				
				if(roundedPercentage > 0)
					os << int(p.second) << " = " << roundedPercentage << "%" << endl;
			});

			if(!os.str().empty())
			{
				s.insert(os.str());
				s2sectorIds[os.str()].push_back(sectorId);
			}

			//cout << "[" << top << "|" << left << "] ";
			//cout << "sector id: " << sectorId << " -> " << os.str() << endl << endl;
			sectorId2topLeft[sectorId] = make_pair(top, left);

			sectorId++;
		}
		cout << endl;
	}
	cout << endl;
	
	ofstream ofs(outputPathToSectorsFile);

	ofs << "sectorId ---> [top, left]" << endl << "--------------------------------------" << endl;
	for_each(sectorId2topLeft.begin(), sectorId2topLeft.end(), [&ofs](pair<int, pair<int, int>> p)
	{
		ofs << p.first << " ---> [" << p.second.first << ", " << p.second.second << "]" << endl;
	});
	ofs << endl << endl;

	ofs << "number of distinct sectors: " << s.size() << endl << endl;

	ofs << "sector soil class area ---> sectorId1 sectorId2 ....." << endl << "------------------------------------" << endl;
	for_each(s2sectorIds.begin(), s2sectorIds.end(), [&](pair<string, vector<int>> p)
	{
		//if(p.first.substr(0, 11) == "-9999 = 100")
		//	ofs << "-9999 = 100 -> " << p.second.size() << " sectors" << endl << endl;
		//else
		//{
			ofs << p.first << "sectors ---> ";
			for_each(p.second.begin(), p.second.end(), [&](int v){ ofs << v << " "; });
			ofs << endl << endl;
		//}
	});
}

void createSoilClassGrid(string pathToSoilProfileGrid, string outputPathToSoilClassIdsGrid)
{
	GridPPtr profileIdsGrid(new GridP("grid", GridP::ASCII, pathToSoilProfileGrid, UTM21S_EPSG32721));
	
	DBPtr con(new SqliteDB("../carbiocial.sqlite"));
	
	//get all the profiles in a region into an easy to access map
	ostringstream s;
	s << 
		"select distinct id, soil_class_id "
		"from soil_profile_data";
	con->select(s.str().c_str());

	map<int, int> profileId2soilClassId;
	DBRow row;
	while (!(row = con->getRow()).empty())
		profileId2soilClassId[satoi(row[0])] = satoi(row[1]);
	
	GridPPtr soilClassIdsGrid(profileIdsGrid->clone());
	for(int r = 0, rs = profileIdsGrid->rows(); r < rs; r++)
	{
		for(int c = 0, cs = profileIdsGrid->cols(); c < cs; c++)
		{
			if(profileIdsGrid->isDataField(r, c))
				soilClassIdsGrid->setDataAt(r, c, profileId2soilClassId[int(profileIdsGrid->dataAt(r, c))]);
		}
	}
			
	soilClassIdsGrid->writeAscii<int>(outputPathToSoilClassIdsGrid);
}

void createVoronoiSoilProfileGrid(string pathToSoilRegionGrid, 
																	string outputPathToSoilProfileIdsGrid,
																	string outputPathToSoilClassIdsGrid)
{
	GridPPtr solos(new GridP("solos", GridP::ASCII, pathToSoilRegionGrid, UTM21S_EPSG32721));

	vector<LatLngCoord> tlTrBrBlLatLngBounds = RC2latLng(solos->rcRect().toTlTrBrBlVector());

	DBPtr con(new SqliteDB("../carbiocial.sqlite"));

	//get all the profiles in a region into an easy to access map
	ostringstream s;
	s << 
		"select distinct lat_times_10000, lng_times_10000, id, soil_class_id "
		"from soil_profile_data "
		"order by lat_times_10000, lng_times_10000";
	con->select(s.str().c_str());

	vector<LatLngCoord> regionLatLngCoords;
	map<int, int> profileId2soilClassId;
	DBRow row;
	while (!(row = con->getRow()).empty())
	{
		double lat = double(satoi(row[0])/10000.);
		double lng = double(satoi(row[1])/10000.);
		LatLngCoord llc(lat, lng);
		regionLatLngCoords.push_back(llc);
		profileId2soilClassId[satoi(row[2])] = satoi(row[3]);
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
	GridPPtr voronoiProfileIds(solos->clone());
	GridPPtr voronoiSoilClassIds(solos->clone());
	for(int r = 0, rs = voronoiProfileIds->rows(); r < rs; r++)
	{
		for(int c = 0, cs = voronoiProfileIds->cols(); c < cs; c++)
		{
			if(voronoiProfileIds->isDataField(r, c))
			{
				RectCoord cellRc = voronoiProfileIds->rcCoordAtCenter(r, c);

				int regionId = int(voronoiProfileIds->dataAt(r, c));
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

				voronoiProfileIds->setDataAt(r, c, closestDistanceSoFar == -1
					? voronoiProfileIds->noDataValue()
					: closestProfileId);
				voronoiSoilClassIds->setDataAt(r, c, closestDistanceSoFar == -1
					? voronoiProfileIds->noDataValue() 
					: profileId2soilClassId[closestProfileId]);
			}
		}
		cout << "done row: " << r << endl;
	}

	voronoiProfileIds->writeAscii<int>(outputPathToSoilProfileIdsGrid);
	voronoiSoilClassIds->writeAscii<int>(outputPathToSoilClassIdsGrid);
}

void cloneHohenheimDBTables2localSQLite()
{
	DBPtr hhcon(new MysqlDB("144.41.15.33", "mberg", "ZaLf2013", "carbiocial"));
	DBPtr lcon(new SqliteDB("../carbiocial.sqlite"));

	string tables[] = 
	{ "mpmas_crop_activity_ids" , 
		"tbl_crop_calendar", 
		"tbl_farm_types",
		"tbl_fertilizer",
		"tbl_fertilizer_use",
		"tbl_fertilizer_use_perennials",
		"tbl_municipalities",
		"tbl_operations",
		"tbl_production_practices",
		"tbl_products",
		"tbl_seasons",
		"tbl_soil_types",
		"tbl_unique_sectors_in_municipality",
		"tbl_yields",
		"tbl_yields_perennials"
	};
	
	string deleteAll("delete from ");
	string select("select * from ");
	string insert("insert or replace into ");

	for(int ti = 0; ti < 15; ti++)
	{
		string t = tables[ti];

		ostringstream doss;
		doss << deleteAll << t;
		lcon->del(doss.str());
		
		ostringstream soss;
		soss << select << t;

		hhcon->select(soss.str());
		DBRow row;
		while(!(row = hhcon->getRow()).empty())
		{
			ostringstream ioss;
			ioss << insert << t << " values (";
			for(int i = 0, size = row.size(); i < size; i++)
			{
				bool isString = 
					(t == "tbl_farm_types" && (i == 1 || i == 2)) ||
					(t == "tbl_fertilizer" && (i == 1 || i == 2)) ||
					(t == "tbl_fertilizer_use" && i == 2) ||
					(t == "tbl_fertilizer_use_perennials" && i == 2) ||
					(t == "tbl_municipalities" && (i == 0 || i == 2 || i  == 3 || i == 4)) ||
					(t == "tbl_operations" && (i == 1 || i == 2)) ||
					(t == "tbl_production_practices" && (i == 2 || i  == 3 || i == 4)) ||
					(t == "tbl_products" && (i == 1 || i == 2 || i  == 3)) ||
					(t == "tbl_seasons" && (i == 1 || i == 2)) ||
					(t == "tbl_soil_types" && (i == 1 || i == 2))
					? true : false;
				
				ioss << (isString ? "'" : "") << (row[i].empty() ? "NULL" : row[i]) << (isString ? "'" : "") << (i < (size - 1) ? ", " : "");
			}
			ioss << ")";

			//cout << ioss.str() << endl;
			bool success = lcon->insert(ioss.str());
			cout << (success ? "success" : "error") << " inserting " << ioss.str() << endl;
		}
	}

}

int main(int argc, char** argv)
{
	cloneHohenheimDBTables2localSQLite();

	//Tools::testRoundFloorCeil();

	/*
	sectorizeSubSolosGrid(
		"../solos-soil-class-ids_brazil-sinop_900.asc", 
		"../sectors_sinop_growing-window_round-to-10.txt",
		-1, sinop);
	//*/
	/*
	sectorizeSubSolosGrid(
		"../solos-soil-class-ids_brazil-campo-verde_900.asc", 
		"../sectors_campo-verde_growing-window_round-to-10.txt",
		-1, campoVerde);
		//*/

	/*
	sectorizeFullSolosGrid(
		"../solos-soil-class-ids_brazil_900.asc", 
		"../sectors-10x10-round-to-0.txt");
		//*/
	
	/*
	createSoilClassGrid(
		"../solos-profile-ids_sinop_900.asc",
		"../solos-soil-class-ids_sinop_900.asc");
	createSoilClassGrid(
		"../solos-profile-ids_campo-verde_900.asc",
		"../solos-soil-class-ids_campo-verde_900.asc");
	//*/

	/*
	createVoronoiSoilProfileGrid(
		"../solos_brazil_900.asc", 
		"../solos-profile-ids_brazil_900.asc",
		"../solos-soil-class-ids_brazil_900.asc");
		//*/
	
	exit(0);
}
