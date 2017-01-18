//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
// 

package geomsim;



moduleinterface GMComponent { }



simple GMLocalHost extends LocalSite
{
    @class(GMLocalHost);
}


module GMLocalSite like ILocalSite
{
    parameters:
        int siteID;
        int streams;

    gates:
        input stream[streams];
        inout coord;
    
	submodules:
	    host: GMLocalHost {
	        siteID = siteID;
	        streams = streams;
	    };
	    protocol: <> like GMComponent;
	    
	connections:
	    host.coord <--> coord;
	    for i=0..streams-1 {
	        host.stream[i] <-- stream[i];
	    }
}


simple GMCoordHost extends Coordinator
{
    @class(GMCoordHost);
}


module GMCoordinator like ICoordinator
{
	parameters:
        int sites;
        int streams;
    gates:
        inout site[sites];
        
    submodules:
        host: GMCoordHost {
            sites = sites;
            streams = streams;
        }
        protocol: <> like GMComponent;        
        
    connections:
       	for i=0..sites-1 {
       	    site[i] <--> host.site[i];
       	}
}

