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


//
// A FileDataSource uses a reader object to read data from an external file
//


simple FileDataSource extends DataSource
{
	parameters:	    	    
	    string filepath;  // path to the file to be opened
	    int buffer = default(4096);  // the buffer determines batching in reading the file.
}



simple CrawdadDataSource extends FileDataSource
{
    @class(CrawdadDataSource);
}
