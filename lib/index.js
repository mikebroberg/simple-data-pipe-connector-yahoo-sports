//-------------------------------------------------------------------------------
// Copyright IBM Corp. 2015
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-------------------------------------------------------------------------------

'use strict';

var pipesSDK = require('simple-data-pipe-sdk');
var connectorExt = pipesSDK.connectorExt;
var _ = require('lodash');


/**
 * Sample connector using a few JSON records
 */
function demoConnector( parentDirPath ){

	// TODO: Replace 'template' with a unique connector id (Should match the value of property "simple_data_pipe"."name" in package.json)	
	var connectorID = 'template';

	// Call constructor from super class

	// TODO: Replace 'Sample data source' with the desired cnnector display name
	// TODO: Customize connector options
	connectorExt.call(this, 
					 connectorID, 			
					 'Sample data source', 
					 { 
					  recreateTargetDb: true // if set (default: false) all data from the staging database is cleared prior to data load	
					 });	
	

	/*
	 * @return list of data sets (also referred to as tables for legacy reasons) from which the user can choose from
	 */
	this.getTables = function(){

		var dataSets = [];

		// dataSets.push({name:'dataSetName', labelPlural:'dataSetName'}); // assign the name (display name) to each property

		// in this sample the data source contains two static data sets, named 'sample data set 1' and 'sample data set 2'

		var dataSetName = 'sample data set 1';
		dataSets.push({name:dataSetName, labelPlural:dataSetName});

		dataSetName = 'sample data set 2';
		dataSets.push({name:dataSetName, labelPlural:dataSetName});

		return dataSets;
	};

	/*
	 * Customization optional.
	 */
	this.getTablePrefix = function(){
		// The prefix is used to generate names for the Cloudant staging databases that hold your data.
		return connectorID;
	};
	
	/*
	 * @param pipe - data pipe configuration
	 */
	this.fetchRecords = function( dataSet, pushRecordFn, done, pipeRunStep, pipeRunStats, logger, pipe, pipeRunner ){

		// The data set is typically selected by the user in the "Filter Data" panel during the pipe configuration step
		// dataSet: {name: 'data set name'}

		// Bunyan logging - https://github.com/trentm/node-bunyan
		logger.debug('Fetching data set ' + dataSet.name + ' from cloud data source.');

		// TODO: fetch data from source
		// in this sample the data source contains two static data sets that can be retrieved

		var dataSet1Data = 	[
		 						{'firstname': 'George', 'lastname' : 'Clooney', 'address': 'hollywood blv', 'age': 57 },
		   				  		{'firstname': 'Will', 'lastname' : 'Smith', 'address': 'Rodeo drive', 'age': 45 },
		   				  		{'firstname': 'Brad', 'lastname' : 'Pitt', 'address': 'Beverly hills', 'age': 47 }
        					];     					

		var dataSet2Data = 	[
		 						{'firstname': 'Kevin', 'lastname' : 'Bacon', 'address': 'Hollywood blv', 'age': 47 },
		   				  		{'firstname': 'Sandra', 'lastname' : 'Bullock', 'address': 'Sunset Heights', 'age': 45 },
		   				  		{'firstname': 'Leonardo', 'lastname' : 'DiCaprio', 'address': 'Sunrise Hills', 'age': 42 },
		   				  		{'firstname': 'Jennifer', 'lastname' : 'Lawrence', 'address': 'Sunny Side', 'age': 26 },
        					];


		var records = dataSet1Data;
		if(dataSet.name === 'sample data set 2') {
			records = dataSet2Data;
		}		


        logger.info('Enriching data.');        				

		// TODO: optional; enrich data
		_.forEach(records, function(record){
				record.username = record.firstname + '_' + record.lastname;

		});

		// TODO: invoke pushRecordFn to store records in Cloudant; 
		//       the total number of records is automatically calculated if this function is invoked multiple times
		// Parameter: pass a single record or a set of records to be persisted
		//             
		pushRecordFn(records);

		// Invoke done callback function after data processing is complete.
		// Accepts an optional parameter string, which is displayed to the end user, along with the record count 
		// after the data pipe run completed
		return done('This is a static data set.');

	};
}

//Extend event Emitter
require('util').inherits(demoConnector, connectorExt);

module.exports = new demoConnector();