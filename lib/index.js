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
 * Sample connector that stores a few JSON records in Cloudant
 * Build your own connector by following the TODO instructions
 */
function demoConnector( parentDirPath ){

	 /* 
	  * Customization is mandatory
	  */

	// TODO: 
	//   Replace 'template' with a unique connector id (Should match the value of property "simple_data_pipe"."name" in package.json)	
	//   Replace 'Sample Data Source' with the desired display name of the data source (e.g. reddit) from which data will be loaded
	var connectorInfo = {
						  id: 'template',				// internal connector ID 
						  name: 'Sample Data Source'	// connector display name
						};

	// TODO						
	var connectorOptions = {
					  		recreateTargetDb: true, // if set (default: false) all data currently stored in the staging database is removed prior to data load
					  		useCustomTables: true   // keep true (default: false)
						   };						

	// Call constructor from super class; 
	connectorExt.call(this, 
					 connectorInfo.id, 			
					 connectorInfo.name, 
					 connectorOptions	  
					 );	

	/*
	 * Customization is mandatory!
	 * @return list of data sets (also referred to as tables for legacy reasons) from which the user can choose from
	 */
	this.getTables = function(){

		var dataSets = [];

		// TODO: 'Define' the data set or data sets that can be loaded from the data source. The user gets to choose one.
		// dataSets.push({name:'dataSetName', labelPlural:'dataSetName'}); // assign the name to each property

		/* 
		 * In this example the data source contains two static data sets, named 'sample data set 1' and 'sample data set 2'.
		 */

			// data set 1
			var dataSetName = 'sample data set 1';
		    // dataSets.push({name:'dataSetName', labelPlural:'dataSetName'}); // assign the name to each property
			dataSets.push({name:dataSetName, labelPlural:dataSetName});

			// data set 2
			dataSetName = 'sample data set 2';
			dataSets.push({name:dataSetName, labelPlural:dataSetName});

			// In the UI the user gets to choose from: 
			//  -> sample data set 1
			//  -> sample data set 2

			// Sometimes you might want to provide the user with the option to load all data sets concurrently
			// To enable that feature, define a single data set that contains only property 'labelPlural' 
			dataSets.push({labelPlural:'All data sets'});

			// In the UI the user gets to choose from: 
			//  -> All data sets
			//  -> sample data set 1
			//  -> sample data set 2

		// sort list; if present, the ALL_DATA option should be displayed first
		return dataSets.sort(function (dataSet1, dataSet2) {

															if(! dataSet1.name)	// ALL_DATA (only property labelPlural is defined)
																return -1;

															if(! dataSet2.name) // ALL_DATA (only property labelPlural is defined)
																return 1;

															return dataSet1.name - dataSet2.name;
														   });
	}; // getTables

	/*
	 * Customization is not needed.
	 */
	this.getTablePrefix = function(){
		// The prefix is used to generate names for the Cloudant staging databases that hold your data. The recommended
		// value is the connector ID to assure unqiueness.
		return connectorInfo.id;
	};
	
	/*
	 * Customization is mandatory!
	 * @param pipe - data pipe configuration
	 */
	this.fetchRecords = function( dataSet, pushRecordFn, done, pipeRunStep, pipeRunStats, logger, pipe, pipeRunner ){

		// The data set is typically selected by the user in the "Filter Data" panel during the pipe configuration step
		// dataSet: {name: 'data set name'}. However, if you enabled the ALL option (see get Tables)

		// Bunyan logging - https://github.com/trentm/node-bunyan
		logger.debug('Fetching data set ' + dataSet.name + ' from cloud data source.');

		// TODO: fetch data from cloud data source
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
		// after the data pipe run completed.
		return done('This is a static data set.');

	};
}

//Extend event Emitter
require('util').inherits(demoConnector, connectorExt);

module.exports = new demoConnector();