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


/**
 * Sample connector using a few JSON records
 */
function demoConnector( parentDirPath ){
	
	// Call constructor from super class
	// TODO: Replace 'template' with the connector id (simple-data-pipe.name in package.json)
	// TODO: Replace 'Demo Connector' with the desired display name
	connectorExt.call(this, 'template', 'Demo Connector');
	
	/*
	 *
	 */
	this.getTablePrefix = function(){
		// TODO Replace 'template' with a unique prefix for your connector
		return 'template';
	};
	
	/*
	 *
	 */
	this.fetchRecords = function( table, pushRecordFn, done, pipeRunStep, pipeRunStats, logger, pipe, pipeRunner ){


		logger.debug('Fetching data from cloud data source.');

		// TODO: fetch data from source
		var records = 	[
		 				  {'firstname': 'George', 'lastname' : 'Clooney', 'address': 'hollywood blv', 'age': 57 },
		   				  {'firstname': 'Will', 'lastname' : 'Smith', 'address': 'Rodeo drive', 'age': 45 },
		   				  {'firstname': 'Brad', 'lastname' : 'Pitt', 'address': 'Beverly hills', 'age': 47 }
        				];

		// TODO: optional; enrich data

		// TODO: store data in Cloudant
		pushRecordFn(records);

		// invoke callback without an argument after data processing is complete
		// invoke callback with err argument in case of an issue
		return done();

	};
}

//Extend event Emitter
require('util').inherits(demoConnector, connectorExt);

module.exports = new demoConnector();