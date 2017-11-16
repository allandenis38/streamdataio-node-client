/*
 Copyright 2016 Streamdata.io

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// add EventSource dependency
var EventSource = require('eventsource');
// add json patch dependency
var JsonPatch   = require('fast-json-patch');
// // add http dependency
// var Http        = require('http');
// add Request dependency
var request = require('request');

var myObjects = [];

function server() {
  // define variables
  var eventSource = null;

  // initialize data 
  var data = [];

  // rdefine endpoint's url to which updated data will be forwarded
  var restEndpointUrl = 'http://localhost:8081/';

  function connect() {

  // building the URL to get your API streamed as follow:

    // targetUrl is the JSON API you wish to stream
    // you can use this example API which simulates updating stocks prices from a financial market
    var targetUrl = 'http://stockmarket.streamdata.io/v2/prices';

    // appToken is the way Streamdata.io authenticates you as a valid user.
    // you MUST provide a valid token for your request to go through.
    var appToken = 'YOUR_SDIO_TOKEN';

    // finally the url you will request is composed as follow
    // the call to your target API is made through https://streamdata.motwin.net/ proxy
    var url = 'https://streamdata.motwin.net/' + targetUrl + '?X-Sd-Token=' + appToken;

    // simply use the eventsource API to get connected
    var eventSource = new EventSource(url);

    // add callbacks to react to EventSource events

    // the standard 'open' callback will be called when connection is established with the server
    eventSource.addEventListener('open', function() {
      console.log("connected!");
    });

    // the standard 'error' callback will be called when an error occur with the evenSource
    // for example with an invalid token provided
    eventSource.addEventListener('error', function(e) {
      console.log('ERROR!', e);
      eventSource.close();
    });

    // the streamdata.io specific 'data' event will be called when a fresh Json data set 
    // is pushed by Streamdata.io coming from the API
    eventSource.addEventListener('data', function(e) {
      console.log("data: \n" + e.data);
      // memorize the fresh data set
      data = JSON.parse(e.data);
      
      // send initial snapshot
      requestRestAPI("POST", data);
    });

    // the streamdata.io specific 'patch' event will be called when a fresh Json patch 
    // is pushed by streamdata.io from the API. This patch has to be applied to the 
    // latest data set provided.
    eventSource.addEventListener('patch', function(e) {
      // display the patch
      console.log("patch: \n" + e.data);
      
      // apply the patch to data using json patch API
      var patches = JSON.parse(e.data);

      data = JsonPatch.applyPatch(data, patches).newDocument;

      forwardData(patches);
    });
  };

  function forwardData(patches) {
    // define buckets to store modified items
    var addedData = [];
    var removedData = [];
    var updatedData = [];

    // decode each patch and store it in appropriate batch bucket
    patches.forEach( function(patch) {
      
      //Extract object index from json-patch path 
      var objectPathIndex = getObjectPathIndex(patch);
      
      switch(patch.op) {
        case 'add':
          addedData.push(data[objectPathIndex]);
          break;
        case 'remove':
          removedData.push(data[objectPathIndex]);
          break;
        case 'replace':
          updatedData.push(data[objectPathIndex]);
          break;
        default:
          console.log("Error: Unsupported operation");
      }
    });

    (addedData.length > 0) ? requestRestAPI("POST", addedData) : console.log("No data to add");
    (updatedData.length > 0) ? requestRestAPI("PUT", updatedData) : console.log("No data to update");
    (removedData.length > 0) ? requestRestAPI("DELETE", removedData) : console.log("No data to delete");
  }

  function requestRestAPI(method, data) {
    request({
      url: restEndpointUrl, 
      method: method,
      json: data,
      }, 
      function(err,response,body){ 
        console.log('Response status code:', response && response.statusCode);
        if (err) { console.error('error:', err); }       
      })
  }

  function getObjectPathIndex(patch) {
    var sepIndex = patch.path.indexOf('/', 1);
    return patch.path.substring(1, sepIndex);
  }

  connect();
}

console.log('starting');
server();

