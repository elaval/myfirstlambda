'use strict';
console.log('Loading function');

/**
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - tableName: required for operations that interact with DynamoDB
 *   - payload: a parameter to pass to the operation being performed
 */
var MongoClient = require('mongodb').MongoClient;

let atlas_connection_uri;
let cachedDb = null;

const collectionHandlers = {
    "GET": listItems,
    "POST": createItem
  }
  
const itemHandlers = {
    "DELETE": deleteItem,
    "GET": getItem,
    "PATCH": patchItem,
    "POST": postItem,
    "PUT": putItem
}

exports.handler = (event, context, callback) => {
    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'] || null;
    console.log("URI", uri);

    let id = (event["pathParameters"] !== null && "id" in event["pathParameters"]) ? event["pathParameters"]["id"] : undefined;
    let handlers = (id === undefined) ? collectionHandlers : itemHandlers;
    
    let httpMethod = event["httpMethod"];
    if (httpMethod in handlers) {
        return handlers[httpMethod](event, context, callback);
    }
};


function listItems(event, context, callback) {
    const response = {
      statusCode: 200,
      headers: {
        "Content-Type" : "application/json"
      },
      body: JSON.stringify({ action: "listItems" })
    };
  
    callback(null, response);
  }
  
function createItem(event, context, callback) {
    const response = {
        statusCode: 201,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "createItem" })
    };

    callback(null, response);
}

function deleteItem(event, context, callback) {
    const response = {
        statusCode: 204,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "deleteItem", id: event["pathParameters"]["id"] })
    };

    callback(null, response);
}

function getItem(event, context, callback) {
    const response = {
        statusCode: 200,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "getItem", id: event["pathParameters"]["id"] })
    };

    callback(null, response);
}

function patchItem(event, context, callback) {
    const response = {
        statusCode: 200,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "patchItem", id: event["pathParameters"]["id"] })
    };

    callback(null, response);
}

function postItem(event, context, callback) {
    const response = {
        statusCode: 200,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "postItem", id: event["pathParameters"]["id"] })
    };

    callback(null, response);
}

function putItem(event, context, callback) {
    const response = {
        statusCode: 200,
        headers: {
            "Content-Type" : "application/json"
          },
        body: JSON.stringify({ action: "putItem", id: event["pathParameters"]["id"] })
    };

    callback(null, response);
}

function getClaims(event) {
    const token = event.headers['Authorization'];
    const tokenData = token.split('.')[1];
    const buf = Buffer.from(tokenData, 'base64').toString();
    const claims = JSON.parse(buf);
    return claims;
}

function processEvent(event, context, callback) {
    console.log('Calling MongoDB Atlas from AWS Lambda with event: ');
    // console.log('Calling MongoDB Atlas from AWS Lambda with event: ' + JSON.stringify(event));
    const jsonContents = JSON.parse(JSON.stringify(event));
    const claims = getClaims(event);

    //the following line is critical for performance reasons to allow re-use of database connections across calls to this Lambda function and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, while subsequent, close calls will only take a few hundred milliseconds.
    context.callbackWaitsForEmptyEventLoop = false;
    
    try {
        if (cachedDb == null) {
            console.log('=> connecting to database');
            MongoClient.connect(atlas_connection_uri, function (err, db) {
                if (err) {
                    console.log("Error: ", err);
                    return;
                } else {
                    cachedDb = db;
                    console.log("About to getDoc")
                    return getDoc(db, jsonContents, claims, callback);
                }
            });
        }
        else {
            getDoc(cachedDb, jsonContents, claims, callback);
        }
    }
    catch (err) {
        console.error('an error occurred', err);
    }
}

function getDoc (db, json, claims, callback) {
    const collection = db.db("educacion").collection("establecimientos");
    const rbd = json.pathParameters.id;
    console.log(json.pathParameters, rbd);

    collection.findOne({RBD:rbd})
    .then((d) => {
        console.log("After findOne", rbd,d);
        callback(null, {
            headers:{
                "Content-Type":"application/json"
            },
            statusCode: 200,
            body:JSON.stringify({user:claims.sub, data:d})
        });
    })
    .catch((err) => {
        console.error("an error occurred in createDoc", err);
        callback(null, JSON.stringify(err));
    })

};


