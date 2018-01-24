'use strict';
console.log('Loading function');

/**
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - tableName: required for operations that interact with DynamoDB
 *   - payload: a parameter to pass to the operation being performed
 */
let  MongoClient = require('mongodb').MongoClient;
let  ObjectID = require('mongodb').ObjectID;

let atlas_connection_uri;
let cachedDb = null;
let database = "general_data";
let collectionName = null;

const collectionHandlers = {
    "evidence" : {
        "GET": listItems,
        "POST": createItem
    },
    "profile" : {
        "GET": getItem_profile,
        "POST": postItem_profile
    }
  }
  
const itemHandlers = {
    "evidence" : {
        "DELETE": deleteItem,
        "GET": getItem,
        "PATCH": patchItem,
        "PUT": putItem
    }
}

const collections = {
    "evidence" : "evidence",
    "profile" : "profile"
}

exports.handler = (event, context, callback) => {
    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'] || null;
    console.log("URI", uri);

    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'] || null;
    console.log("URI", uri);
    
    if (atlas_connection_uri != null) {
        processEvent(event, context, callback);
    } 
    else {
        atlas_connection_uri = uri;
        console.log('the Atlas connection string is ' + atlas_connection_uri);
        processEvent(event, context, callback);
    } 
};

function processEvent(event, context, callback) {
    console.log('Calling MongoDB Atlas from AWS Lambda with event: ');
    // console.log('Calling MongoDB Atlas from AWS Lambda with event: ' + JSON.stringify(event));
    const jsonContents = JSON.parse(JSON.stringify(event));
    //const claims = getClaims(event);

    //the following line is critical for performance reasons to allow re-use of database connections across calls to this Lambda function and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, while subsequent, close calls will only take a few hundred milliseconds.
    context.callbackWaitsForEmptyEventLoop = false;
    
    try {
        if (cachedDb == null) {
            console.log('=> connecting to database');
            MongoClient.connect(atlas_connection_uri, function (err, db) {
                if (err) {
                    console.log("Error: ", err);
                    return callback(null, {
                        headers:{
                            "Content-Type":"application/json"
                        },
                        statusCode: 500,
                        body:JSON.stringify({error:err})
                    });
                    
                } else {
                    cachedDb = db;
                    console.log("About to getDoc")
                    return routeHandler(event, context, callback);
                }
            });
        }
        else {
            routeHandler(event, context, callback);
        }
    }
    catch (err) {
        console.error('an error occurred', err);
    }
}

function routeHandler(event, context, callback) {
    // get resource in the path.  ´/profile´or ´/evidence' for '/evidence/1234'
    let resource = event.resource.match(/^\/([^\/]+)/)[1];  
    collectionName = collections[resource];

    let id = (event["pathParameters"] !== null && "id" in event["pathParameters"]) ? event["pathParameters"]["id"] : undefined;
    let handlers = (id === undefined) ? collectionHandlers[resource] : itemHandlers[resource];

    let httpMethod = event["httpMethod"];
    if (httpMethod in handlers) {
        return handlers[httpMethod](event, context, callback);
    }
}


function listItems(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);
    const jsonContents = JSON.parse(JSON.stringify(event));
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    const options = {
        "limit": 20
    };

    var cursor = collection.find({userId:userId});
    cursor.toArray()
    .then((d) => {
        const response = buildResponse(200, {user:userId, data:d});
        callback(null, response);

    })
}
  
function createItem(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    let objectToInsert = JSON.parse(event.body);
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    objectToInsert['userId']=userId;

    collection.insertOne(objectToInsert)
    .then((r) => {
        const id = objectToInsert._id;
        const response = buildResponse(201, objectToInsert);

    
        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(500, err);

    
        callback(null, response);
    })
 
}

function deleteItem(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    const jsonContents = JSON.parse(JSON.stringify(event));
    const id = jsonContents.pathParameters['id'];
    let objectId = null;

    try {
        objectId = ObjectID(id);
    } catch(err) {
        console.log(err);
    }
    
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    collection.deleteOne({'_id':objectId, 'userId': userId})
    .then((r) => {
        const response = buildResponse(204, null);


        if (r && r.result && r.result.n == 0) {
            response.statusCode = 404;
        }
        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(500, err);

    
        callback(null, response);
    })
}

function getItem(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    const jsonContents = JSON.parse(JSON.stringify(event));
    const id = jsonContents.pathParameters['id'];
    
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    collection.findOne({'_id':ObjectID(id), 'userId': userId})
    .then((doc) => {
        const response = buildResponse(200, doc);

        if (!doc) {
            response.statusCode = 404;
        }
    
        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(500, err);

    
        callback(null, response);
    })
}

function patchItem(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    const objectToPatch = JSON.parse(event.body);

    const jsonContents = JSON.parse(JSON.stringify(event));
    const id = jsonContents.pathParameters['id'];
    let objectId = null;

    try {
        objectId = ObjectID(id);
    } catch(err) {
        console.log(err);
    }
    
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    collection.findAndModify(
        {"_id":objectId ,"userId": userId}, // Find object with these properties
        [['_id','asc']],  // If more than one found, sort by this criteris
        {$set:objectToPatch}, // Properties to be modified
        {"new": true} // Return the new object
    )
    .then((r) => {
        const response = buildResponse(200, r.value);


        if (r.lastErrorObject.n == 0) {
            response.statusCode = 404;
        }
    
        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(400, err);
    
        callback(null, response);
    })


}

function getItem_profile(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);
    
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    collection.findOne({'userId':userId})
    .then((doc) => {
        let response = buildResponse(200, doc)

        if (!doc) {
            response.statusCode = 200;
        }
    
        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(500, err);
        callback(null, response);
    })
}

function echo(event, context, callback) {
    const response = {
        statusCode: 201,
        headers: {
            "Content-Type" : "application/json",
            "Location": `/profile`
          },
        body: JSON.stringify(event)
    };

    callback(null, response);
}


function postItem_profile(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    let objectToUpsert = JSON.parse(event.body);
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    objectToUpsert['userId']=userId;

    collection.save(objectToUpsert)
    .then((r) => {
        const id = objectToUpsert._id;
        const response = buildResponse(201, objectToUpsert);

        callback(null, response);
    })
    .catch((err) => {
        const response = buildResponse(500, err);
        callback(null, response);
    })
}

function putItem(event, context, callback) {
    const collection = cachedDb.db(database).collection(collectionName);

    const object = JSON.parse(event.body);

    const jsonContents = JSON.parse(JSON.stringify(event));
    const id = jsonContents.pathParameters['id'];
    
    const claims = getClaims(event);
    const userId = claims && claims.sub;

    object['userId'] = userId;

    collection.findAndModify(
        {"_id":ObjectID(id),"userId": userId}, // Find object with these properties
        [['_id','asc']],  // If more than one found, sort by this criteris
        object, // Properties to be modified
        {"new": true} // Return the new object
    )
    .then((r) => {
        const response = {
            statusCode: 201,
            headers: {
                "Content-Type" : "application/json",
              },
            body: JSON.stringify(r.value)
        };
    
        callback(null, response);
    })
    .catch((err) => {
        const response = {
            statusCode: 500,
            headers: {
                "Content-Type" : "application/json"
              },
            body: JSON.stringify(err)
        };
    
        callback(null, response);
    })


}

function buildResponse(code, body) {
    const response = {
        statusCode: code,
        headers: {
            "Content-Type" : "application/json",
            "Access-Control-Allow-Origin" : "*",
          },
        body: JSON.stringify(body)
    };
    return response
}

function getClaims(event) {
    const token = event.headers['Authorization'];
    const tokenData = token.split('.')[1];
    const buf = Buffer.from(tokenData, 'base64').toString();
    const claims = JSON.parse(buf);
    return claims;
}



