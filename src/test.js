'use strict';

console.log('Loading function');

/**
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - tableName: required for operations that interact with DynamoDB
 *   - payload: a parameter to pass to the operation being performed
 */


exports.handler = (event, context, callback) => {
   callback(null,{
    headers:{
        "Content-Type":"application/json"
    },
    statusCode: 200,
    body:JSON.stringify({event:event, context:context})});
};


