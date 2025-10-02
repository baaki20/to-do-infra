const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

const ddb = new AWS.DynamoDB.DocumentClient();
const TABLE_NAME = process.env.TABLE_NAME;

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    let body;
    let statusCode = 200;
    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
    };

    try {
        const userId = event.requestContext.authorizer.claims.sub;
        if (!userId) {
            throw new Error('User ID not found in token.');
        }

        switch (event.httpMethod) {
            case 'DELETE':
                body = await ddb.delete({
                    TableName: TABLE_NAME,
                    Key: {
                        PK: `USER#${userId}`,
                        SK: `TASK#${event.pathParameters.taskId}`
                    }
                }).promise();
                break;
            case 'POST':
                const requestJSON = JSON.parse(event.body);
                const taskId = uuidv4();
                const deadline = new Date(Date.now() + 5 * 60 * 1000).toISOString();

                await ddb.put({
                    TableName: TABLE_NAME,
                    Item: {
                        PK: `USER#${userId}`,
                        SK: `TASK#${taskId}`,
                        EntityType: 'TASK',
                        Description: requestJSON.Description,
                        Date: new Date().toISOString(),
                        Status: 'Pending',
                        Deadline: deadline,
                    }
                }).promise();
                body = { TaskId: taskId };
                break;
            case 'GET':
                if (event.pathParameters && event.pathParameters.taskId) {
                    body = await ddb.get({
                        TableName: TABLE_NAME,
                        Key: {
                            PK: `USER#${userId}`,
                            SK: `TASK#${event.pathParameters.taskId}`
                        }
                    }).promise();
                } else {
                    body = await ddb.query({
                        TableName: TABLE_NAME,
                        KeyConditionExpression: 'PK = :pk',
                        ExpressionAttributeValues: {
                            ':pk': `USER#${userId}`
                        }
                    }).promise();
                }
                break;
            case 'PUT':
                const updateJSON = JSON.parse(event.body);
                const params = {
                    TableName: TABLE_NAME,
                    Key: {
                        PK: `USER#${userId}`,
                        SK: `TASK#${event.pathParameters.taskId}`
                    },
                    UpdateExpression: 'set #s = :status',
                    ExpressionAttributeNames: {
                        '#s': 'Status'
                    },
                    ExpressionAttributeValues: {
                        ':status': updateJSON.Status
                    },
                    ReturnValues: 'UPDATED_NEW'
                };
                body = await ddb.update(params).promise();
                break;
            default:
                throw new Error(`Unsupported method "${event.httpMethod}"`);
        }
    } catch (err) {
        statusCode = 400;
        body = err.message;
    } finally {
        body = JSON.stringify(body);
    }

    return {
        statusCode,
        body,
        headers,
    };
};