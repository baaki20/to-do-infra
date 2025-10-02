// src/handlers/expiry.js
const AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

const TABLE_NAME = process.env.TABLE_NAME;
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

exports.handler = async (event) => {
    console.log('Received SQS event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const messageBody = JSON.parse(record.body);
            const { userId, taskId } = messageBody;

            const task = await ddb.get({
                TableName: TABLE_NAME,
                Key: {
                    PK: `USER#${userId}`,
                    SK: `TASK#${taskId}`
                }
            }).promise();

            if (task.Item && task.Item.Status === 'Pending') {
                await ddb.update({
                    TableName: TABLE_NAME,
                    Key: {
                        PK: `USER#${userId}`,
                        SK: `TASK#${taskId}`
                    },
                    UpdateExpression: 'set #s = :expired',
                    ExpressionAttributeNames: {
                        '#s': 'Status'
                    },
                    ExpressionAttributeValues: {
                        ':expired': 'Expired'
                    }
                }).promise();
                console.log(`Task ${taskId} for user ${userId} updated to 'Expired'`);

                const subject = `Your Task Has Expired!`;
                const message = `Your task "${task.Item.Description}" has reached its deadline and is now marked as expired.`;
                await sns.publish({
                    TopicArn: SNS_TOPIC_ARN,
                    Subject: subject,
                    Message: message
                }).promise();
                console.log(`Expiry notification sent for task ${taskId}`);
            } else {
                console.log(`Task ${taskId} is not pending, no action needed.`);
            }

        } catch (error) {
            console.error('Error processing SQS record:', error);
            throw error;
        }
    }
    return {};
};