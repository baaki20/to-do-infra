const AWS = require('aws-sdk');
const sqs = new AWS.SQS();

const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;

const unmarshall = (item) => AWS.DynamoDB.Converter.unmarshall(item);

exports.handler = async (event) => {
    console.log('Received DynamoDB Stream event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            if (record.dynamodb.NewImage && unmarshall(record.dynamodb.NewImage).EntityType !== 'TASK') {
                continue;
            }

            const eventName = record.eventName;
            const newImage = record.dynamodb.NewImage ? unmarshall(record.dynamodb.NewImage) : null;
            const oldImage = record.dynamodb.OldImage ? unmarshall(record.dynamodb.OldImage) : null;
            
            const userId = newImage ? newImage.PK.split('#')[1] : oldImage.PK.split('#')[1];
            const taskId = newImage ? newImage.SK.split('#')[1] : oldImage.SK.split('#')[1];
            const deadline = newImage ? newImage.Deadline : null;
            const status = newImage ? newImage.Status : (oldImage ? oldImage.Status : null);

            const messageGroupId = taskId; 
            const baseParams = {
                QueueUrl: SQS_QUEUE_URL,
                MessageGroupId: messageGroupId,
                MessageBody: JSON.stringify({ userId, taskId }),
                MessageDeduplicationId: taskId,
            };

            if (eventName === 'INSERT' && status === 'Pending') {
                const deadlineTime = new Date(deadline).getTime();
                const now = Date.now();
                
                let delaySeconds = Math.max(0, Math.floor((deadlineTime - now) / 1000));
                
                if (delaySeconds > 900) {
                    console.warn(`Task ${taskId} deadline is too far out for SQS DelaySeconds. Deadline: ${deadline}`);
                    delaySeconds = 900; 
                }

                console.log(`Scheduling expiry for task ${taskId} in ${delaySeconds} seconds.`);
                await sqs.sendMessage({
                    ...baseParams,
                    DelaySeconds: delaySeconds,
                }).promise();

            } else if (eventName === 'MODIFY' || eventName === 'REMOVE') {
                const oldStatus = oldImage ? oldImage.Status : null;
                
                if (oldStatus === 'Pending' && (status === 'Completed' || eventName === 'REMOVE')) {
                    console.log(`Task ${taskId} completed/deleted. Sending immediate SQS message to cancel potential expiry.`);
                    await sqs.sendMessage({
                        ...baseParams,
                        MessageBody: JSON.stringify({ userId, taskId, action: 'CANCEL' }),
                        DelaySeconds: 0, 
                    }).promise();
                }
            }
        } catch (error) {
            console.error('Error processing stream record:', error);
            throw error;
        }
    }
};