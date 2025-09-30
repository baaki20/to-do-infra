// src/handlers/stream_handler.js
const AWS = require('aws-sdk');
const sqs = new AWS.SQS();

const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;

// Helper to convert DynamoDB JSON structure to a standard JavaScript object
const unmarshall = (item) => AWS.DynamoDB.Converter.unmarshall(item);

exports.handler = async (event) => {
    console.log('Received DynamoDB Stream event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            // Only process records that are tasks
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

            // The MessageGroupId ensures that all events for a specific taskId are processed in order.
            const messageGroupId = taskId; 
            const baseParams = {
                QueueUrl: SQS_QUEUE_URL,
                MessageGroupId: messageGroupId,
                MessageBody: JSON.stringify({ userId, taskId }),
                MessageDeduplicationId: taskId, // Use TaskId as Deduplication ID
            };

            if (eventName === 'INSERT' && status === 'Pending') {
                // 1. New Task Created: Schedule the expiry event using DelaySeconds
                const deadlineTime = new Date(deadline).getTime();
                const now = Date.now();
                
                // Calculate time difference in seconds. Max SQS delay is 15 minutes (900 seconds)
                let delaySeconds = Math.max(0, Math.floor((deadlineTime - now) / 1000));
                
                // Note: If deadline is > 15 min, this requires a more complex scheduler (e.g., EventBridge Scheduler). 
                // We'll proceed with the SQS FIFO delay (max 15 minutes) as per common project scope limitations.
                if (delaySeconds > 900) {
                    console.warn(`Task ${taskId} deadline is too far out for SQS DelaySeconds. Deadline: ${deadline}`);
                    // For now, schedule at max delay, or use a shorter duration for testing.
                    // For this project, assume deadlines are kept within the SQS limit or a warning is logged.
                    delaySeconds = 900; 
                }

                console.log(`Scheduling expiry for task ${taskId} in ${delaySeconds} seconds.`);
                await sqs.sendMessage({
                    ...baseParams,
                    DelaySeconds: delaySeconds,
                }).promise();

            } else if (eventName === 'MODIFY' || eventName === 'REMOVE') {
                // 2. Task Completed or Deleted: Send an immediate message to ensure idempotency check.
                // The expiry.js handler will see Status != 'Pending' and ignore the delayed expiry message.
                
                // Check if the status changed from Pending to Completed/Expired/Deleted
                const oldStatus = oldImage ? oldImage.Status : null;
                
                if (oldStatus === 'Pending' && (status === 'Completed' || eventName === 'REMOVE')) {
                    console.log(`Task ${taskId} completed/deleted. Sending immediate SQS message to cancel potential expiry.`);
                    
                    // Send message immediately to SQS FIFO. It ensures the status change is processed
                    // before the (still delayed) expiry message.
                    await sqs.sendMessage({
                        ...baseParams,
                        MessageBody: JSON.stringify({ userId, taskId, action: 'CANCEL' }),
                        DelaySeconds: 0, 
                    }).promise();
                }
            }
        } catch (error) {
            console.error('Error processing stream record:', error);
            // Throwing the error here will retry the batch of records
            throw error;
        }
    }
};