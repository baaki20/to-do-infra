// src/handlers/post_auth.js
const AWS = require('aws-sdk');
const sns = new AWS.SNS();

const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

exports.handler = async (event) => {
    console.log('PostAuthentication trigger received event:', JSON.stringify(event, null, 2));

    // Only subscribe the user on their initial sign-up confirmation.
    if (event.triggerSource !== 'PostAuthentication_ConfirmSignUp') {
        console.log('Trigger is not from sign-up confirmation. Skipping SNS subscription.');
        return event;
    }

    // Get the user's email from the Cognito event
    const userEmail = event.request.userAttributes.email;

    if (!userEmail) {
        console.error('User email not found in event.');
        return event;
    }

    const params = {
        Protocol: 'email',
        TopicArn: SNS_TOPIC_ARN,
        Endpoint: userEmail,
    };

    try {
        const result = await sns.subscribe(params).promise();
        console.log(`Successfully subscribed ${userEmail} to SNS topic. Subscription ARN: ${result.SubscriptionArn}`);
        // Return the event to Cognito so the authentication flow can proceed
        return event;
    } catch (error) {
        console.error(`Error subscribing user ${userEmail} to SNS topic:`, error);
        // It's important to still return the event even on error so as not to halt the authentication flow
        return event;
    }
};