using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQS_Mover
{
    public class AWSSQSHelper
    {
        ///////////////////////////////////////////////////////////////////////
        //                           Fields                                  //
        ///////////////////////////////////////////////////////////////////////

        public IAmazonSQS queue { get; set; }   // AMAZON simple queue service reference
        public GetQueueUrlResponse queueurl { get; set; }   // AMAZON queue url
        public ReceiveMessageRequest rcvMessageRequest { get; set; }   // AMAZON receive message request
        public ReceiveMessageResponse rcvMessageResponse { get; set; }   // AMAZON receive message response
        public DeleteMessageRequest delMessageRequest { get; set; }   // AMAZON delete message request

        public bool IsValid { get; set; }   // True when the queue is OK

        public int ErrorCode { get; set; }   // Last error code
        public string ErrorMessage { get; set; }   // Last error message

        public const int e_Exception = -1;

        private Object padLock = new Object ();

        ///////////////////////////////////////////////////////////////////////
        //                    Methods & Functions                            //
        ///////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Class constructor
        /// </summary>
        public AWSSQSHelper ()
        {
        }

        /// <summary>
        /// Class constructor
        /// </summary>
        public AWSSQSHelper (string queuename, int maxnumberofmessages, String AWSAccessKey = "", String AWSSecretKey = "")
        {
            OpenQueue (queuename, maxnumberofmessages, AWSAccessKey, AWSSecretKey);
        }

        /// <summary>
        /// The method clears the error information associated with the queue
        /// </summary>
        private void ClearErrorInfo ()
        {
            ErrorCode = 0;
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// The method opens the queue
        /// </summary>
        public bool OpenQueue (string queuename, int maxnumberofmessages, String AWSAccessKey, String AWSSecretKey)
        {
            ClearErrorInfo ();

            IsValid = false;

            if (!string.IsNullOrWhiteSpace (queuename))
            {
                // Checking for the need to use provided credentials instead of reading from app.Config
                if (!String.IsNullOrWhiteSpace (AWSSecretKey) && !String.IsNullOrWhiteSpace (AWSSecretKey))
                {
                    AWSCredentials awsCredentials = new BasicAWSCredentials (AWSAccessKey, AWSSecretKey);
                    queue = AWSClientFactory.CreateAmazonSQSClient (awsCredentials, RegionEndpoint.USEast1);
                }
                else
                {
                    queue = AWSClientFactory.CreateAmazonSQSClient (RegionEndpoint.USEast1);
                }

                try
                {
                    // Get queue url
                    GetQueueUrlRequest sqsRequest = new GetQueueUrlRequest ();
                    sqsRequest.QueueName = queuename;
                    queueurl = queue.GetQueueUrl (sqsRequest);

                    // Format receive messages request
                    rcvMessageRequest = new ReceiveMessageRequest ();
                    rcvMessageRequest.QueueUrl = queueurl.QueueUrl;
                    rcvMessageRequest.MaxNumberOfMessages = maxnumberofmessages;

                    // Format the delete messages request
                    delMessageRequest = new DeleteMessageRequest ();
                    delMessageRequest.QueueUrl = queueurl.QueueUrl;

                    IsValid = true;
                }
                catch (Exception ex)
                {
                    ErrorCode = e_Exception;
                    ErrorMessage = ex.Message;
                }
            }

            return IsValid;
        }

        /// <summary>
        /// Returns the approximate number of queued messages
        /// </summary>
        public int ApproximateNumberOfMessages ()
        {
            ClearErrorInfo ();

            int result = 0;
            try
            {
                GetQueueAttributesRequest attrreq = new GetQueueAttributesRequest ();
                attrreq.QueueUrl = queueurl.QueueUrl;
                attrreq.AttributeNames.Add ("ApproximateNumberOfMessages");
                GetQueueAttributesResponse attrresp = queue.GetQueueAttributes (attrreq);
                if (attrresp != null)
                    result = attrresp.ApproximateNumberOfMessages;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// The method loads a one or more messages from the queue
        /// </summary>
        public bool DeQueueMessages (List<String> messageAttributes = null)
        {
            ClearErrorInfo ();

            if (messageAttributes != null)
            {
                rcvMessageRequest.AttributeNames = messageAttributes;
            }

            try
            {
                rcvMessageResponse = queue.ReceiveMessage (rcvMessageRequest);
                return true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxWaitTimeInMilliseconds"></param>
        /// <param name="waitCallback">
        /// Function callback whenever there is no message queued. <para/>
        /// Input parameters: retry count, wait time. <para/>
        /// Returns a boolean indicating if we should continue or not.</param>
        /// <param name="throwOnError"></param>
        /// <returns></returns>
        public IEnumerable<Message> GetMessagesWithWait (int maxWaitTimeInMilliseconds = 1800000, Func<int, int, bool> waitCallback = null, bool throwOnError = false)
        {
            int fallbackWaitTime = 1;

            // start dequeue loop
            do
            {
                // dequeue messages
                foreach (var message in GetMessages (throwOnError))
                {
                    // Reseting fallback time
                    fallbackWaitTime = 1;

                    // process message
                    yield return message;
                }

                // If no message was found, increases the wait time
                int waitTime;
                if (fallbackWaitTime <= 12)
                {
                    // Exponential increase on the wait time, truncated after 12 retries
                    waitTime = Convert.ToInt32 (Math.Pow (2, fallbackWaitTime) * 1000);
                }
                else // Reseting Wait after 12 fallbacks
                {
                    waitTime = 2000;
                    fallbackWaitTime = 0;
                }

                if (waitTime > maxWaitTimeInMilliseconds)
                    waitTime = maxWaitTimeInMilliseconds;

                fallbackWaitTime++;

                // Sleeping before next try
                //Console.WriteLine ("Fallback (seconds) => " + waitTime);
                if (waitCallback != null)
                {
                    if (!waitCallback (fallbackWaitTime, waitTime))
                        break;
                }
                Thread.Sleep (waitTime);

            } while (true); // Loops Forever
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="throwOnError"></param>
        /// <returns></returns>
        public IEnumerable<Message> GetMessages (bool throwOnError = false)
        {
            do
            {
                // Dequeueing messages from the Queue
                if (!DeQueueMessages ())
                {
                    Thread.Sleep (250); // Hiccup                   
                    continue;
                }

                // Checking for no message received, and false positives situations
                if (!AnyMessageReceived ())
                {
                    break;
                }

                // Iterating over dequeued messages
                IEnumerable<Message> messages = null;
                try
                {
                    messages = GetDequeuedMessages ();
                }
                catch (Exception ex)
                {
                    ErrorCode = e_Exception;
                    ErrorMessage = ex.Message;
                    if (throwOnError)
                        throw ex;
                }

                if (messages == null) continue;

                foreach (Message awsMessage in messages)
                {
                    yield return awsMessage;
                }

            } while (true); // Loops Forever
        }

        /// <summary>
        /// The method deletes the message from the queue
        /// </summary>
        public bool DeleteMessage (Message message)
        {
            ClearErrorInfo ();

            bool result = false;
            try
            {
                delMessageRequest.ReceiptHandle = message.ReceiptHandle;
                queue.DeleteMessage (delMessageRequest);
                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        public bool DeleteMessages (IList<Message> messages)
        {
            ClearErrorInfo ();

            try
            {
                int skip = 0;
                int limit = 10;

                // Iterating to remove in blocks of 10
                while (skip < messages.Count)
                {
                    var request = new DeleteMessageBatchRequest
                    {
                        QueueUrl = queueurl.QueueUrl,
                        Entries = messages.Skip (skip).Take (limit).Select (i => new DeleteMessageBatchRequestEntry (i.MessageId, i.ReceiptHandle)).ToList ()
                    };

                    // Delete messages request
                    var response = queue.DeleteMessageBatch (request);

                    // Inc. Skip
                    skip += limit;

                    // Errors Check
                    if (response.Failed != null && response.Failed.Count > 0)
                    {
                        ErrorMessage = String.Format ("ErrorCount: {0}, Messages: [{1}]", response.Failed.Count,
                            String.Join (",", response.Failed.Select (i => i.Message).Distinct ()));

                        // Retrying failed messages
                        var retryList = messages.Where (i => response.Failed.Any (j => j.Id == i.MessageId));
                        foreach (var e in retryList)
                        {
                            if (!DeleteMessage (e))
                            {
                                return false;
                            }
                        }
                    }
                }

                return String.IsNullOrEmpty (ErrorMessage);
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return false;
        }

        public bool EnqueueMessages (IList<string> messages)
        {
            ClearErrorInfo ();

            bool result = false;
            try
            {
                var request = new SendMessageBatchRequest
                {
                    QueueUrl = queueurl.QueueUrl
                };

                List<SendMessageBatchRequestEntry> entries = new List<SendMessageBatchRequestEntry> ();

                // Messages counter
                int ix = 0;

                // Iterating until theres no message left
                while (ix < messages.Count)
                {
                    entries.Clear ();

                    // Storing upper limit of iteration
                    var len = Math.Min (ix + 10, messages.Count);

                    // Iterating over 10
                    for (var i = ix; i < len; i++)
                    {
                        entries.Add (new SendMessageBatchRequestEntry (i.ToString (), messages[i]));
                        ix++;
                    }

                    // Renewing entries from the object
                    request.Entries = entries;

                    // Batch Sending
                    var response = queue.SendMessageBatch (request);

                    // If any message failed to enqueue, use individual enqueue method
                    if (response.Failed != null && response.Failed.Count > 0)
                    {
                        // Hiccup
                        Thread.Sleep (100);

                        foreach (var failedMessage in response.Failed)
                        {
                            // Individual Enqueues
                            EnqueueMessage (failedMessage.Message);
                        }
                    }

                }

                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// Inserts a message in the queue
        /// </summary>
        public bool EnqueueMessage (string msgbody)
        {
            ClearErrorInfo ();

            bool result = false;
            try
            {
                SendMessageRequest sendMessageRequest = new SendMessageRequest ();
                sendMessageRequest.QueueUrl = queueurl.QueueUrl;
                sendMessageRequest.MessageBody = msgbody;
                queue.SendMessage (sendMessageRequest);
                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// Inserts a message in the queue and retries when an error is detected
        /// </summary>
        public bool EnqueueMessage (string msgbody, int maxretries)
        {
            // Insert domain info into queue
            bool result = false;
            int retrycount = maxretries;
            while (true)
            {
                // Try the insertion
                if (EnqueueMessage (msgbody))
                {
                    result = true;
                    break;
                }

                // Retry
                retrycount--;
                if (retrycount <= 0)
                    break;
                Thread.Sleep (new Random().Next(1000, 2000));
            }

            // Return
            return result;
        }

        public bool AnyMessageReceived ()
        {
            try
            {
                if (rcvMessageResponse == null)
                    return false;

                var messageResults = rcvMessageResponse.Messages;

                if (messageResults != null && messageResults.FirstOrDefault () != null)
                {
                    return true;
                }
            }
            catch
            {
                // Nothing to do here                
            }

            return false;
        }

        public void ClearQueue ()
        {
            do
            {
                // Dequeueing Messages
                if (!DeQueueMessages ())
                {
                    // Checking for the need to abort (queue error)
                    if (!String.IsNullOrWhiteSpace (ErrorMessage))
                    {
                        return; // Abort
                    }

                    continue; // Continue in case de dequeue fails, to make sure no message will be kept in the queue
                }

                // Retrieving Message Results
                var resultMessages = rcvMessageResponse.Messages;

                // Checking for no message dequeued
                if (resultMessages.Count == 0)
                {
                    break; // Breaks loop
                }

                // Iterating over messages of the result to remove it
                foreach (Message message in resultMessages)
                {
                    // Deleting Message from Queue
                    DeleteMessage (message);
                }

            } while (true);
        }

        public void PurgeQueue ()
        {
            queue.PurgeQueue (new PurgeQueueRequest
            {
                QueueUrl = queueurl.QueueUrl
            });
        }

        public void ClearQueues (List<String> queueNames, String AWSAccessKey, String AWSSecretKey)
        {
            // Iterating over queues
            foreach (string queueName in queueNames)
            {
                OpenQueue (queueName, 10, AWSAccessKey, AWSSecretKey);

                do
                {
                    // Dequeueing Messages
                    if (!DeQueueMessages ())
                    {
                        continue; // Continue in case de dequeue fails, to make sure no message will be kept in the queue
                    }

                    // Retrieving Message Results
                    var resultMessages = rcvMessageResponse.Messages;

                    // Checking for no message dequeued
                    if (resultMessages.Count == 0)
                    {
                        break;
                    }

                    // Iterating over messages of the result to remove it
                    foreach (Message message in resultMessages)
                    {
                        // Deleting Message from Queue
                        DeleteMessage (message);
                    }

                } while (true);
            }
        }

        public IEnumerable<Message> GetDequeuedMessages ()
        {
            //return new List<Message> () { new Message () { Body = "http://www.amazon.co.uk/Pacific-Rim-DVD-UV-Copy/dp/B00A6UHBRO/ref=sr_1_1?ie=UTF8&qid=1429189548&sr=8-1&keywords=pacific+rim+dvd" } };
            return rcvMessageResponse.Messages;
        }
    }
}
