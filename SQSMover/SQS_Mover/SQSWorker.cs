using Amazon.SQS.Model;
using NLog;
using SQS_Mover.SimpleHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQS_Mover
{
    public class SQSWorker
    {
        private ManualResetEvent _doneEvent;
        private FlexibleOptions _programOptions;
        private Logger _logger;
        private String _id;

        /// <summary>
        /// Class Constructor
        /// </summary>
        public SQSWorker (ManualResetEvent doneEvent, FlexibleOptions options)
        {
            _doneEvent = doneEvent;
            _programOptions = options;

            // Logger
            _logger = LogManager.GetCurrentClassLogger ();
        }

        /// <summary>
        /// Does all the heavy lifting of Dequeueing messages and queueing it to be
        /// recorded on mongodb.
        /// </summary>
        /// <param name="threadContext"></param>
        public void ThreadPoolCallback (Object threadContext)
        {
            // Thread ID
            _id = Convert.ToString ((int)threadContext);

            // Control Variables
            int logThreshold       = _programOptions.Get<int>("LogThreshold", 10000);
            int messagesPerDequeue = _programOptions.Get<int>("MessagesPerDequeue", 10);
            int movedMessages      = 0;

            _logger.Info ("Started Worker : " + _id);

            AWSSQSHelper sourceQueueHandler = new AWSSQSHelper (_programOptions["SourceQueue"], messagesPerDequeue, _programOptions["AWSAccessKey"], _programOptions["AWSSecretKey"]);
            AWSSQSHelper targetQueueHandler = new AWSSQSHelper (_programOptions["TargetQueue"], messagesPerDequeue, _programOptions["AWSAccessKey"], _programOptions["AWSSecretKey"]);

            // Local Buffer of SQS Messages
            List<Message> sqsBuffer = new List<Message> ();

            // Iterating over Dequeued Messages
            foreach (var sqsMessage in sourceQueueHandler.GetMessages ())
            {   
                // Feeding local buffer of messages to be deleted
                sqsBuffer.Add (sqsMessage);

                // Incrementing counter of moved messages
                movedMessages++;

                // Enqueueing and Deleting messages every 1000
                if (sqsBuffer.Count == 1000)
                {
                    _logger.Info (String.Format ("Enqueueing Messages to target - Worker {0}", _id));

                    targetQueueHandler.EnqueueMessages (sqsBuffer.Select (t => t.Body).ToList ());

                    _logger.Info (String.Format ("Deleting Messages from source - Worker {0}", _id));

                    sourceQueueHandler.DeleteMessages (sqsBuffer);
                    sqsBuffer.Clear ();
                }

                // Checking for the need to log progress
                if (movedMessages % logThreshold == 0)
                {
                    _logger.Info (String.Format("Moved {0} messages - Thread {1}", movedMessages, _id));
                }
            }

            // Enqueuing and Removing Remainder of buffer
            _logger.Info ("Enqueueing remainder of buffered messages to target SQS Queue");
            targetQueueHandler.EnqueueMessages (sqsBuffer.Select (t => t.Body).ToList ());

            _logger.Info ("Deleting remainder of buffered messages from source SQS Queue");
            sourceQueueHandler.DeleteMessages (sqsBuffer);
            sqsBuffer.Clear ();

            // Finished Processing
            _logger.Info (String.Format ("Finished Moving SQS Messages. Halting {0}", _id));

            // Setting up "Done Event" to signal Thread Pool
            _doneEvent.Set ();
        }
    }
}
