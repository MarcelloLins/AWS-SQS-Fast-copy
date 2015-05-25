using NLog;
using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using SQS_Mover.SimpleHelpers;
using System.Collections.Generic;
using Amazon.SQS.Model;

namespace SQS_Mover
{
    class Program
    {
        public static FlexibleOptions ProgramOptions { get; private set; }

        /// <summary>
        /// Main program entry point.
        /// </summary>
        static void Main (string[] args)
        {
            // set error exit code
            System.Environment.ExitCode = -50;
            try
            {
                // load configurations
                ProgramOptions = ConsoleUtils.Initialize (args, true);           
                    
                // start execution
                Execute (ProgramOptions);

                // check before ending for waitForKeyBeforeExit option
                if (ProgramOptions.Get ("waitForKeyBeforeExit", false))
                    ConsoleUtils.WaitForAnyKey ();
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger ().Fatal (ex);

                // check before ending for waitForKeyBeforeExit option
                if (ProgramOptions.Get ("waitForKeyBeforeExit", false))
                    ConsoleUtils.WaitForAnyKey ();

                ConsoleUtils.CloseApplication (-60, true);
            }
            // set success exit code
            ConsoleUtils.CloseApplication (0, false);
        }
        
        static Logger logger = LogManager.GetCurrentClassLogger ();
        static DateTime started = DateTime.UtcNow;

        private static void Execute (FlexibleOptions options)
        {
            logger.Info ("Start");
            
            // Setting up Threading Environment
            int threadsCount = ProgramOptions.Get<int> ("Threads", Environment.ProcessorCount * 2);

            // Executing Multithreaded dequeueing
            ManualResetEvent[] doneEvents = new ManualResetEvent[threadsCount];

            for (int threadIndex = 0; threadIndex < threadsCount; threadIndex++)
            {
                // Done = False (Thread isn't done doing it's work)
                doneEvents[threadIndex] = new ManualResetEvent (false);

                // Setting up SQS Worker
                SQSWorker worker = new SQSWorker (doneEvents[threadIndex], options);

                // Setting up Thread into ThreadPool
                ThreadPool.QueueUserWorkItem (worker.ThreadPoolCallback, threadIndex);
            }

            // Wait for all threads in pool to finish processing SQS
            WaitHandle.WaitAll (doneEvents);
            logger.Info ("All Threads Halted");
            logger.Info ("All Messages Moved");
        }
    }
}