﻿using System;
using System.Linq;
using System.Threading;
using SharedLibrary;
using SharedLibrary.Models;
using SharedLibrary.MongoDB;
using WebUtilsLib;
using System.Diagnostics;
using System.Collections.Generic;
using NLog;
using System.IO;
using SharedLibrary.Proxies;
using System.Text;

namespace PlayStoreWorker
{
    class Daily
    {
        /// <summary>
        /// Entry point of the worker piece of the process
        /// Notice that you can run as many workers as you want to in order to make the crawling faster
        /// </summary>
        /// <param name="args"></param>
        static void Main (string[] args)
        {
            // Configuring Log Object
            Logger logger = LogManager.GetCurrentClassLogger ();
            logger.Info ("Worker Started");

            // Control Variable (Bool - Should the process use proxies? )
            bool isUsingProxies = false;

            // Checking for the need to use proxies
            if (args != null && args.Length == 1)
            {
                // Setting flag to true
                isUsingProxies = true;

                // Loading proxies from .txt received as argument
                String fPath = args[0];

                // Sanity Check
                if (!File.Exists (fPath))
                {
                    logger.Fatal ("Couldnt find proxies on path : " + fPath);
                    System.Environment.Exit (-100);
                }

                // Reading Proxies from File
                string[] fLines = File.ReadAllLines (fPath, Encoding.GetEncoding ("UTF-8"));

                try
                {
                    // Actual Load of Proxies
                    ProxiesLoader.Load (fLines.ToList ());
                }
                catch (Exception ex)
                {
                    logger.Fatal (ex);
                    System.Environment.Exit (-101);
                }
            }

            // Parser
            PlayStoreParser parser = new PlayStoreParser();

            // Configuring MongoDB Wrapper
            MongoDBWrapper mongoDB   = new MongoDBWrapper();
            string fullServerAddress = String.Join(":", Consts.MONGO_SERVER, Consts.MONGO_PORT);
            mongoDB.ConfigureDatabase(Consts.MONGO_USER, Consts.MONGO_PASS, Consts.MONGO_AUTH_DB, fullServerAddress, Consts.MONGO_TIMEOUT, Consts.MONGO_DATABASE, Consts.GAMES_COLLECTION);

            // Creating Instance of Web Requests Server
            WebRequests server = new WebRequests ();
            
            // Queued App Model
            QueuedApp app;

            // Populate GamesQueue
            int nQueuedGames = mongoDB.PopulateQueuedGames();
            Console.WriteLine(nQueuedGames + " Games added to QueuedGames.\n");

            // Retry Counter (Used for exponential wait increasing logic)
            int retryCounter = 0;

            // Iterating Over MongoDB Records while no document is found to be processed                
            while ((app = mongoDB.GetFromGamesQueue()) != null)
            {
                try
                {

                    // Building APP URL
                    string appUrl = app.Url;

                    // Sanity check of app page url
                    if (app.Url.IndexOf ("http", StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        appUrl = Consts.APP_URL_PREFIX + app.Url;
                    }

                    // Checking if this app is on the database already
                    if (mongoDB.GameIsUpdated (appUrl))
                    {
                        // Console Feedback, Comment this line to disable if you want to
                        Console.WriteLine("Game already updated today, skipped.");

                        // Delete it from the queue and continues the loop
                        mongoDB.RemoveFromQueue (app.Url);
                        continue;
                    }

                    // Configuring server and Issuing Request
                    server.Headers.Add (Consts.ACCEPT_LANGUAGE);
                    server.Host              = Consts.HOST;
                    server.UserAgent         = Consts.GITHUBURL;
                    server.Encoding          = "utf-8";
                    server.EncodingDetection = WebRequests.CharsetDetection.DefaultCharset;

                    // Checking for the need to use "HTTP Proxies"
                    if (isUsingProxies)
                    {
                        server.Proxy = ProxiesLoader.GetWebProxy ();
                    }

                    // Issuing HTTP Request
                    string response          = server.Get (appUrl);

                    // Flag Indicating Success while processing and parsing this app
                    bool ProcessingWorked = true;

                    // Sanity Check
                    if (String.IsNullOrEmpty (response) || server.StatusCode != System.Net.HttpStatusCode.OK)
                    {
                        logger.Info ("Error opening app page : " + appUrl);
                        ProcessingWorked = false;
                        
                        // Renewing WebRequest Object to get rid of Cookies
                        server = new WebRequests ();

                        // Fallback time variable
                        double waitTime;

                        // Checking which "Waiting Logic" to use - If there are proxies being used, there's no need to wait too much
                        // If there are no proxies in use, on the other hand, the process must wait more time
                        if (isUsingProxies)
                        {
                            // Waits two seconds everytime
                            waitTime = TimeSpan.FromSeconds (2).TotalMilliseconds;
                        }
                        else
                        {
                            // Increments retry counter
                            retryCounter++;

                            // Checking for maximum retry count
                            if (retryCounter >= 8)
                            {
                                waitTime = TimeSpan.FromMinutes (20).TotalMilliseconds;
                            }
                            else
                            {
                                // Calculating next wait time ( 2 ^ retryCounter seconds)
                                waitTime = TimeSpan.FromSeconds (Math.Pow (2, retryCounter)).TotalMilliseconds;
                            }
                        }

                        // Hiccup to avoid google blocking connections in case of heavy traffic from the same IP
                        Console.WriteLine ("======================================================");
                        Console.WriteLine ("\n\tFallback : " + waitTime + " Seconds");
                        Thread.Sleep (Convert.ToInt32 (waitTime));

                        // If The Status code is "ZERO" (it means 404) - App must be removed from "Queue"
                        if (server.StatusCode == 0)
                        {
                            // Console Feedback
                            Console.WriteLine ("\tApp Not Found (404) - " + app.Url);

                            mongoDB.RemoveFromQueue (app.Url);
                        }
                        Console.WriteLine ("======================================================");
                    }
                    else
                    {
                        // Reseting retry counter
                        retryCounter = 0;

                        // Parsing Useful App Data
                        AppModel parsedApp = parser.ParseAppPage (response, appUrl);

                        List<String> relatedApps = new List<String> ();

                        // Avoiding Exceptions caused by "No Related Apps" situations - Must be treated differently
                        try
                        {

                            // Parsing "Related Apps" and "More From Developer" Apps (URLS Only)
                            foreach (string extraAppUrl in parser.ParseExtraApps (response))
                            {
                                relatedApps.Add (Consts.APP_URL_PREFIX + extraAppUrl);
                            }

                            // Adding "Related Apps" to Apps Model
                            parsedApp.RelatedUrls = relatedApps.Distinct ().ToArray ();
                        }
                        catch
                        {
                            Console.WriteLine ("\tNo Related Apps Found. Skipping");
                        }

                        // Inserting App into Mongo DB Database
                        if (!mongoDB.Insert<AppModel>(parsedApp))
                        {
                            ProcessingWorked = false;
                        }

                        // If the processing failed, do not remove the app from the database, instead, keep it and flag it as not busy 
                        // so that other workers can try to process it later
                        if (!ProcessingWorked)
                        {
                            mongoDB.ToggleBusyApp(app, false);
                        }
                        else // On the other hand, if processing worked, removes it from the database
                        {
                            // Console Feedback, Comment this line to disable if you want to
                            Console.ForegroundColor = ConsoleColor.DarkGreen;
                            Console.WriteLine("Inserted Game : " + parsedApp.Name);
                            Console.ForegroundColor = ConsoleColor.White;

                            mongoDB.RemoveFromQueue(app.Url);
                        }

                    }
                }
                catch (Exception ex)
                {
                    logger.Error (ex);
                }
                finally
                {
                    try
                    {
                        // Toggles Busy status back to false
                        mongoDB.ToggleBusyApp(app, false);
                    }
                    catch (Exception ex)
                    {
                        // Toggle Busy App may raise an exception in case of lack of internet connection, so, i must use this
                        // "inner catch" to avoid it from happenning
                        logger.Error (ex);
                    }
                }
            }
        }
    }
}
