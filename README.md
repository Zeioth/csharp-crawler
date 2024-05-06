# csharp-crawler
This C# project is a multi thread and multi process C# solution for crawling and sraping apps from Google Playstore. Formerly used by the startup GameMetrics to generate real time report for big data analytics, and crossing data from multiple sources.

## Projects in the solution

* **PlayStoreCrawler**: Responsible for indexing all pages in Google play. This information is stored in our database and it is meant to be used by the PlayStoreWorker.
* **PlayStoreWorker**: Responsible for creating a queue of apps to scrape. This queue is shared across all spawned PlayStoreWorker clients. Then it GET the data of every app page and parse the app fields (name, description, publishing date...). This project is the core part of the solution.
* **DailyUpdater**: Responsible for updating app information that can change often (n of downloads, n of reviews...). It can parse much faster than the PlayStoreWorker, or the FullDataWorker. It's meant to run permanenty to produce real time reports.
* **FullDataWorker**: Responsible for updating all fields of every app periodically. Running this client weekly is enough.
* **ReviewParser**: Responsible for login as a registered user and scrape the reviews of every app using selenium.
