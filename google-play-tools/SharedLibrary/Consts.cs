﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharedLibrary
{
    public class Consts
    {
        // Web Request Parameters and URLs
        public static readonly string CRAWL_URL          = "https://play.google.com/store/search?q={0}&c=apps";
        public static readonly string REVIEWS_URL        = "https://play.google.com/store/getreviews";
        public static readonly string HOST               = "play.google.com";
        public static readonly string ORIGIN             = "https://play.google.com";
        public static readonly string REFERER            = "https://play.google.com/store/apps";
        public static readonly string INITIAL_POST_DATA  = "ipf=1&xhr=1";
        public static readonly string PLAY_STORE_PREFIX  = "https://play.google.com/store/apps/details?id=";
        public static readonly string GITHUBURL          = "";

        // Old Post Data - To Allow Rolling Back if Needed
        // public static readonly string POST_DATA          = "start={0}&num=48&numChildren=0&ipf=1&xhr=1";

        public static readonly string POST_DATA          = "start=0&num=0&numChildren=0&pagTok={0}&ipf=1&xhr=1";
        public static readonly string APP_URL_PREFIX     = "https://play.google.com";
        public static readonly string ACCEPT_LANGUAGE    = "Accept-Language: en-US,en;q=0.6,en;q=0.4,es;q=0.2";
        public static readonly string REVIEWS_POST_DATA  = "reviewType=0&pageNum={0}&id={1}&reviewSortOrder=2&xhr=1";
        public static readonly string USER_AGENT         = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36";

        // XPaths
        public static readonly string APP_LINKS          = "//div[@class='details']/a[@class='card-click-target' and @tabindex='-1' and @aria-hidden='true']";
        public static readonly string APP_NAME           = "//div[@class='info-container']/div[@class='document-title' and @itemprop='name']/div";
        public static readonly string APP_CATEGORY       = "//div/a[@class='document-subtitle category']";
        public static readonly string APP_DEV            = "//div[@class='info-container']/div[@itemprop='author']/a/span[@itemprop='name']";
        public static readonly string APP_TOP_DEV        = "//meta[@itemprop='topDeveloperBadgeUrl']";
        public static readonly string DEV_URL            = "//div[@class='info-container']/div[@itemprop='author']/meta[@itemprop='url']";
        public static readonly string APP_PUBLISH_DATE   = "//div[@class='info-container']/div[@itemprop='author']/div[@class='document-subtitle']";
        public static readonly string APP_FREE_PAID      = "//span[@itemprop='offers' and @itemtype='http://schema.org/Offer']/meta[@itemprop='price']";
        public static readonly string APP_REVIEWERS      = "//div[@class='header-star-badge']/div[@class='stars-count']";
        public static readonly string APP_DESCRIPTION    = "//div[@class='show-more-content text-body' and @itemprop='description']";
        public static readonly string APP_SCORE_VALUE    = "//div[@class='rating-box']/div[@class='score-container']/meta[@itemprop='ratingValue']";
        public static readonly string APP_SCORE_COUNT    = "//div[@class='rating-box']/div[@class='score-container']/meta[@itemprop='ratingCount']";
        public static readonly string APP_FIVE_STARS     = "//div[@class='rating-histogram']/div[@class='rating-bar-container five']/span[@class='bar-number']";
        public static readonly string APP_FOUR_STARS     = "//div[@class='rating-histogram']/div[@class='rating-bar-container four']/span[@class='bar-number']";
        public static readonly string APP_THREE_STARS    = "//div[@class='rating-histogram']/div[@class='rating-bar-container three']/span[@class='bar-number']";
        public static readonly string APP_TWO_STARS      = "//div[@class='rating-histogram']/div[@class='rating-bar-container two']/span[@class='bar-number']";
        public static readonly string APP_ONE_STARS      = "//div[@class='rating-histogram']/div[@class='rating-bar-container one']/span[@class='bar-number']";
        public static readonly string APP_COVER_IMG      = "//div[@class='details-info']/div[@class='cover-container']/img[@class='cover-image']";
        public static readonly string APP_UPDATE_DATE    = "//div[@class='meta-info']/div[@itemprop='datePublished']";
        public static readonly string APP_SIZE           = "//div[@class='meta-info']/div[@itemprop='fileSize']";
        public static readonly string APP_VERSION        = "//div[@class='content' and @itemprop='softwareVersion']";
        public static readonly string APP_INSTALLS       = "//div[@class='content' and @itemprop='numDownloads']";
        public static readonly string APP_CONTENT_RATING = "//div[@class='content' and @itemprop='contentRating']";
        public static readonly string APP_OS_REQUIRED    = "//div[@class='content' and @itemprop='operatingSystems']";
        public static readonly string EXTRA_APPS         = "//div[@class='card-content id-track-click id-track-impression']/a[@class='card-click-target']";
        public static readonly string IN_APP_PURCHASE    = "//div[@class='info-container']/div[@class='inapp-msg']";
        public static readonly string DEVELOPER_URLS     = "//div[@class='content contains-text-link']/a[@class='dev-link']";
        public static readonly string PHYSICAL_ADDRESS   = "//div[@class='content physical-address']";
        public static readonly string APP_SCREENSHOTS    = "//div[@class='thumbnails' ]//img[contains(@class,'screenshot')]";
        public static readonly string WHATS_NEW          = "//div[@class='recent-change']";

        // HTML Values
        public static readonly string NO_RESULT_MESSAGE = "Nenhum resultado para sua pesquisa"; // TODO: CHANGE THIS TO YOUR OWN LANGUAGE.
                                                                                                // THIS CONSTANT IS USED TO CHECK FOR "NO MORE APPS" WHEN YOU PAGINATE/SCROLL THROUGH
                                                                                                // THE SEARCH RESULTS. THE PHRASE MEANS "NO RESULT FOR YOUR SEARCH"

        // Retry Values
        public static readonly int MAX_REQUEST_ERRORS   = 100;
        public static readonly int MAX_QUEUE_TRIES      = 5;

        // AWS
        public static readonly string QUEUE_NAME        = "PlayStoreQueue";

        // MongoDB - Remote Server
        /*public static readonly string MONGO_SERVER         = "mobiledata.bigdatacorp.com.br";
        public static readonly string MONGO_PORT             = "21766";
        public static readonly string MONGO_USER             = "GitHubCrawlerUser";
        public static readonly string MONGO_PASS             = "g22LrJvULU5B";
        public static readonly string MONGO_DATABASE         = "MobileAppsData";
        public static readonly string MONGO_COLLECTION       = "ProcessedApps_2015_03";
        public static readonly string REVIEWS_COLLECTION     = "ProcessedReviews";
        public static readonly string QUEUED_APPS_COLLECTION = "QueuedApps_2015";
        public static readonly string REVIEWERS_COLLECTION   = "ReviewersData";
        public static readonly string MONGO_AUTH_DB          = "MobileAppsData";
        public static readonly int    MONGO_TIMEOUT          = 120000;*/

        // NOTE: You must manually create the database in mongodb. Collections will be created automatically.
        public static readonly string MONGO_SERVER           = "127.0.0.1";
        public static readonly string MONGO_PORT             = "27766";
        public static readonly string MONGO_USER             = "";
        public static readonly string MONGO_PASS             = "";
        public static readonly string MONGO_DATABASE         = "playstore";
        public static readonly string MONGO_COLLECTION       = "AppsData";
        public static readonly string GAMES_COLLECTION       = "GamesData";
        public static readonly string REVIEWS_COLLECTION     = "ReviewsData";
        public static readonly string QUEUED_APPS_COLLECTION = "QueuedApps";
        public static readonly string QUEUED_GAMES_COLLECTION = "QueuedGames";
        public static readonly string REVIEWERS_COLLECTION   = "ReviewersData";
        public static readonly string[] QUEUED_GAMES_CATEGORIES = new string[] { "/store/apps/category/GAME_ARCADE" };
        public static readonly string MONGO_AUTH_DB          = "";
        public static readonly int MONGO_TIMEOUT = 120000;

        // Date Time Format
        public static readonly string DATE_FORMAT       = "yyyy MMMM dd";
    }
}
