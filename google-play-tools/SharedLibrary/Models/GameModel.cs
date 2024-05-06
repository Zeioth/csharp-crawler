using System;
using System.Linq;
using MongoDB.Bson;

namespace SharedLibrary.Models
{
    // Serialization Models for App Data to be stored on MongoDB
    public class GameModel:AppModel
    {
        public BsonDateTime   Updated                {get;set;} 

        // Override of the ToString Method
        public string ToString()
        {
            // Serializing Object as CSV
            return String.Join (",", 
                                ("\"" + Url                    + "\""),
                                ("\"" + ReferenceDate.ToString ("yyyy-MM-dd").Replace (",","") + "\""),
                                ("\"" + Name.Replace (",", "") + "\" "),
                                ("\"" + Developer.Replace (",", "") + "\""),
                                ("\"" + IsTopDeveloper         + "\""),
                                ("\"" + DeveloperURL.Replace (",", "") + "\""),
                                ("\"" + PublicationDate.ToString ("yyyy-MM-dd").Replace (",", "") + "\""),
                                ("\"" + Category.Replace (",", "") + "\""),
                                ("\"" + IsFree                 + "\""),
                                ("\"" + Price                  + "\""), 
                                ("\"" + Reviewers              + "\""),
                                ("\"" + Score.Total            + "\""), 
                                ("\"" + Score.Count            + "\""), 
                                ("\"" + Score.FiveStars        + "\""),
                                ("\"" + Score.FourStars        + "\""),
                                ("\"" + Score.ThreeStars       + "\""),
                                ("\"" + Score.TwoStars         + "\""),
                                ("\"" + Score.OneStars         + "\""),
                                ("\"" + LastUpdateDate.ToString ("yyyy-MM-dd") + "\""),
                                ("\"" + AppSize                + "\""),
                                ("\"" + Instalations.Replace (",", ".") + "\""),
                                ("\"" + CurrentVersion.Replace (",", "") + "\""),
                                ("\"" + MinimumOSVersion.Replace (",", "") + "\""),
                                ("\"" + ContentRating.Replace (",", "") + "\""),
                                ("\"" + HaveInAppPurchases     +"\""),
                                ("\"" + DeveloperEmail.Replace (",", "") + "\""),
                                ("\"" + DeveloperWebsite.Replace (",", "") + "\""),
                                ("\"" + DeveloperPrivacyPolicy.Replace (",", "") + "\""));
        }
    }
}
