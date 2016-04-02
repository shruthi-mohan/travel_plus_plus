from pyspark import SparkConf, SparkContext, SQLContext
import yelp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import Row
conf = SparkConf().setAppName('yelp')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

MY_CONSUMER_KEY=my_consumer_key
MY_CONSUMER_SECRET=my_consumer_secret
MY_ACCESS_TOKEN=my_access_token_key
MY_ACCESS_SECRET=my_access_token_secret

input = "/user/shruthim/cities_list.txt"

yelp_api = yelp.Api(consumer_key=MY_CONSUMER_KEY,
                    consumer_secret=MY_CONSUMER_SECRET,
                    access_token_key=MY_ACCESS_TOKEN,
                    access_token_secret=MY_ACCESS_SECRET)


def extract_restaurant(location_name):
                location_name_t = location_name.encode('utf-8').strip()
                print "Extracting restaurant details in location: "+location_name_t
                results=[]
                try:
                        search_results = yelp_api.Search(term="Nightlife", location=location_name_t)
                        for business in search_results.businesses:
                                results.append(Row(name=business.name.encode('utf-8').strip(),
                                                                phone=str(business.phone),
                              address=business.location.display_address[0].encode('utf-8').strip()+' '+business.location.display_address[1].encode('utf-8').strip(),
                                                          city=business.location.city.encode('utf-8').strip(),
                              latitude=business.location.coordinate.get("latitude"),
                              longitude=business.location.coordinate.get("longitude")
                          ))
                        return results

                except:
                        results.append(Row(name="xxxx", phone=" ", address=" ", city=" ", latitude=0, longitude=0))
                        return results


data = sc.textFile(input)

city_details = data.map(extract_restaurant).flatMap(lambda x:x).coalesce(1)
city_details_df = city_details.toDF()
city_details_df_t = city_details_df.filter(city_details_df['name']!="xxxx")
city_details_df_t.show()
filename = '/user/shruthim/Restaurants/'
city_details_df_t.save(filename, "com.databricks.spark.csv")

