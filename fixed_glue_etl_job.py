import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

class YogyakartaTourismETL:
    def __init__(self, glue_context, spark_session):
        self.glueContext = glue_context
        self.spark = spark_session
        
        # Configuration - Fixed output prefix
        self.database_name = "yogyakarta_tourism_db"
        self.source_table = "yogya_tourism_raw_json"
        self.output_bucket = "rdv-apify-storage"
        self.output_prefix = "processed"  # Fixed from "processed-data" to "processed"
        
    def read_source_data(self):
        """
        Read data from Glue Data Catalog
        """
        print("Reading source data from Glue Data Catalog...")
        
        # Create dynamic frame from catalog
        dynamic_frame = self.glueContext.create_dynamic_frame.from_catalog(
            database=self.database_name,
            table_name=self.source_table,
            transformation_ctx="source_data"
        )
        
        # Convert to Spark DataFrame for easier manipulation
        df = dynamic_frame.toDF()
        
        print(f"Source data loaded. Total records: {df.count()}")
        print("DataFrame schema:")
        df.printSchema()
        return df, dynamic_frame
    
    def identify_data_sources(self, df):
        """
        Identify different data sources within the array structure
        """
        print("Identifying data sources...")
        
        # Check if 'array' column exists, if not check column structure
        if "array" not in df.columns:
            print("Available columns:", df.columns)
            # Based on the schema, the data seems to be already flattened
            # Let's work with the existing structure
            exploded_df = df.withColumn("row_id", monotonically_increasing_id())
        else:
            # Explode the array to work with individual records
            exploded_df = df.select(explode(col("array")).alias("record"))
            # Add row numbers for tracking
            exploded_df = exploded_df.withColumn("row_id", monotonically_increasing_id())
            # Flatten the record structure
            exploded_df = exploded_df.select(col("row_id"), col("record.*"))
        
        # Identify record types based on available fields
        identified_df = exploded_df.withColumn(
            "data_source_type",
            # Identify booking hotels
            when(col("hotelId").isNotNull() & 
                 col("facilities").isNotNull() & 
                 col("type").isNotNull() & 
                 col("reviewTitle").isNull(), "booking_hotel")
            # Identify booking reviews
            .when(col("hotelId").isNotNull() & 
                  col("hotelRatingScores").isNotNull() & 
                  col("reviewTitle").isNotNull(), "booking_review")
            # Identify tripadvisor hotels
            .when(col("locationId").isNotNull() & 
                  col("amenities").isNotNull() & 
                  col("latitude").isNotNull() & 
                  col("text").isNull(), "tripadvisor_hotel")
            # Identify tripadvisor reviews
            .when(col("locationId").isNotNull() & 
                  col("text").isNotNull() & 
                  col("publishedDate").isNotNull(), "tripadvisor_review")
            # Identify geospatial attractions
            .when(col("categoryName").isNotNull() & 
                  col("additionalInfo").isNotNull() & 
                  col("placeId").isNotNull(), "geospatial_attraction")
            .otherwise("unknown")
        )
        
        # Show distribution of data types
        type_counts = identified_df.groupBy("data_source_type").count().collect()
        print("Data source distribution:")
        for row in type_counts:
            print(f"  {row['data_source_type']}: {row['count']} records")
        
        return identified_df
    
    def transform_booking_hotels(self, df):
        """
        Transform Booking.com hotel data
        """
        print("Transforming Booking.com hotel data...")
        
        booking_hotels = df.filter(col("data_source_type") == "booking_hotel")
        
        if booking_hotels.count() == 0:
            print("No booking hotel data found")
            return None
        
        # Extract and flatten key fields with proper null handling
        transformed = booking_hotels.select(
            col("row_id").alias("source_row_id"),
            col("hotelId").alias("booking_hotel_id"),
            col("name").alias("hotel_name"),
            col("type").alias("accommodation_type"),
            col("description"),
            when(col("stars").isNotNull(), col("stars").cast("integer")).alias("stars"),
            col("price"),
            col("currency"),
            # Fixed: Handle complex rating structure
            when(col("rating").isNotNull(), 
                 coalesce(col("rating.double"), col("rating").cast("double"))).alias("rating"),
            when(col("reviews").isNotNull(), col("reviews").cast("integer")).alias("reviews"),
            
            # Location fields - handle nested structure properly
            when(col("location.lat").isNotNull(), col("location.lat").cast("double")).alias("latitude"),
            when(col("location.lng").isNotNull(), col("location.lng").cast("double")).alias("longitude"),
            col("address").alias("full_address"),
            col("street").alias("street_address"),
            col("countryCode").alias("country"),
            col("state").alias("region"),
            col("postalCode").alias("postal_code"),
            
            # Operational info
            col("breakfast"),
            col("checkIn").alias("check_in_time"),
            col("checkOut").alias("check_out_time"),
            col("url").alias("booking_url"),
            
            # Facilities (will be processed separately)
            col("facilities"),
            
            # Metadata
            lit("booking.com").alias("platform"),
            current_timestamp().alias("processed_at")
        )
        
        print(f"Transformed {transformed.count()} booking hotel records")
        return transformed
    
    def transform_booking_reviews(self, df):
        """
        Transform Booking.com review data
        """
        print("Transforming Booking.com review data...")
        
        booking_reviews = df.filter(col("data_source_type") == "booking_review")
        
        if booking_reviews.count() == 0:
            print("No booking review data found")
            return None
        
        transformed = booking_reviews.select(
            col("row_id").alias("source_row_id"),
            col("id").alias("review_id"),
            col("hotelId").alias("booking_hotel_id"),
            # Fixed: Handle complex rating structure
            when(col("rating").isNotNull(), 
                 coalesce(col("rating.double"), col("rating").cast("double"))).alias("rating"),
            col("reviewTitle").alias("review_title"),
            col("likedText").alias("liked_text"),
            col("dislikedText").alias("disliked_text"),
            col("travelerType").alias("traveler_type"),
            col("userLocation").alias("user_location"),
            col("userName").alias("user_name"),
            when(col("numberOfNights").isNotNull(), col("numberOfNights").cast("integer")).alias("number_of_nights"),
            col("roomInfo").alias("room_info"),
            when(col("helpfulVotes").isNotNull(), col("helpfulVotes").cast("integer")).alias("helpful_votes"),
            col("reviewLanguage").alias("review_language"),
            
            # Date fields - handle various date formats
            when(col("checkInDate").isNotNull(), 
                 to_date(col("checkInDate"), "yyyy-MM-dd")).alias("check_in_date"),
            when(col("checkOutDate").isNotNull(), 
                 to_date(col("checkOutDate"), "yyyy-MM-dd")).alias("check_out_date"),
            when(col("reviewDate").isNotNull(), 
                 to_date(col("reviewDate"), "yyyy-MM-dd")).alias("review_date"),
            
            # Rating scores (will be processed separately)
            col("hotelRatingScores").alias("category_ratings"),
            
            # Metadata
            lit("booking.com").alias("platform"),
            current_timestamp().alias("processed_at")
        )
        
        print(f"Transformed {transformed.count()} booking review records")
        return transformed
    
    def transform_tripadvisor_hotels(self, df):
        """
        Transform TripAdvisor hotel data
        """
        print("Transforming TripAdvisor hotel data...")
        
        ta_hotels = df.filter(col("data_source_type") == "tripadvisor_hotel")
        
        if ta_hotels.count() == 0:
            print("No TripAdvisor hotel data found")
            return None
        
        transformed = ta_hotels.select(
            col("row_id").alias("source_row_id"),
            col("locationId").alias("tripadvisor_location_id"),
            col("name").alias("hotel_name"),
            col("category").alias("accommodation_type"),
            col("description"),
            # Fixed: Handle complex rating structure
            when(col("rating").isNotNull(), 
                 coalesce(col("rating.double"), col("rating").cast("double"))).alias("rating"),
            when(col("numberOfReviews").isNotNull(), col("numberOfReviews").cast("integer")).alias("reviews_count"),
            col("hotelClass").alias("hotel_class"),
            
            # Location fields
            when(col("latitude").isNotNull(), col("latitude").cast("double")).alias("latitude"),
            when(col("longitude").isNotNull(), col("longitude").cast("double")).alias("longitude"),
            col("address").alias("full_address"),
            col("street").alias("street_address"),
            col("city"),
            col("state"),
            col("countryCode").alias("country"),
            col("postalCode").alias("postal_code"),
            
            # Contact info
            col("phone"),
            col("email"),
            col("website"),
            
            # Ranking and pricing
            when(col("rankingPosition").isNotNull(), col("rankingPosition").cast("integer")).alias("ranking_position"),
            when(col("rankingDenominator").isNotNull(), col("rankingDenominator").cast("integer")).alias("ranking_denominator"),
            col("priceLevel").alias("price_level"),
            col("priceRange").alias("price_range"),
            
            # Additional info
            when(col("photoCount").isNotNull(), col("photoCount").cast("integer")).alias("photo_count"),
            col("amenities"),
            
            # Metadata
            lit("tripadvisor.com").alias("platform"),
            current_timestamp().alias("processed_at")
        )
        
        print(f"Transformed {transformed.count()} TripAdvisor hotel records")
        return transformed
    
    def transform_tripadvisor_reviews(self, df):
        """
        Transform TripAdvisor review data
        """
        print("Transforming TripAdvisor review data...")
        
        ta_reviews = df.filter(col("data_source_type") == "tripadvisor_review")
        
        if ta_reviews.count() == 0:
            print("No TripAdvisor review data found")
            return None
        
        transformed = ta_reviews.select(
            col("row_id").alias("source_row_id"),
            col("id").alias("review_id"),
            col("locationId").alias("tripadvisor_location_id"),
            # Fixed: Handle complex rating structure
            when(col("rating").isNotNull(), 
                 coalesce(col("rating.double"), col("rating").cast("double"))).alias("rating"),
            col("title").alias("review_title"),
            col("text").alias("review_text"),
            col("lang").alias("review_language"),
            col("tripType").alias("trip_type"),
            
            # User info - handle nested user structure
            col("user.name").alias("user_name"),
            col("user.userLocation.name").alias("user_location"),
            when(col("user.contributions.totalContributions").isNotNull(), 
                 col("user.contributions.totalContributions").cast("integer")).alias("user_total_contributions"),
            
            # Date info - handle various date formats
            when(col("publishedDate").isNotNull(), 
                 to_date(col("publishedDate"), "yyyy-MM-dd")).alias("published_date"),
            col("travelDate").alias("travel_date"),
            
            # Photos - handle array size
            when(col("photos").isNotNull(), size(col("photos"))).otherwise(lit(0)).alias("photos_count"),
            
            # Metadata
            lit("tripadvisor.com").alias("platform"),
            current_timestamp().alias("processed_at")
        )
        
        print(f"Transformed {transformed.count()} TripAdvisor review records")
        return transformed
    
    def transform_geospatial_attractions(self, df):
        """
        Transform geospatial attraction data
        """
        print("Transforming geospatial attraction data...")
        
        geo_attractions = df.filter(col("data_source_type") == "geospatial_attraction")
        
        if geo_attractions.count() == 0:
            print("No geospatial attraction data found")
            return None
        
        transformed = geo_attractions.select(
            col("row_id").alias("source_row_id"),
            col("placeId").alias("place_id"),
            col("title").alias("attraction_name"),
            col("categoryName").alias("category_name"),
            when(col("totalScore").isNotNull(), col("totalScore").cast("double")).alias("rating"),
            when(col("reviewsCount").isNotNull(), col("reviewsCount").cast("integer")).alias("reviews_count"),
            when(col("imagesCount").isNotNull(), col("imagesCount").cast("integer")).alias("images_count"),
            
            # Location info - handle various coordinate formats
            when(col("latitude").isNotNull(), col("latitude").cast("double"))
                .when(col("location.lat").isNotNull(), col("location.lat").cast("double")).alias("latitude"),
            when(col("longitude").isNotNull(), col("longitude").cast("double"))
                .when(col("location.lng").isNotNull(), col("location.lng").cast("double")).alias("longitude"),
            col("address").alias("full_address"),
            col("neighborhood"),
            col("city"),
            col("state"),
            col("postalCode").alias("postal_code"),
            
            # Contact info
            col("phone"),
            col("phoneUnformatted").alias("phone_unformatted"),
            
            # Status
            when(col("permanentlyClosed").isNotNull(), col("permanentlyClosed").cast("boolean")).alias("permanently_closed"),
            when(col("temporarilyClosed").isNotNull(), col("temporarilyClosed").cast("boolean")).alias("temporarily_closed"),
            
            # Additional structured info
            col("additionalInfo"),
            col("openingHours").alias("opening_hours"),
            col("categories"),
            
            # Metadata
            lit("google_maps").alias("platform"),
            current_timestamp().alias("processed_at")
        )
        
        print(f"Transformed {transformed.count()} geospatial attraction records")
        return transformed
    
    def calculate_distances(self, hotels_df, attractions_df):
        """
        Calculate distances between hotels and attractions using Haversine formula
        """
        print("Calculating distances between hotels and attractions...")
        
        if attractions_df is None:
            print("Missing attraction data for distance calculation")
            return None
        
        # Combine all hotel data sources
        all_hotels = None
        if isinstance(hotels_df, list):
            valid_hotels = [df for df in hotels_df if df is not None]
            if valid_hotels:
                # Standardize columns for union
                standardized_hotels = []
                for hotel_df in valid_hotels:
                    if hotel_df is not None:
                        standardized = hotel_df.select(
                            col("hotel_name"),
                            col("latitude"),
                            col("longitude"),
                            col("platform")
                        ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())
                        standardized_hotels.append(standardized)
                
                if standardized_hotels:
                    all_hotels = standardized_hotels[0]
                    for df in standardized_hotels[1:]:
                        all_hotels = all_hotels.union(df)
        elif hotels_df is not None:
            all_hotels = hotels_df.select(
                col("hotel_name"),
                col("latitude"),
                col("longitude"),
                col("platform")
            ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        
        if all_hotels is None or all_hotels.count() == 0:
            print("No valid hotel location data found")
            return None
        
        # Select relevant attraction data
        attractions = attractions_df.select(
            col("attraction_name"),
            col("latitude").alias("attr_latitude"),
            col("longitude").alias("attr_longitude"),
            col("category_name")
        ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        
        if attractions.count() == 0:
            print("No valid attraction location data found")
            return None
        
        # Cross join to calculate all distances
        cross_df = all_hotels.crossJoin(attractions)
        
        # Define Haversine distance calculation UDF
        def haversine_distance(lat1, lon1, lat2, lon2):
            if any(x is None for x in [lat1, lon1, lat2, lon2]):
                return None
            
            try:
                # Convert to radians
                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                
                # Haversine formula
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                c = 2 * math.asin(math.sqrt(a))
                r = 6371  # Earth's radius in kilometers
                
                return r * c
            except:
                return None
        
        # Register UDF
        haversine_udf = udf(haversine_distance, DoubleType())
        
        # Calculate distances
        distances_df = cross_df.withColumn(
            "distance_km",
            haversine_udf(col("latitude"), col("longitude"), 
                         col("attr_latitude"), col("attr_longitude"))
        )
        
        # Filter for nearby attractions (within 10km)
        nearby_attractions = distances_df.filter(col("distance_km") <= 10.0)
        
        print(f"Calculated {nearby_attractions.count()} hotel-attraction distance pairs")
        return nearby_attractions
    
    def create_summary_statistics(self, transformed_data):
        """
        Create summary statistics for the transformed data
        """
        print("Creating summary statistics...")
        
        stats = {}
        
        for data_type, df in transformed_data.items():
            if df is not None:
                count = df.count()
                stats[data_type] = {
                    'record_count': count,
                    'columns': len(df.columns)
                }
                
                # Add specific stats based on data type
                if 'rating' in df.columns:
                    try:
                        rating_stats = df.select(
                            avg(col("rating")).alias("avg_rating"),
                            min(col("rating")).alias("min_rating"),
                            max(col("rating")).alias("max_rating")
                        ).collect()[0]
                        
                        stats[data_type].update({
                            'avg_rating': rating_stats['avg_rating'],
                            'min_rating': rating_stats['min_rating'],
                            'max_rating': rating_stats['max_rating']
                        })
                    except Exception as e:
                        print(f"Warning: Could not calculate rating stats for {data_type}: {e}")
        
        print("Summary Statistics:")
        for data_type, stat in stats.items():
            print(f"  {data_type}:")
            for key, value in stat.items():
                print(f"    {key}: {value}")
        
        return stats
    
    def save_transformed_data(self, transformed_data):
        """
        Save transformed data to S3 in parquet format
        """
        print("Saving transformed data to S3...")
        
        for data_type, df in transformed_data.items():
            if df is not None:
                output_path = f"s3://{self.output_bucket}/{self.output_prefix}/{data_type}/"
                
                print(f"Saving {data_type} to {output_path}")
                
                try:
                    # Convert back to Dynamic Frame for optimized writing
                    dynamic_frame = DynamicFrame.fromDF(df, self.glueContext, f"{data_type}_frame")
                    
                    # Write to S3 in Parquet format
                    self.glueContext.write_dynamic_frame.from_options(
                        frame=dynamic_frame,
                        connection_type="s3",
                        connection_options={
                            "path": output_path,
                            "partitionKeys": ["platform"] if "platform" in df.columns else []
                        },
                        format="parquet",
                        transformation_ctx=f"write_{data_type}"
                    )
                    
                    print(f"Successfully saved {data_type}")
                except Exception as e:
                    print(f"Error saving {data_type}: {e}")
                    # Try saving without partitioning as fallback
                    try:
                        self.glueContext.write_dynamic_frame.from_options(
                            frame=dynamic_frame,
                            connection_type="s3",
                            connection_options={"path": output_path},
                            format="parquet",
                            transformation_ctx=f"write_{data_type}_fallback"
                        )
                        print(f"Successfully saved {data_type} (without partitioning)")
                    except Exception as e2:
                        print(f"Failed to save {data_type}: {e2}")
        
        print("Data saving process completed!")
    
    def run_etl_pipeline(self):
        """
        Run the complete ETL pipeline
        """
        print("Starting Yogyakarta Tourism ETL Pipeline...")
        print("=" * 60)
        
        try:
            # Step 1: Read source data
            source_df, source_dynamic_frame = self.read_source_data()
            
            # Step 2: Identify data sources
            identified_df = self.identify_data_sources(source_df)
            
            # Step 3: Transform each data source
            transformed_data = {}
            
            transformed_data['booking_hotels'] = self.transform_booking_hotels(identified_df)
            transformed_data['booking_reviews'] = self.transform_booking_reviews(identified_df)
            transformed_data['tripadvisor_hotels'] = self.transform_tripadvisor_hotels(identified_df)
            transformed_data['tripadvisor_reviews'] = self.transform_tripadvisor_reviews(identified_df)
            transformed_data['geospatial_attractions'] = self.transform_geospatial_attractions(identified_df)
            
            # Step 4: Calculate distances between hotels and attractions
            hotel_dataframes = [transformed_data['booking_hotels'], transformed_data['tripadvisor_hotels']]
            distances_df = self.calculate_distances(hotel_dataframes, transformed_data['geospatial_attractions'])
            if distances_df is not None:
                transformed_data['hotel_attraction_distances'] = distances_df
            
            # Step 5: Create summary statistics
            stats = self.create_summary_statistics(transformed_data)
            
            # Step 6: Save transformed data
            self.save_transformed_data(transformed_data)
            
            print("\n" + "=" * 60)
            print("ETL Pipeline completed successfully!")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"Error in ETL pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            raise e

# Main execution
if __name__ == "__main__":
    # Initialize ETL processor
    etl_processor = YogyakartaTourismETL(glueContext, spark)
    
    # Run ETL pipeline
    success = etl_processor.run_etl_pipeline()
    
    if success:
        print("\nETL job completed successfully!")
        print(f"Transformed data available in S3 bucket: {etl_processor.output_bucket}/{etl_processor.output_prefix}/")
    else:
        print("\nETL job failed!")

# Commit the job
job.commit()