import boto3
import json
from botocore.exceptions import ClientError
import time

class YogyakartaTourismDataCrawler:
    def __init__(self, region_name='us-east-1'):
        """
        Initialize AWS clients for Glue service
        """
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.iam_client = boto3.client('iam', region_name=region_name)
        
        # Configuration
        self.bucket_name = 'rdv-apify-storage'
        self.database_name = 'yogyakarta_tourism_db'
        self.crawler_role_name = 'LabRole'
        self.crawler_name = 'yogyakarta-tourism-crawler'
        
        # S3 paths for different data sources
        self.s3_paths = {
            'booking_hotels': 's3://rdv-apify-storage/raw-json/booking - full hotel.json',
            'booking_reviews': 's3://rdv-apify-storage/raw-json/booking - full review of hotel.json',
            'tripadvisor_hotels': 's3://rdv-apify-storage/raw-json/tripadvisor - full hotel.json',
            'tripadvisor_reviews': 's3://rdv-apify-storage/raw-json/tripadvisor - full review of hotel.json',
            'geospatial_attractions': 's3://rdv-apify-storage/raw-json/geospatial tujuan wisata.json'
        }

    # def create_iam_role_for_crawler(self):
    #     """
    #     Create IAM role for Glue Crawler with necessary permissions
    #     """
    #     # Trust policy for Glue service
    #     trust_policy = {
    #         "Version": "2012-10-17",
    #         "Statement": [
    #             {
    #                 "Effect": "Allow",
    #                 "Principal": {
    #                     "Service": "glue.amazonaws.com"
    #                 },
    #                 "Action": "sts:AssumeRole"
    #             }
    #         ]
    #     }
        
    #     # IAM policy for S3 and Glue permissions
    #     crawler_policy = {
    #         "Version": "2012-10-17",
    #         "Statement": [
    #             {
    #                 "Effect": "Allow",
    #                 "Action": [
    #                     "s3:GetObject",
    #                     "s3:ListBucket",
    #                     "s3:GetBucketLocation"
    #                 ],
    #                 "Resource": [
    #                     f"arn:aws:s3:::{self.bucket_name}",
    #                     f"arn:aws:s3:::{self.bucket_name}/*"
    #                 ]
    #             },
    #             {
    #                 "Effect": "Allow",
    #                 "Action": [
    #                     "glue:*",
    #                     "logs:CreateLogGroup",
    #                     "logs:CreateLogStream",
    #                     "logs:PutLogEvents"
    #                 ],
    #                 "Resource": "*"
    #             }
    #         ]
    #     }
        
    #     try:
    #         # Create IAM role
    #         response = self.iam_client.create_role(
    #             RoleName=self.crawler_role_name,
    #             AssumeRolePolicyDocument=json.dumps(trust_policy),
    #             Description='IAM role for Yogyakarta Tourism Glue Crawler'
    #         )
            
    #         role_arn = response['Role']['Arn']
    #         print(f"Created IAM role: {role_arn}")
            
    #         # Attach inline policy
    #         self.iam_client.put_role_policy(
    #             RoleName=self.crawler_role_name,
    #             PolicyName='YogyakartaTourismCrawlerPolicy',
    #             PolicyDocument=json.dumps(crawler_policy)
    #         )
            
    #         print("Attached crawler policy to IAM role")
            
    #         # Wait for role to be ready
    #         time.sleep(10)
            
    #         return role_arn
            
    #     except ClientError as e:
    #         if e.response['Error']['Code'] == 'EntityAlreadyExists':
    #             # Role already exists, get its ARN
    #             response = self.iam_client.get_role(RoleName=self.crawler_role_name)
    #             role_arn = response['Role']['Arn']
    #             print(f"Using existing IAM role: {role_arn}")
    #             return role_arn
    #         else:
    #             print(f"Error creating IAM role: {e}")
    #             raise

    def create_glue_database(self):
        """
        Create Glue database for storing metadata
        """
        try:
            response = self.glue_client.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'Database for Yogyakarta Tourism accommodation prediction data pipeline'
                }
            )
            print(f"Created Glue database: {self.database_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Database {self.database_name} already exists")
            else:
                print(f"Error creating database: {e}")
                raise

    def verify_s3_data_structure(self):
        """
        Verify that S3 data structure matches expected schema
        """
        print("Verifying S3 data structure...")
        
        # Expected data structure based on provided samples
        expected_structures = {
            'booking_hotels': {
                'required_fields': ['name', 'type', 'stars', 'location', 'address', 'facilities', 'hotelId'],
                'location_fields': ['lat', 'lng'],
                'address_fields': ['full', 'street', 'country', 'region', 'postalCode'],
                'facilities_structure': 'nested_array_with_categories'
            },
            'booking_reviews': {
                'required_fields': ['rating', 'reviewTitle', 'travelerType', 'hotelRatingScores', 'hotelId'],
                'rating_categories': ['Staff', 'Facilities', 'Cleanliness', 'Comfort', 'Value for money', 'Location'],
                'temporal_fields': ['checkInDate', 'checkOutDate', 'reviewDate']
            },
            'tripadvisor_hotels': {
                'required_fields': ['name', 'type', 'category', 'rating', 'latitude', 'longitude', 'amenities'],
                'location_fields': ['latitude', 'longitude', 'address', 'addressObj'],
                'amenities_structure': 'simple_array'
            },
            'tripadvisor_reviews': {
                'required_fields': ['rating', 'text', 'title', 'locationId', 'publishedDate'],
                'user_structure': 'nested_object',
                'place_info_structure': 'nested_object'
            },
            'geospatial_attractions': {
                'required_fields': ['title', 'categoryName', 'location', 'totalScore', 'reviewsCount'],
                'location_fields': ['lat', 'lng'],
                'additional_info_structure': 'complex_nested_object'
            }
        }
        
        # Check if files exist in S3
        for data_type, s3_path in self.s3_paths.items():
            bucket_name = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            try:
                response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
                file_size = response['ContentLength']
                print(f"✓ {data_type}: Found file at {key} (Size: {file_size} bytes)")
                
                # Get expected structure info
                if data_type in expected_structures:
                    structure = expected_structures[data_type]
                    print(f"  - Expected fields: {', '.join(structure['required_fields'])}")
                
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"✗ {data_type}: File not found at {key}")
                else:
                    print(f"✗ {data_type}: Error accessing file - {e}")

    def create_crawler_with_multiple_targets(self):
        """
        Create Glue crawler with multiple S3 targets for different data sources
        """
        # Get IAM role ARN
        try:
            response = self.iam_client.get_role(RoleName=self.crawler_role_name)
            role_arn = response['Role']['Arn']
        except ClientError:
            print("IAM role not found. Creating new role...")
            # role_arn = self.create_iam_role_for_crawler()
        
        # Define S3 targets for crawler
        s3_targets = []
        for data_type, s3_path in self.s3_paths.items():
            # Extract path without filename for crawler target
            path_parts = s3_path.split('/')
            s3_target_path = '/'.join(path_parts[:-1]) + '/'
            
            s3_targets.append({
                'Path': s3_target_path,
                'Exclusions': [
                    '*.tmp',
                    '*.log',
                    '*_$folder$'
                ]
            })
        
        # Remove duplicates (since all files are in the same folder)
        unique_s3_targets = []
        seen_paths = set()
        for target in s3_targets:
            if target['Path'] not in seen_paths:
                unique_s3_targets.append(target)
                seen_paths.add(target['Path'])
        
        # Crawler configuration
        crawler_config = {
            'Name': self.crawler_name,
            'Role': role_arn,
            'DatabaseName': self.database_name,
            'Description': 'Crawler for Yogyakarta Tourism accommodation data from multiple sources',
            'Targets': {
                'S3Targets': unique_s3_targets
            },
            'TablePrefix': 'yogya_tourism_',
            'SchemaChangePolicy': {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            'RecrawlPolicy': {
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            'LineageConfiguration': {
                'CrawlerLineageSettings': 'ENABLE'
            },
            'Configuration': json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior": "InheritFromTable"
                    },
                    "Tables": {
                        "AddOrUpdateBehavior": "MergeNewColumns"
                    }
                },
                "Grouping": {
                    "TableGroupingPolicy": "CombineCompatibleSchemas"
                }
            })
        }
        
        try:
            response = self.glue_client.create_crawler(**crawler_config)
            print(f"Created Glue crawler: {self.crawler_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Crawler {self.crawler_name} already exists. Updating configuration...")
                
                # Update existing crawler
                update_config = crawler_config.copy()
                del update_config['Name']  # Remove name from update config
                
                self.glue_client.update_crawler(
                    Name=self.crawler_name,
                    **update_config
                )
                print("Updated existing crawler configuration")
                return True
            else:
                print(f"Error creating crawler: {e}")
                raise

    def run_crawler(self):
        """
        Start the Glue crawler to discover schema
        """
        try:
            response = self.glue_client.start_crawler(Name=self.crawler_name)
            print(f"Started crawler: {self.crawler_name}")
            
            # Monitor crawler status
            print("Monitoring crawler execution...")
            while True:
                response = self.glue_client.get_crawler(Name=self.crawler_name)
                state = response['Crawler']['State']
                
                print(f"Crawler state: {state}")
                
                if state == 'READY':
                    last_crawl = response['Crawler'].get('LastCrawl', {})
                    if last_crawl:
                        print(f"Crawler completed successfully!")
                        print(f"Tables created: {last_crawl.get('TablesCreated', 0)}")
                        print(f"Tables updated: {last_crawl.get('TablesUpdated', 0)}")
                        print(f"Tables deleted: {last_crawl.get('TablesDeleted', 0)}")
                    break
                elif state == 'STOPPING' or state == 'STOPPED':
                    print("Crawler stopped")
                    break
                
                time.sleep(30)  # Wait 30 seconds before checking again
                
        except ClientError as e:
            print(f"Error running crawler: {e}")
            raise

    def get_discovered_tables(self):
        """
        Get information about tables discovered by the crawler
        """
        try:
            response = self.glue_client.get_tables(DatabaseName=self.database_name)
            tables = response['TableList']
            
            print(f"\nDiscovered {len(tables)} tables in database '{self.database_name}':")
            print("=" * 80)
            
            for table in tables:
                table_name = table['Name']
                storage_descriptor = table.get('StorageDescriptor', {})
                location = storage_descriptor.get('Location', 'N/A')
                
                print(f"\nTable: {table_name}")
                print(f"Location: {location}")
                print(f"Columns: {len(storage_descriptor.get('Columns', []))}")
                
                # Display column information
                columns = storage_descriptor.get('Columns', [])
                if columns:
                    print("Schema:")
                    for col in columns[:10]:  # Show first 10 columns
                        col_name = col.get('Name', 'Unknown')
                        col_type = col.get('Type', 'Unknown')
                        print(f"  - {col_name}: {col_type}")
                    
                    if len(columns) > 10:
                        print(f"  ... and {len(columns) - 10} more columns")
                
                # Show partition information if available
                partition_keys = table.get('PartitionKeys', [])
                if partition_keys:
                    print(f"Partition Keys: {[pk['Name'] for pk in partition_keys]}")
                
                print("-" * 40)
            
            return tables
            
        except ClientError as e:
            print(f"Error getting tables: {e}")
            raise

    def generate_data_catalog_report(self):
        """
        Generate comprehensive report of discovered data catalog
        """
        try:
            tables = self.get_discovered_tables()
            
            # Generate detailed report
            report = {
                'database_name': self.database_name,
                'total_tables': len(tables),
                'discovery_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'tables': []
            }
            
            for table in tables:
                table_info = {
                    'name': table['Name'],
                    'location': table.get('StorageDescriptor', {}).get('Location', ''),
                    'input_format': table.get('StorageDescriptor', {}).get('InputFormat', ''),
                    'output_format': table.get('StorageDescriptor', {}).get('OutputFormat', ''),
                    'columns': [],
                    'row_count': table.get('Parameters', {}).get('recordCount', 'Unknown'),
                    'file_size': table.get('Parameters', {}).get('sizeKey', 'Unknown')
                }
                
                # Add column details
                columns = table.get('StorageDescriptor', {}).get('Columns', [])
                for col in columns:
                    table_info['columns'].append({
                        'name': col.get('Name'),
                        'type': col.get('Type'),
                        'comment': col.get('Comment', '')
                    })
                
                report['tables'].append(table_info)
            
            # Save report to file
            report_filename = f"yogyakarta_tourism_data_catalog_report_{int(time.time())}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"\nData catalog report saved to: {report_filename}")
            return report
            
        except Exception as e:
            print(f"Error generating report: {e}")
            raise

    def setup_complete_pipeline(self):
        """
        Run complete setup for schema discovery phase
        """
        print("Starting Yogyakarta Tourism Data Pipeline Schema Discovery Setup")
        print("=" * 70)
        
        try:
            # Step 1: Verify S3 data structure
            print("\n1. Verifying S3 data structure...")
            self.verify_s3_data_structure()
            
            # Step 2: Create IAM role
            print("\n2. Setting up IAM role for crawler...")
            # self.create_iam_role_for_crawler()
            
            # Step 3: Create Glue database
            print("\n3. Creating Glue database...")
            self.create_glue_database()
            
            # Step 4: Create and configure crawler
            print("\n4. Creating Glue crawler...")
            self.create_crawler_with_multiple_targets()
            
            # Step 5: Run crawler for schema discovery
            print("\n5. Running crawler for schema discovery...")
            self.run_crawler()
            
            # Step 6: Generate data catalog report
            print("\n6. Generating data catalog report...")
            report = self.generate_data_catalog_report()
            
            print("\n" + "=" * 70)
            print("Schema Discovery Phase Completed Successfully!")
            print(f"Database: {self.database_name}")
            print(f"Tables discovered: {report['total_tables']}")
            print(f"Crawler: {self.crawler_name}")
            print("=" * 70)
            
            return True
            
        except Exception as e:
            print(f"\nError in pipeline setup: {e}")
            return False


# Usage Example
if __name__ == "__main__":
    # Initialize the crawler
    crawler = YogyakartaTourismDataCrawler(region_name='us-east-1')  # Change region as needed
    
    # Run complete setup
    success = crawler.setup_complete_pipeline()
    
    if success:
        print("\nNext steps:")
        print("1. Review the discovered tables in AWS Glue Console")
        print("2. Verify schema accuracy against source data")
        print("3. Proceed with ETL transformation design")
        print("4. Set up data quality checks")
    else:
        print("\nSetup failed. Please check the error messages above.")
