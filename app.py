from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import uuid
import os
import threading
from datetime import datetime
import time
import psycopg2
from psycopg2 import Error
import psycopg2.extras
import hashlib
import re
from nk15 import IntegratedNaukriScraper
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

# Database Configuration
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'job_scraper_db'),
    'port': int(os.getenv('DB_PORT', 5432))
}

# In-memory storage for job status
jobs = {}

class ScrapingJob:
    def __init__(self, job_id, platform, keywords, location, limit, headless=True, fast_mode=False):
        self.job_id = job_id
        self.platform = platform
        self.keywords = keywords
        self.location = location
        self.limit = limit
        self.headless = headless
        self.slow_mode = not fast_mode
        self.status = "pending"
        self.progress = 0
        self.message = "Job queued"
        self.result = None
        self.error = None
        self.start_time = datetime.now()
        self.end_time = None
        self.db_saved = False
        self.skipped_count = 0
        
    def run_scraping(self):
        try:
            self.status = "running"
            self.progress = 10
            self.message = f"Initializing {self.platform} scraper..."
            
            # Initialize scraper
            scraper = IntegratedNaukriScraper(
                headless=self.headless, 
                slow_mode=self.slow_mode,
                debug=False,
                db_config=DB_CONFIG
            )
            
            self.progress = 30
            self.message = f"Searching for '{self.keywords}' in {self.location} (checking for duplicates)..."
            
            # Run the scraper
            df = scraper.search_jobs(
                keywords=self.keywords,
                location=self.location,
                limit=self.limit
            )
            
            self.progress = 80
            self.message = "Processing results and saving to database..."
            
            if df.empty:
                self.status = "completed"
                self.progress = 100
                self.message = "No new jobs found (all already in database or no results)"
                self.result = {
                    "jobs": [], 
                    "stats": {
                        "total_jobs": 0, 
                        "saved_to_db": 0,
                        "skipped_duplicates": 0,
                        "jobs_with_skills": 0,
                        "companies": 0,
                        "jobs_with_salary": 0,
                        "locations": 0,
                        "jobs_with_description": 0
                    }
                }
            else:
                # Convert DataFrame to JSON-serializable format
                jobs_data = df.to_dict('records')
                
                # Save to PostgreSQL database
                saved_count = self._save_to_database(jobs_data)
                
                # Calculate comprehensive stats
                stats = {
                    "total_jobs": len(jobs_data),
                    "saved_to_db": saved_count,
                    "skipped_duplicates": self.skipped_count,
                    "jobs_with_skills": sum(1 for job in jobs_data if job.get('extracted_skills', '') not in ['Skills not specified', '']),
                    "companies": len(set(job.get('company', '') for job in jobs_data)),
                    "jobs_with_salary": sum(1 for job in jobs_data if job.get('salary_range', '') not in ['N/A', 'Not Disclosed', '']),
                    "locations": len(set(job.get('location', '') for job in jobs_data)),
                    "jobs_with_description": sum(1 for job in jobs_data if job.get('description', '') not in ['Not available', ''])
                }
                
                self.result = {
                    "jobs": jobs_data,
                    "stats": stats
                }
                
                self.status = "completed"
                self.progress = 100
                self.message = f"Successfully scraped {len(jobs_data)} NEW jobs. {saved_count} saved to DB. {self.skipped_count} duplicates skipped."
                self.db_saved = saved_count > 0
            
            # Close scraper
            scraper.close()
            
        except Exception as e:
            self.status = "error"
            self.error = str(e)
            self.message = f"Scraping failed: {str(e)}"
            self.progress = 0
        finally:
            self.end_time = datetime.now()

    def _save_to_database(self, jobs_data):
        """Save job data to PostgreSQL database with source_platform column"""
        connection = None
        try:
            connection = psycopg2.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                dbname=DB_CONFIG['database'],
                port=DB_CONFIG['port'],
                sslmode='require'
            )
            connection.autocommit = False 
            cursor = connection.cursor()
            
            # Create table with source_platform column
            create_table_query = """
                CREATE TABLE IF NOT EXISTS naukri_jobs (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(500),
                    company VARCHAR(255),
                    location VARCHAR(255),
                    posted_time VARCHAR(100),
                    description TEXT,
                    employment_type VARCHAR(100),
                    seniority_level VARCHAR(100),
                    industries VARCHAR(255),
                    job_function VARCHAR(255),
                    company_url VARCHAR(500),
                    company_logo VARCHAR(500),
                    company_about TEXT,
                    job_url TEXT,
                    search_keywords VARCHAR(255),
                    search_location VARCHAR(255),
                    extracted_skills TEXT,
                    salary_range VARCHAR(255),
                    applicant_count VARCHAR(100),
                    source_platform VARCHAR(50) DEFAULT 'naukri',
                    job_hash VARCHAR(64) UNIQUE,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            cursor.execute(create_table_query)
            
            # Create indexes
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_search ON naukri_jobs (search_keywords, search_location)",
                "CREATE INDEX IF NOT EXISTS idx_company ON naukri_jobs (company)",
                "CREATE INDEX IF NOT EXISTS idx_location ON naukri_jobs (location)",
                "CREATE INDEX IF NOT EXISTS idx_scraped ON naukri_jobs (scraped_at)",
                "CREATE INDEX IF NOT EXISTS idx_platform ON naukri_jobs (source_platform)",
            ]
            for query in index_queries:
                cursor.execute(query)

            # Insert job data with source_platform
            insert_query = """
            INSERT INTO naukri_jobs (
                title, company, location, posted_time, description, employment_type,
                seniority_level, industries, job_function, company_url, company_logo,
                company_about, job_url, search_keywords, search_location,
                extracted_skills, salary_range, applicant_count, source_platform, job_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO UPDATE SET
                description=EXCLUDED.description,
                extracted_skills=EXCLUDED.extracted_skills,
                salary_range=EXCLUDED.salary_range,
                applicant_count=EXCLUDED.applicant_count,
                posted_time=EXCLUDED.posted_time
            """
            
            saved_count = 0
            for job in jobs_data:
                try:
                    job_hash = job.get('job_hash', '')
                    
                    values = (
                        job.get('title'),
                        job.get('company'),
                        job.get('location'),
                        job.get('posted_time'),
                        job.get('description'),
                        job.get('employment_type'),
                        job.get('seniority_level'),
                        job.get('industries'),
                        job.get('job_function'),
                        job.get('company_url'),
                        job.get('company_logo'),
                        job.get('company_about'),
                        job.get('job_url'),
                        self.keywords,
                        self.location,
                        job.get('extracted_skills'),
                        job.get('salary_range'),
                        job.get('applicant_count'),
                        'naukri',  # source_platform
                        job_hash
                    )
                    cursor.execute(insert_query, values)
                    saved_count += 1
                except Exception as e:
                    print(f"⚠️ Error inserting job {job.get('title')}: {e}")
                    continue
            
            connection.commit()
            print(f"💾 Saved {saved_count} jobs to database")
            return saved_count
            
        except Error as e:
            print(f"❌ Database error: {e}")
            return 0
        finally:
            if connection and not connection.closed:
                cursor.close()
                connection.close()

def run_scraping_job(job_id):
    """Run scraping job in background thread"""
    job = jobs[job_id]
    job.run_scraping()

@app.route('/')
def serve_frontend():
    return render_template('scraper.html')

@app.route('/marketplace')
def serve_marketplace():
    return render_template('marketplace.html')

@app.route('/api/scrape', methods=['POST'])
def start_scraping():
    """Start a new scraping job"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['platform', 'keywords', 'location']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({
                    'success': False,
                    'error': f'Missing required field: {field}'
                }), 400
        
        platform = data['platform']
        keywords = data['keywords']
        location = data['location']
        limit = int(data.get('limit', 10))
        headless = data.get('headless', True)
        fast_mode = data.get('fast', True)
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Create job object
        job = ScrapingJob(
            job_id=job_id,
            platform=platform,
            keywords=keywords,
            location=location,
            limit=limit,
            headless=headless,
            fast_mode=fast_mode
        )
        
        # Store job
        jobs[job_id] = job
        
        # Start scraping in background thread
        thread = threading.Thread(target=run_scraping_job, args=(job_id,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'Scraping job started for {keywords} in {location} (auto-skipping duplicates)'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to start scraping job: {str(e)}'
        }), 500

@app.route('/api/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get status of a scraping job - FIXED response format"""
    if job_id not in jobs:
        return jsonify({
            'success': False,
            'error': 'Job not found'
        }), 404
    
    job = jobs[job_id]
    
    # Return data in the format frontend expects
    response = {
        'success': True,
        'status': {
            'status': job.status,
            'progress': job.progress,
            'message': job.message,
            'platform': job.platform,
            'keywords': job.keywords,
            'location': job.location,
            'start_time': job.start_time.isoformat() if job.start_time else None,
            'db_saved': job.db_saved,
            'skipped_duplicates': job.skipped_count
        }
    }
    
    if job.status == 'completed' and job.result:
        response['status']['result'] = job.result
        response['status']['quality_stats'] = job.result.get('stats', {})
    
    if job.status == 'error' and job.error:
        response['status']['error'] = job.error
    
    return jsonify(response)

@app.route('/api/jobs', methods=['GET'])
def get_all_jobs():
    """Get all jobs from database - NEW ENDPOINT"""
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get all jobs ordered by scraped_at DESC
        cursor.execute("""
            SELECT * FROM naukri_jobs 
            ORDER BY scraped_at DESC
        """)
        jobs_result = cursor.fetchall()
        
        # Convert to list of dicts
        jobs_list = []
        for job in jobs_result:
            job_dict = dict(job)
            if job_dict.get('scraped_at'):
                job_dict['scraped_at'] = job_dict['scraped_at'].isoformat()
            # Add keywords field compatibility
            job_dict['keywords'] = job_dict.get('search_keywords') or 'manual_entry'
            jobs_list.append(job_dict)
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'jobs': jobs_list,
            'count': len(jobs_list)
        })
        
    except Error as e:
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500
    finally:
        if connection and not connection.closed:
            try:
                connection.close()
            except:
                pass

@app.route('/api/jobs', methods=['POST'])
def add_manual_job():
    """Add a job manually - NEW ENDPOINT"""
    connection = None
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get('title') or not data.get('company') or not data.get('location'):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: title, company, location'
            }), 400
        
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        cursor = connection.cursor()
        
        # Generate hash for manual entry
        def normalize(text):
            if not text:
                return ""
            text = text.lower()
            text = re.sub(r'[^\w\s]', '', text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        
        title_norm = normalize(data.get('title', ''))
        company_norm = normalize(data.get('company', ''))
        location_norm = normalize(data.get('location', ''))
        hash_input = f"{title_norm}|{company_norm}|{location_norm}"
        job_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
        
        # Insert job
        insert_query = """
        INSERT INTO naukri_jobs (
            title, company, location, posted_time, description, employment_type,
            seniority_level, industries, job_function, company_url, company_logo,
            company_about, job_url, search_keywords, search_location,
            extracted_skills, salary_range, applicant_count, source_platform, job_hash
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_hash) DO UPDATE SET
            description=EXCLUDED.description,
            extracted_skills=EXCLUDED.extracted_skills,
            salary_range=EXCLUDED.salary_range
        RETURNING id
        """
        
        values = (
            data.get('title'),
            data.get('company'),
            data.get('location'),
            data.get('posted_time', 'Manual Entry'),
            data.get('description', 'No description provided'),
            data.get('employment_type', 'Full-time'),
            data.get('seniority_level', 'Associate'),
            data.get('industries', 'Information Technology'),
            data.get('job_function', 'Engineering'),
            data.get('company_url', 'N/A'),
            data.get('company_logo', 'Logo not available'),
            data.get('company_about', 'Company information not available'),
            data.get('job_url', 'N/A'),
            'manual_entry',  # search_keywords
            data.get('location'),  # search_location
            data.get('extracted_skills', 'Skills not specified'),
            data.get('salary_range', 'N/A'),
            data.get('applicant_count', 'Not specified'),
            data.get('source_platform', 'naukri'),  # source_platform
            job_hash
        )
        
        cursor.execute(insert_query, values)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'message': 'Job added successfully'
        })
        
    except Error as e:
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500
    finally:
        if connection and not connection.closed:
            try:
                connection.close()
            except:
                pass

@app.route('/api/jobs/recent', methods=['GET'])
def get_recent_jobs():
    """Get list of recent scraping jobs"""
    recent_jobs = []
    
    sorted_jobs = sorted(jobs.items(), key=lambda x: x[1].start_time if x[1].start_time else datetime.min, reverse=True)
    
    for job_id, job in sorted_jobs[:10]:
        recent_jobs.append({
            'id': job_id,
            'platform': job.platform,
            'keywords': job.keywords,
            'location': job.location,
            'status': job.status,
            'progress': job.progress,
            'start_time': job.start_time.isoformat() if job.start_time else None,
            'total_jobs': job.result.get('stats', {}).get('total_jobs', 0) if job.result else 0,
            'skipped_duplicates': job.result.get('stats', {}).get('skipped_duplicates', 0) if job.result else 0,
            'db_saved': job.db_saved
        })
    
    return jsonify({
        'success': True,
        'jobs': recent_jobs
    })

@app.route('/api/jobs/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    """Delete a specific job"""
    if job_id in jobs:
        del jobs[job_id]
        return jsonify({
            'success': True,
            'message': 'Job deleted successfully'
        })
    else:
        return jsonify({
            'success': False,
            'error': 'Job not found'
        }), 404

@app.route('/api/database/stats', methods=['GET'])
def get_database_stats():
    """Get database statistics"""
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        cursor = connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM naukri_jobs")
        total_jobs = cursor.fetchone()[0]
        
        cursor.execute("SELECT company, COUNT(*) FROM naukri_jobs GROUP BY company ORDER BY COUNT(*) DESC LIMIT 10")
        companies = cursor.fetchall()
        
        cursor.execute("SELECT location, COUNT(*) FROM naukri_jobs GROUP BY location ORDER BY COUNT(*) DESC LIMIT 10")
        locations = cursor.fetchall()
        
        cursor.execute("SELECT title, company, location, scraped_at FROM naukri_jobs ORDER BY scraped_at DESC LIMIT 5")
        latest_jobs = cursor.fetchall()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_jobs': total_jobs,
                'top_companies': [{'company': row[0], 'count': row[1]} for row in companies],
                'top_locations': [{'location': row[0], 'count': row[1]} for row in locations],
                'latest_jobs': [{'title': row[0], 'company': row[1], 'location': row[2], 'scraped_at': row[3].isoformat() if row[3] else None} for row in latest_jobs]
            }
        })
        
    except Error as e:
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500
    finally:
        if connection and not connection.closed:
            cursor.close()
            connection.close()

@app.route('/api/database/search', methods=['GET'])
def search_database():
    """Search jobs in database"""
    connection = None
    try:
        keywords = request.args.get('keywords', '')
        location = request.args.get('location', '')
        company = request.args.get('company', '')
        limit = int(request.args.get('limit', 50))
        
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = "SELECT * FROM naukri_jobs WHERE 1=1"
        params = []
        
        if keywords:
            query += " AND (title ILIKE %s OR extracted_skills ILIKE %s OR description ILIKE %s)"
            params.extend([f'%{keywords}%', f'%{keywords}%', f'%{keywords}%'])
        
        if location:
            query += " AND location ILIKE %s"
            params.append(f'%{location}%')
            
        if company:
            query += " AND company ILIKE %s"
            params.append(f'%{company}%')
        
        query += " ORDER BY scraped_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        jobs_result = cursor.fetchall()
        
        jobs_list = []
        for job in jobs_result:
            job_dict = dict(job)
            if job_dict.get('scraped_at'):
                job_dict['scraped_at'] = job_dict['scraped_at'].isoformat()
            jobs_list.append(job_dict)
        
        return jsonify({
            'success': True,
            'jobs': jobs_list,
            'count': len(jobs_list)
        })
        
    except Error as e:
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500
    finally:
        if connection and not connection.closed:
            cursor.close()
            connection.close()

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    active_jobs = len([job for job in jobs.values() if job.status == 'running'])
    completed_jobs = len([job for job in jobs.values() if job.status == 'completed'])
    
    db_healthy = False
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        db_healthy = True
    except:
        pass
    finally:
        if connection and not connection.closed:
            connection.close()
    
    return jsonify({
        'success': True,
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if db_healthy else 'disconnected',
        'jobs_summary': {
            'total': len(jobs),
            'active': active_jobs,
            'completed': completed_jobs,
            'pending': len(jobs) - active_jobs - completed_jobs
        }
    })

def initialize_database():
    """Attempt to connect to the PostgreSQL database on startup"""
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dbname=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            sslmode='require'
        )
        connection.close()
        print(f"✅ Database connection test successful!")
        
    except Error as e:
        print(f"❌ Database initialization error: {e}")

def cleanup_old_jobs():
    """Clean up old job data periodically"""
    while True:
        time.sleep(300)
        if len(jobs) > 50:
            sorted_jobs = sorted(jobs.items(), key=lambda x: x[1].start_time if x[1].start_time else datetime.min)
            for job_id, _ in sorted_jobs[:len(jobs) - 50]:
                del jobs[job_id]
            print(f"🧹 Cleaned up old jobs, kept {len(jobs)} most recent")

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_jobs)
cleanup_thread.daemon = True
cleanup_thread.start()

if __name__ == '__main__':
    initialize_database()
    
    print("🚀 Starting Naukri Scraper API with PostgreSQL Database & DEDUPLICATION...")
    print("📊 API Endpoints:")
    print("    POST /api/scrape - Start scraping job")
    print("    GET  /api/status/<job_id> - Check job status") 
    print("    GET  /api/jobs - Get all jobs from database (NEW)")
    print("    POST /api/jobs - Add job manually (NEW)")
    print("    GET  /api/jobs/recent - List recent scraping jobs")
    print("    GET  /api/database/stats - Get database statistics")
    print("    GET  /api/database/search - Search jobs in database")
    print("    DELETE /api/jobs/<job_id> - Delete job")
    print("    GET  /api/health - Health check")
    print("\n💾 Database includes 'source_platform' column (default: 'naukri')")
    print("🔒 SSLMODE='require' ENABLED for secure database connection.")
    print("🔍 DEDUPLICATION ENABLED")
    print("🧹 Automatic cleanup: Keeps last 50 jobs in memory")
    print("🌐 Access at: http://localhost:5000")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
