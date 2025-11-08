from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import uuid
import os
import threading
from datetime import datetime
import time
import psycopg2  # <-- CHANGED: PostgreSQL connector
from psycopg2 import Error # <-- CHANGED: PostgreSQL Error handling
import hashlib
from nk15 import IntegratedNaukriScraper
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

# MySQL Database Configuration
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'job_scraper_db'),
    'port': int(os.getenv('DB_PORT', 5432)) # <-- CHANGED: Default port to 5432 (PostgreSQL default)
}

# In-memory storage for job status (no persistence)
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
            
            # Initialize scraper with DB config for deduplication
            scraper = IntegratedNaukriScraper(
                headless=self.headless, 
                slow_mode=self.slow_mode,
                debug=False,
                db_config=DB_CONFIG
            )
            
            self.progress = 30
            self.message = f"Searching for '{self.keywords}' in {self.location} (checking for duplicates)..."
            
            # Run the scraper (it will automatically skip duplicates)
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
                self.result = {"jobs": [], "stats": {"total_jobs": 0, "skipped_duplicates": 0}}
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
        finally:
            self.end_time = datetime.now()

    def _save_to_database(self, jobs_data):
        """Save job data to PostgreSQL database - ONLY 18 required columns"""
        connection = None # Initialize connection outside try/except
        try:
            connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
            # We explicitly disable autocommit to use a transaction
            connection.autocommit = False 
            cursor = connection.cursor()
            
            # Create table with ONLY 18 required columns + internal fields
            # NOTE: PostgreSQL uses SERIAL for auto-increment, TEXT for large strings, 
            # and requires explicit `UNIQUE` constraint for ON CONFLICT clause.
            create_table_query = """
                CREATE TABLE IF NOT EXISTS naukri_jobs (
                    id SERIAL PRIMARY KEY,  -- CHANGED to SERIAL
                    title VARCHAR(500),
                    company VARCHAR(255),
                    location VARCHAR(255),
                    posted_time VARCHAR(100),
                    description TEXT,       -- CHANGED to TEXT
                    employment_type VARCHAR(100),
                    seniority_level VARCHAR(100),
                    industries VARCHAR(255),
                    job_function VARCHAR(255),
                    company_url VARCHAR(500),
                    company_logo VARCHAR(500),
                    company_about TEXT,     -- CHANGED to TEXT
                    job_url TEXT,           -- CHANGED to TEXT
                    search_keywords VARCHAR(255),
                    search_location VARCHAR(255),
                    extracted_skills TEXT,  -- CHANGED to TEXT
                    salary_range VARCHAR(255),
                    applicant_count VARCHAR(100),
                    job_hash VARCHAR(64) UNIQUE, -- ADDED UNIQUE CONSTRAINT
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_search (search_keywords, search_location), -- Note: Psycopg2/PG supports index creation this way
                    INDEX idx_company (company),
                    INDEX idx_location (location),
                    INDEX idx_scraped (scraped_at)
                )
            """
            cursor.execute(create_table_query)
            
            # Insert job data with ONLY 18 required fields
            # NOTE: Uses PostgreSQL's ON CONFLICT DO UPDATE SET (UPSERT)
            insert_query = """
            INSERT INTO naukri_jobs (
                title, company, location, posted_time, description, employment_type,
                seniority_level, industries, job_function, company_url, company_logo,
                company_about, job_url, search_keywords, search_location,
                extracted_skills, salary_range, applicant_count, job_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_hash) DO UPDATE SET -- CHANGED to PostgreSQL syntax
                description=EXCLUDED.description,
                extracted_skills=EXCLUDED.extracted_skills,
                salary_range=EXCLUDED.salary_range,
                applicant_count=EXCLUDED.applicant_count,
                posted_time=EXCLUDED.posted_time
            """
            
            saved_count = 0
            for job in jobs_data:
                try:
                    # Use the pre-computed hash from scraper
                    job_hash = job.get('job_hash', '')
                    
                    # Map to EXACTLY 18 required columns as per specification
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
                        self.keywords,  # search_keywords
                        self.location,  # search_location
                        job.get('extracted_skills'),
                        job.get('salary_range'),
                        job.get('applicant_count'),
                        job_hash
                    )
                    cursor.execute(insert_query, values)
                    saved_count += 1
                except Exception as e:
                    print(f"⚠️ Error inserting job {job.get('title')}: {e}")
                    # Continue loop, but print error
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
    """Get status of a scraping job"""
    if job_id not in jobs:
        return jsonify({
            'success': False,
            'error': 'Job not found'
        }), 404
    
    job = jobs[job_id]
    
    response = {
        'success': True,
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
    
    if job.status == 'completed' and job.result:
        response['result'] = job.result
        response['quality_stats'] = job.result.get('stats', {})
    
    if job.status == 'error' and job.error:
        response['error'] = job.error
    
    return jsonify(response)

@app.route('/api/jobs/recent', methods=['GET'])
def get_recent_jobs():
    """Get list of recent scraping jobs"""
    recent_jobs = []
    
    # Sort jobs by start time (newest first) and take last 10
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
        connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
        cursor = connection.cursor()
        
        # Get total jobs count
        cursor.execute("SELECT COUNT(*) FROM naukri_jobs")
        total_jobs = cursor.fetchone()[0]
        
        # Get jobs by company
        cursor.execute("SELECT company, COUNT(*) FROM naukri_jobs GROUP BY company ORDER BY COUNT(*) DESC LIMIT 10")
        companies = cursor.fetchall()
        
        # Get jobs by location
        cursor.execute("SELECT location, COUNT(*) FROM naukri_jobs GROUP BY location ORDER BY COUNT(*) DESC LIMIT 10")
        locations = cursor.fetchall()
        
        # Get latest jobs
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
        
        connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor) # Using DictCursor for dictionary results
        
        query = "SELECT * FROM naukri_jobs WHERE 1=1"
        params = []
        
        if keywords:
            query += " AND (title ILIKE %s OR extracted_skills ILIKE %s OR description ILIKE %s)" # ILIKE for case-insensitive search in PG
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
        
        # Convert fetched records to standard dictionary list and handle datetime objects
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

@app.route('/api/database/check-duplicates', methods=['POST'])
def check_duplicates():
    """Check if specific jobs exist in database"""
    connection = None
    try:
        data = request.get_json()
        jobs_to_check = data.get('jobs', [])
        
        connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
        cursor = connection.cursor()
        
        # Compute hashes and check
        results = []
        for job in jobs_to_check:
            title = job.get('title', '')
            company = job.get('company', '')
            location = job.get('location', '')
            job_url = job.get('job_url', '')
            
            # Compute hash
            title_normalized = re.sub(r'\s+', ' ', title.strip().lower())
            company_normalized = re.sub(r'\s+', ' ', company.strip().lower())
            location_normalized = re.sub(r'\s+', ' ', location.strip().lower())
            hash_input = f"{title_normalized}|{company_normalized}|{location_normalized}|{job_url}"
            job_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
            
            cursor.execute("SELECT COUNT(*) FROM naukri_jobs WHERE job_hash = %s", (job_hash,))
            exists = cursor.fetchone()[0] > 0
            
            results.append({
                'title': title,
                'company': company,
                'exists': exists,
                'hash': job_hash
            })
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'results': results
        })
        
    except Error as e:
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    active_jobs = len([job for job in jobs.values() if job.status == 'running'])
    completed_jobs = len([job for job in jobs.values() if job.status == 'completed'])
    
    # Check database connection
    db_healthy = False
    connection = None
    try:
        connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
        db_healthy = connection.is_connected()
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

# Initialize database on startup
def initialize_database():
    """Attempt to connect to the PostgreSQL database on startup"""
    try:
        connection = psycopg2.connect(**DB_CONFIG) # <-- CHANGED
        connection.close()
        print(f"✅ Database connection test successful!")
        
    except Error as e:
        print(f"❌ Database initialization error: {e}")

# Clean up old job data periodically (keep only last 50 jobs)
def cleanup_old_jobs():
    while True:
        time.sleep(300)  # 5 minutes
        if len(jobs) > 50:
            # Sort by start time and remove oldest
            sorted_jobs = sorted(jobs.items(), key=lambda x: x[1].start_time if x[1].start_time else datetime.min)
            for job_id, _ in sorted_jobs[:len(jobs) - 50]:
                del jobs[job_id]
            print(f"🧹 Cleaned up old jobs, kept {len(jobs)} most recent")

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_jobs)
cleanup_thread.daemon = True
cleanup_thread.start()

if __name__ == '__main__':
    # Initialize database
    initialize_database()
    
    print("🚀 Starting Naukri Scraper API with PostgreSQL Database & DEDUPLICATION...")
    print("📊 API Endpoints:")
    print("   POST /api/scrape - Start scraping job (auto-skips duplicates)")
    print("   GET  /api/status/<job_id> - Check job status") 
    print("   GET  /api/jobs/recent - List recent jobs")
    print("   GET  /api/database/stats - Get database statistics")
    print("   GET  /api/database/search - Search jobs in database")
    print("   POST /api/database/check-duplicates - Check if jobs exist")
    print("   DELETE /api/jobs/<job_id> - Delete job")
    print("   GET  /api/health - Health check")
    print("\n💾 Saving ONLY 18 required columns to PostgreSQL")
    print("🔍 DEDUPLICATION ENABLED:")
    print("   - Loads existing job hashes on startup")
    print("   - Checks each job before scraping")
    print("   - Skips jobs already in database")
    print("   - Uses hash of: title + company + location + URL")
    print("\n🧹 Automatic cleanup: Keeps last 50 jobs in memory")
    print("🌐 Access the frontend at: http://localhost:5000")
    
    # Run without debug mode to prevent restarts
    app.run(host='0.0.0.0', port=5000, debug=False)
