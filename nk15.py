#!/usr/bin/env python3
"""
INTEGRATED NAUKRI SCRAPER - PERFECT DEDUPLICATION
Uses job_url as PRIMARY unique identifier + normalized hash as backup
"""

import time
import random
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional, Set
import hashlib
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options


# DO NOT ADD: from nk15 import IntegratedNaukriScraper
# This causes circular import!


class IntegratedNaukriScraper:
    """
    Naukri scraper - extracts 18 required fields
    PERFECT deduplication using job_url + normalized hash
    """
    
    SELECTORS = {
        'job_cards': [
            'article.jobTuple',
            'div.srp-jobtuple-wrapper',
            'div.cust-job-tuple',
            'article[data-job-id]',
        ],
        'title': [
            'a.title',
            'a[class*="title"]',
        ],
        'company': [
            'a.subTitle',
            'a.comp-name',
        ],
        'location': [
            'span.locWdth',
            'span[class*="location"]',
            'li.location',
            'li.fleft.br2.placeHolderLi.location',
        ],
        'salary': [
            'span.salary',
            'li.fleft.br2.placeHolderLi.salary',
        ],
    }
    
    def __init__(self, headless=True, slow_mode=True, debug=False, db_config=None):
        self.headless = headless
        self.slow_mode = slow_mode
        self.debug = debug
        self.driver = None
        self.wait = None
        self.scraped_jobs = []
        self.existing_urls: Set[str] = set()  # PRIMARY: URLs
        self.existing_hashes: Set[str] = set()  # BACKUP: Hashes
        self.db_config = db_config or self._load_db_config()
        self._load_existing_jobs()
        self._setup_driver()
    
    def _load_db_config(self):
        """Load database configuration"""
        load_dotenv()
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'job_scraper_db'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
    
    def _load_existing_jobs(self):
        """Load existing job URLs AND hashes from database"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            # Get both URLs and hashes
            cursor.execute("SELECT job_url, job_hash FROM naukri_jobs")
            records = cursor.fetchall()
            
            self.existing_urls = {self._normalize_url(url) for url, _ in records if url}
            self.existing_hashes = {hash_val for _, hash_val in records if hash_val}
            
            print(f"📊 Loaded {len(self.existing_urls)} existing job URLs from database")
            print(f"📊 Loaded {len(self.existing_hashes)} existing job hashes from database")
            
            cursor.close()
            connection.close()
            
        except Error as e:
            print(f"⚠️ Could not load existing jobs: {e}")
            self.existing_urls = set()
            self.existing_hashes = set()
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for comparison
        Remove query parameters, trailing slashes, etc.
        """
        if not url:
            return ""
        
        # Remove protocol
        url = re.sub(r'^https?://', '', url.lower())
        
        # Remove www.
        url = re.sub(r'^www\.', '', url)
        
        # Remove query parameters
        url = url.split('?')[0]
        
        # Remove trailing slash
        url = url.rstrip('/')
        
        return url
    
    def _compute_job_hash(self, title: str, company: str, location: str) -> str:
        """
        Compute hash from ONLY: title + company + location
        DO NOT include job_url or search keywords
        """
        # Aggressive normalization
        def normalize(text: str) -> str:
            if not text:
                return ""
            # Convert to lowercase
            text = text.lower()
            # Remove all special characters and extra spaces
            text = re.sub(r'[^\w\s]', '', text)
            # Replace multiple spaces with single space
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        
        title_norm = normalize(title)
        company_norm = normalize(company)
        location_norm = normalize(location)
        
        # Create hash from normalized combination
        hash_input = f"{title_norm}|{company_norm}|{location_norm}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    def _is_job_already_scraped(self, title: str, company: str, location: str, job_url: str) -> bool:
        """
        Check if job exists using TWO-LEVEL CHECK:
        1. PRIMARY: Check by normalized URL (most reliable)
        2. BACKUP: Check by hash (catches URL variations)
        """
        # Level 1: Check URL (PRIMARY)
        if job_url:
            normalized_url = self._normalize_url(job_url)
            if normalized_url in self.existing_urls:
                return True
        
        # Level 2: Check hash (BACKUP)
        job_hash = self._compute_job_hash(title, company, location)
        if job_hash in self.existing_hashes:
            return True
        
        return False
    
    def _setup_driver(self):
        """Setup Chrome driver"""
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--start-maximized')
        options.add_argument('--lang=en-IN')
        
        prefs = {
            "profile.managed_default_content_settings.images": 1,
            "profile.managed_default_content_settings.javascript": 1,
        }
        options.add_experimental_option("prefs", prefs)
        
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        if self.headless:
            options.add_argument('--headless=new')
            options.add_argument('--window-size=1920,1080')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.wait = WebDriverWait(self.driver, 30)
            self.driver.implicitly_wait(10)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("✅ Chrome driver initialized")
        except Exception as e:
            print(f"❌ Failed to initialize driver: {e}")
            raise
    
    def _human_delay(self, min_sec=1, max_sec=3):
        """Add random delay"""
        if self.slow_mode:
            time.sleep(random.uniform(min_sec, max_sec))
        else:
            time.sleep(random.uniform(0.5, 1.5))
    
    def _safe_extract(self, element, selectors: List[str], attribute=None, default="N/A"):
        """Safely extract data"""
        for selector in selectors:
            try:
                sub_elements = element.find_elements(By.CSS_SELECTOR, selector)
                if sub_elements:
                    if attribute:
                        attr_value = sub_elements[0].get_attribute(attribute)
                        if attr_value:
                            return attr_value.strip()
                    else:
                        text = sub_elements[0].text.strip()
                        if text:
                            if 'salary' in selectors[0] and re.search(r'not\s+disclosed', text, re.IGNORECASE):
                                return "Not Disclosed"
                            return text
            except:
                continue
        return default

    def _extract_complete_description(self, job_url: str) -> Dict[str, str]:
        """Extract complete job description, skills, and company info"""
        if not job_url or job_url == 'N/A':
            return {
                'description': 'Not available',
                'extracted_skills': 'Skills not specified',
                'company_about': 'Not available'
            }
        
        original_window = self.driver.current_window_handle
        result = {
            'description': 'Not available',
            'extracted_skills': 'Skills not specified',
            'company_about': 'Not available'
        }
        
        try:
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.get(job_url)
            self._human_delay(4, 6)
            
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Build complete description
            description_parts = []
            
            # Main description
            main_desc = self._extract_main_description(soup)
            if main_desc != 'Description not available':
                description_parts.append("JOB DESCRIPTION\n" + main_desc)
            
            # Role details
            role_details = self._extract_role_details(soup)
            if role_details != 'Role details not available':
                description_parts.append("\nROLE DETAILS\n" + role_details)
            
            # Education
            education = self._extract_education(soup)
            if education != 'Education requirements not specified':
                description_parts.append("\nEDUCATION\n" + education)
            
            # Skills
            skills = self._extract_key_skills(soup)
            if skills != 'Skills not specified':
                description_parts.append("\nKEY SKILLS\n" + skills)
                result['extracted_skills'] = skills
            
            # Salary insights
            salary_insights = self._extract_salary_insights(soup)
            if salary_insights['avg_salary'] != 'Not captured':
                description_parts.append(f"\nSALARY INSIGHTS\nAverage: {salary_insights['avg_salary']}, Min: {salary_insights['min_salary']}, Max: {salary_insights['max_salary']}")
            
            if description_parts:
                result['description'] = '\n'.join(description_parts)
            
            # Company info
            company_info = self._extract_company_info(soup)
            result['company_about'] = company_info['about']
            
            self.driver.close()
            self.driver.switch_to.window(original_window)
            return result
            
        except Exception as e:
            print(f"      ⚠️ Job page error: {str(e)[:100]}")
            try:
                self.driver.close()
                self.driver.switch_to.window(original_window)
            except:
                pass
            return result

    def _extract_main_description(self, soup: BeautifulSoup) -> str:
        """Extract main job description"""
        try:
            jd_selectors = [
                {'name': 'div', 'class': re.compile(r'JDC__dang-inner-html', re.I)},
                {'name': 'div', 'class': re.compile(r'job-desc', re.I)},
                {'name': 'section', 'class': re.compile(r'job-desc', re.I)},
            ]
            
            for selector in jd_selectors:
                jd_section = soup.find(**selector)
                if jd_section:
                    for tag in jd_section(['script', 'style', 'button', 'a']):
                        tag.decompose()
                    text = jd_section.get_text(separator='\n', strip=True)
                    if text and len(text) > 100:
                        return re.sub(r'\s+', ' ', text.strip())
            return "Description not available"
        except:
            return "Description not available"

    def _extract_role_details(self, soup: BeautifulSoup) -> str:
        """Extract role details"""
        try:
            role_info = {}
            role_patterns = [
                ('Role:', r'Role:\s*([^\n]+)'),
                ('Industry Type:', r'Industry Type:\s*([^\n]+)'),
                ('Department:', r'Department:\s*([^\n]+)'),
                ('Employment Type:', r'Employment Type:\s*([^\n]+)'),
                ('Role Category:', r'Role Category:\s*([^\n]+)'),
            ]
            page_text = soup.get_text()
            for label, pattern in role_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    role_info[label] = match.group(1).strip()
            if role_info:
                return '\n'.join([f"{k} {v}" for k, v in role_info.items()])
            return "Role details not available"
        except:
            return "Role details not available"

    def _extract_education(self, soup: BeautifulSoup) -> str:
        """Extract education requirements"""
        try:
            education_info = {}
            edu_patterns = [
                ('UG:', r'UG:\s*([^\n]+(?:\n(?!PG:)[^\n]+)*)'),
                ('PG:', r'PG:\s*([^\n]+(?:\n(?!Doctorate)[^\n]+)*)'),
            ]
            page_text = soup.get_text()
            for label, pattern in edu_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = re.sub(r'\s+', ' ', match.group(1).strip())
                    education_info[label] = value
            if education_info:
                return '\n'.join([f"{k} {v}" for k, v in education_info.items()])
            return "Education requirements not specified"
        except:
            return "Education requirements not specified"

    def _extract_key_skills(self, soup: BeautifulSoup) -> str:
        """Extract key skills"""
        try:
            skills_set = set()
            skill_chips = soup.find_all(['a', 'span'], class_=re.compile(r'(chip|tag|skill)', re.I))
            for chip in skill_chips:
                skill = chip.get_text(strip=True)
                if skill and len(skill) < 50 and skill not in ['Key Skills', 'Skills']:
                    skills_set.add(skill)
            if skills_set:
                return ', '.join(sorted(list(skills_set)))
            return "Skills not specified"
        except:
            return "Skills not specified"

    def _extract_company_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract company information"""
        try:
            about_sections = soup.find_all(['div', 'section'], class_=re.compile(r'about-company', re.I))
            if about_sections:
                for tag in about_sections[0](['script', 'style', 'button', 'a']):
                    tag.decompose()
                about_text = about_sections[0].get_text(separator=' ', strip=True)
                if len(about_text) > 50:
                    return {'about': about_text[:1000]}
            return {'about': "Company information not available"}
        except:
            return {'about': "Company information not available"}

    def _extract_salary_insights(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract salary insights"""
        insights = {'avg_salary': 'Not captured', 'min_salary': 'Not captured', 'max_salary': 'Not captured'}
        try:
            salary_section_header = soup.find(string=re.compile(r'Salary Insights', re.I))
            if salary_section_header:
                parent_container = salary_section_header.find_parent('div')
                if parent_container:
                    section_text = parent_container.get_text(separator='\n', strip=True)
                    avg_match = re.search(r'Avg\.\s*salary\s*-\s*([^\n]+)', section_text, re.IGNORECASE)
                    if avg_match:
                        insights['avg_salary'] = avg_match.group(1).strip()
                    min_max_match = re.search(r'Min\s*([^\n]+)\nMax\s*([^\n]+)', section_text, re.IGNORECASE)
                    if min_max_match:
                        insights['min_salary'] = min_max_match.group(1).strip()
                        insights['max_salary'] = min_max_match.group(2).strip()
            return insights
        except:
            return insights

    def _extract_company_logo(self, card) -> str:
        """Extract company logo"""
        try:
            img_selectors = ['img[alt*="company"]', 'img[class*="logo"]', 'div.comp-img img', 'img.comp-img']
            for selector in img_selectors:
                try:
                    imgs = card.find_elements(By.CSS_SELECTOR, selector)
                    for img in imgs:
                        src = img.get_attribute('src')
                        if src and src not in ['', 'null', 'undefined']:
                            if src.startswith('//'):
                                src = 'https:' + src
                            return src
                except:
                    continue
            return "Logo not available"
        except:
            return "Logo not available"

    def _extract_posted_time(self, card) -> str:
        """Extract posted time"""
        try:
            card_text = card.text
            patterns = [
                r'Posted[:\s]*(\d+\s+(?:day|days|week|weeks|month|months)\s+ago)',
                r'Posted[:\s]*(Today|Yesterday|Just\s+Now)',
                r'(\d+\s+(?:day|days|week|weeks|month|months)\s+ago)',
                r'(Today|Yesterday|Just\s+Now)',
            ]
            for pattern in patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
            return "Recently"
        except:
            return "Recently"

    def _extract_salary_range(self, card) -> str:
        """Extract salary range"""
        try:
            card_text = card.text
            salary_match = re.search(r'(\d+\s*-\s*\d+\s+(?:Lacs?|Lakhs?)\s+P\.?A\.?)', card_text, re.IGNORECASE)
            if salary_match:
                return salary_match.group(1).strip()
            if re.search(r'Not\s+Disclosed', card_text, re.IGNORECASE):
                return "Not Disclosed"
            salary_text = self._safe_extract(card, self.SELECTORS['salary'], default="N/A")
            if salary_text != "N/A":
                return salary_text
            return "N/A"
        except:
            return "N/A"

    def _extract_applicant_count(self, card) -> str:
        """Extract applicant count"""
        try:
            card_text = card.text
            patterns = [r'(\d+\+?)\s+(?:Applicants?|Applied)', r'Applicants?[:\s]+(\d+\+?)']
            for pattern in patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    return f"{match.group(1).strip()} applicants"
            return "Not specified"
        except:
            return "Not specified"

    def _extract_employment_type(self, card) -> str:
        """Extract employment type"""
        try:
            text = card.text.lower()
            if any(word in text for word in ['internship', 'intern ']):
                return 'Internship'
            elif any(word in text for word in ['part-time', 'part time']):
                return 'Part-time'
            elif any(word in text for word in ['contract']):
                return 'Contract'
            location = self._safe_extract(card, self.SELECTORS['location'])
            if 'hybrid' in text or 'Hybrid' in location:
                return 'Hybrid'
            elif 'remote' in text or 'work from home' in text:
                return 'Remote'
            return 'Full-time'
        except:
            return 'Full-time'

    def _extract_seniority_level(self, card) -> str:
        """Extract seniority level"""
        try:
            text = card.text.lower()
            exp_match = re.search(r'(\d+)\s*-\s*(\d+)', text)
            if exp_match:
                min_exp = int(exp_match.group(1))
                if min_exp == 0:
                    return 'Entry level'
                elif min_exp <= 2:
                    return 'Associate'
                elif min_exp <= 5:
                    return 'Mid-Senior level'
                else:
                    return 'Senior level'
            if any(word in text for word in ['fresher', 'graduate', 'entry']):
                return 'Entry level'
            elif any(word in text for word in ['senior', 'lead', 'principal']):
                return 'Senior level'
            elif any(word in text for word in ['manager', 'director', 'head']):
                return 'Director'
            return 'Associate'
        except:
            return 'Associate'

    def _extract_industries(self, company: str, title: str) -> str:
        """Extract industries"""
        text = f"{company} {title}".lower()
        industries = {
            'Software Development': ['software', 'developer', 'programming'],
            'Data & Analytics': ['data', 'analytics', 'scientist', 'ai', 'ml'],
            'IT Services': ['it', 'information technology'],
            'Finance': ['finance', 'banking', 'fintech'],
            'Healthcare': ['health', 'medical'],
            'E-commerce': ['ecommerce', 'e-commerce'],
        }
        for industry, keywords in industries.items():
            if any(keyword in text for keyword in keywords):
                return industry
        return 'Information Technology'

    def _extract_job_function(self, title: str) -> str:
        """Extract job function"""
        title_lower = title.lower()
        functions = {
            'Engineering': ['developer', 'engineer', 'software'],
            'Data Science': ['data scientist', 'data analyst', 'machine learning'],
            'Analytics': ['analyst', 'analytics'],
            'Management': ['manager', 'lead', 'head'],
            'Research': ['scientist', 'research'],
        }
        for function, keywords in functions.items():
            if any(keyword in title_lower for keyword in keywords):
                return function
        return 'Engineering'

    def _extract_job_details(self, card) -> Optional[Dict]:
        """Extract ONLY the 18 required fields - with PERFECT duplicate check"""
        try:
            title = self._safe_extract(card, self.SELECTORS['title'])
            if not title or title == 'N/A':
                return None
            
            company = self._safe_extract(card, self.SELECTORS['company'])
            location = self._safe_extract(card, self.SELECTORS['location'])
            
            job_url = self._safe_extract(card, self.SELECTORS['title'], attribute='href')
            if job_url and not job_url.startswith('http'):
                job_url = 'https://www.naukri.com' + job_url
            
            # ✅ PERFECT DUPLICATE CHECK: URL + Hash
            if self._is_job_already_scraped(title, company, location, job_url):
                print(f"  ⏭️  SKIPPED (duplicate): {title[:50]}...")
                return None
            
            print(f"  🔍 NEW: {title[:50]}...", end=" ")
            
            # Card-level extractions
            employment_type = self._extract_employment_type(card)
            seniority_level = self._extract_seniority_level(card)
            salary_range = self._extract_salary_range(card)
            posted_time = self._extract_posted_time(card)
            applicant_count = self._extract_applicant_count(card)
            company_logo = self._extract_company_logo(card)
            
            # Get complete details from job page
            page_details = self._extract_complete_description(job_url)
            
            # Compute hash (WITHOUT URL)
            job_hash = self._compute_job_hash(title, company, location)
            
            # Return ONLY 18 required fields
            job_data = {
                'title': title.strip(),
                'company': company.strip(),
                'location': location.strip(),
                'posted_time': posted_time,
                'description': page_details['description'],
                'employment_type': employment_type,
                'seniority_level': seniority_level,
                'industries': self._extract_industries(company, title),
                'job_function': self._extract_job_function(title),
                'company_url': f"https://www.naukri.com/companies/{re.sub(r'[^a-z0-9]+', '-', company.lower().strip())}" if company != 'N/A' else 'N/A',
                'company_logo': company_logo,
                'company_about': page_details['company_about'],
                'job_url': job_url,
                'extracted_skills': page_details['extracted_skills'],
                'salary_range': salary_range,
                'applicant_count': applicant_count,
                'job_hash': job_hash
            }
            
            # Add to tracking sets
            if job_url:
                self.existing_urls.add(self._normalize_url(job_url))
            self.existing_hashes.add(job_hash)
            
            print("✅")
            return job_data
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:30]}")
            return None

    def search_jobs(self, keywords: str, location: str, limit: int = 10) -> pd.DataFrame:
        """Search and scrape jobs"""
        self.scraped_jobs = []
        skipped_count = 0
        
        try:
            keywords_clean = re.sub(r'[^a-zA-Z0-9\s]', '', keywords).strip().replace(' ', '-').lower()
            location_clean = re.sub(r'[^a-zA-Z0-9\s]', '', location).strip().replace(' ', '-').lower()
            
            if not location_clean or location_clean == 'pan-india':
                url = f"https://www.naukri.com/{keywords_clean}-jobs"
            else:
                url = f"https://www.naukri.com/{keywords_clean}-jobs-in-{location_clean}"
            
            print(f"\n🔎 Searching: {keywords} in {location}")
            print(f"🌐 URL: {url}")
            
            self.driver.get(url)
            self._human_delay(4, 6)
            print("⏳ Waiting for jobs to load...")
            
            job_cards = []
            for selector in self.SELECTORS['job_cards']:
                try:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if cards:
                        job_cards = cards
                        print(f"📊 Found {len(cards)} job cards")
                        break
                except:
                    continue
            
            if not job_cards:
                print("❌ No job cards found")
                return pd.DataFrame()
            
            # Process cards until we get 'limit' NEW jobs
            for i, card in enumerate(job_cards):
                if len(self.scraped_jobs) >= limit:
                    break
                    
                print(f"\n📋 Card {i+1}/{len(job_cards)}")
                job_data = self._extract_job_details(card)
                
                if job_data:
                    job_data['search_keywords'] = keywords
                    job_data['search_location'] = location
                    self.scraped_jobs.append(job_data)
                else:
                    skipped_count += 1
                
                self._human_delay(2, 3)
            
            print(f"\n📊 Summary: {len(self.scraped_jobs)} NEW, {skipped_count} duplicates skipped")
            
            return self._create_dataframe()
            
        except Exception as e:
            print(f"\n❌ Scraping error: {e}")
            return pd.DataFrame()

    def _create_dataframe(self) -> pd.DataFrame:
        """Create DataFrame"""
        if not self.scraped_jobs:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.scraped_jobs)
        
        print(f"\n{'='*70}")
        print(f"🎉 SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print(f"✅ Total NEW jobs scraped: {len(df)}")
        
        if len(df) > 0:
            print(f"\n📊 DATA QUALITY:")
            print(f"   📄 Jobs with description: {sum(df['description'] != 'Not available')}/{len(df)}")
            print(f"   🏢 Jobs with company info: {sum(df['company_about'] != 'Company information not available')}/{len(df)}")
            print(f"   🔧 Jobs with skills: {sum(df['extracted_skills'] != 'Skills not specified')}/{len(df)}")
            
            sample = df.iloc[0]
            print(f"\n🔬 SAMPLE JOB:")
            print(f"   🏢 Company: {sample['company']}")
            print(f"   💼 Title: {sample['title']}")
            print(f"   📍 Location: {sample['location']}")
            print(f"   💰 Salary: {sample['salary_range']}")
        
        return df

    def close(self):
        """Close browser"""
        if self.driver:
            try:
                self.driver.quit()
                print("\n🔒 Browser closed")
            except:
                pass


if __name__ == "__main__":
    print("This script is meant to be imported as a module.")
    print("Use the Flask API endpoints to run the scraper.")