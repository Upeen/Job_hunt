import asyncio
import re
import httpx
import pandas as pd
import streamlit as st
from typing import List, Optional, Dict, Callable, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, HttpUrl, Field
from dateutil import parser
from bs4 import BeautifulSoup
import logging
import time
import json
from urllib.parse import quote_plus
from functools import lru_cache
import hashlib

# ==================== CONFIGURATION & SETUP ====================

# Logging Setup
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Set to INFO for job scraping logs

# Set page config
st.set_page_config(layout="wide", page_title="🚀 Advanced Data Jobs Aggregator", page_icon="🚀")

# Cache TTLs (in seconds)
CACHE_TTL_JOB_DATA = 3600  # 1 hour for job data
CACHE_TTL_FILTERED_DATA = 300  # 5 minutes for filtered views

# Concurrent Request Limits
MAX_CONCURRENT_REQUESTS = 25  # Increased for more sources
MAX_RESULTS_PER_PORTAL = 50   # Hard limit

# ==================== DATA MODELS ====================

class Job(BaseModel):
    id: str
    title: str
    company: str
    location: Optional[str] = None
    remote: bool = False
    portals: List[str] = Field(default_factory=list)
    date_posted: Optional[datetime] = None
    apply_url: Optional[HttpUrl] = None
    description_snippet: Optional[str] = None
    skills: List[str] = Field(default_factory=list)
    source: Optional[str] = None
    job_type: Optional[str] = None
    salary: Optional[str] = None
    company_rating: Optional[str] = None
    experience: Optional[str] = None
    benefits: Optional[str] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None
    profile: Optional[str] = None
    education: Optional[str] = None
    job_level: Optional[str] = None
    application_deadline: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = True

# Expanded Skills List
SKILLS = [
    "Python", "SQL", "R", "Spark", "ML", "AI", "Tableau", "Power BI", "TensorFlow", "PyTorch", "AWS", "GCP",
    "Machine Learning", "Big Data", "Hadoop", "Kafka", "Docker", "Kubernetes", "Excel", "Statistics", "Deep Learning",
    "NLP", "Computer Vision", "Scikit-learn", "Pandas", "NumPy", "Data Visualization", "ETL", "Airflow", "Git", "Linux",
    "Java", "Scala", "NoSQL", "MongoDB", "PostgreSQL", "Azure", "Data Warehousing", "Feature Engineering",
    "GenAI", "LLM", "MLOps", "Databricks", "Snowflake", "Redshift", "Elasticsearch", "Terraform", "CI/CD", "Jenkins",
    "Business Intelligence", "Data Modeling", "Data Pipeline", "Data Governance", "Data Quality", "Data Mining",
    "Predictive Analytics", "Statistical Analysis", "A/B Testing", "Dashboard", "Reporting", "Data Architecture",
    "Hive", "Pig", "Cassandra", "Redis", "Neo4j", "SAS", "SPSS", "MATLAB", "Jupyter", "Flask", "Django", "FastAPI",
    "React", "Node.js", "JavaScript", "TypeScript", "HTML", "CSS", "REST API", "GraphQL", "Microservices", "Agile",
    "Scrum", "Kanban", "JIRA", "Confluence", "DataOps", "FinTech", "HealthTech", "EdTech", "Retail Analytics"
]

# Profile Keywords Mapping
PROFILE_KEYWORDS = {
    "Data Scientist": ["data scientist", "machine learning engineer", "ml engineer", "ai engineer", "nlp", "computer vision", "research scientist"],
    "Data Analyst": ["data analyst", "business analyst", "reporting analyst", "analytics", "bi analyst", "financial analyst"],
    "Data Engineer": ["data engineer", "etl developer", "data pipeline", "data architect", "warehouse", "big data engineer"],
    "BI Developer": ["bi developer", "business intelligence", "tableau", "power bi", "qlik", "looker", "dashboard", "reporting"],
    "ML Engineer": ["ml engineer", "machine learning engineer", "mlops", "model deployment", "ai infrastructure"],
    "Data Architect": ["data architect", "data modeling", "data strategy", "database architect"],
    "Big Data Engineer": ["big data", "hadoop", "spark", "kafka", "flink", "data lake"],
    "AI Researcher": ["ai research", "research scientist", "deep learning researcher", "ai scientist"],
    "Data Product Manager": ["data product manager", "product manager data", "analytics product", "data pm"],
    "Data Science Manager": ["data science manager", "manager data science", "head of data", "director of data"],
    "Quantitative Analyst": ["quantitative analyst", "quant analyst", "quant researcher", "quant developer"],
    "Business Intelligence Analyst": ["business intelligence analyst", "bi analyst", "bi specialist"],
    "Data Visualization Specialist": ["data visualization", "viz specialist", "tableau developer", "power bi developer"],
    "All Data Roles": ["data", "analyst", "scientist", "engineer", "bi", "machine learning", "ai", "python", "sql"]
}

# Job Type Keywords
JOB_TYPE_KEYWORDS = {
    "Full-time": ["full time", "full-time", "permanent", "regular"],
    "Part-time": ["part time", "part-time", "flexible hours"],
    "Contract": ["contract", "freelance", "consultant", "consulting", "temporary"],
    "Internship": ["intern", "internship", "trainee", "fellowship", "apprentice"]
}

# Job Level Keywords
JOB_LEVEL_KEYWORDS = {
    "Entry Level": ["entry level", "fresher", "junior", "0-2 years", "0-3 years", "recent graduate"],
    "Mid Level": ["mid level", "experienced", "2-5 years", "3-5 years", "senior", "lead"],
    "Senior Level": ["senior", "principal", "staff", "5+ years", "8+ years", "expert", "architect"],
    "Executive": ["director", "vp", "vice president", "head of", "chief", "cto", "cio", "manager", "management"]
}

# ==================== CORE FUNCTIONS ====================

def tag_job(job: Job) -> Job:
    """Tags a job with skills, profile, job type, and level based on its title and description."""
    text = f"{job.title} {job.description_snippet or ''}".lower()

    # Tag Skills
    job.skills = [s for s in SKILLS if re.search(rf"\b{re.escape(s.lower())}\b", text)]

    # Tag Profile
    for profile, keywords in PROFILE_KEYWORDS.items():
        if profile == "All Data Roles":
            continue
        if any(re.search(r'\b' + re.escape(kw) + r'\b', text) for kw in keywords):
            job.profile = profile
            break
    if not job.profile:
        job.profile = "Other Data Role"

    # Tag Job Type (if not already set)
    if not job.job_type:
        for job_type, keywords in JOB_TYPE_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                job.job_type = job_type
                break
        if not job.job_type:
            job.job_type = "Full-time"

    # Tag Job Level
    for level, keywords in JOB_LEVEL_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            job.job_level = level
            break
    if not job.job_level:
        job.job_level = "Not Specified"

    return job

async def fetch_with_retry(client: httpx.AsyncClient, url: str, retries: int = 3, timeout: int = 15, headers: Optional[Dict] = None) -> Optional[httpx.Response]:
    """Fetches a URL with retries and exponential backoff."""
    if headers is None:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for i in range(retries):
        try:
            r = await client.get(url, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.warning(f"Attempt {i+1} failed for {url}: {e}")
            if i == retries - 1:
                logger.error(f"Failed to fetch {url} after {retries} attempts: {e}")
                return None
            await asyncio.sleep(0.5 * (2 ** i))  # Exponential backoff: 0.5s, 1s, 2s
    return None

# ==================== SCRAPING FUNCTIONS ====================

# --- API-based Job Boards ---
GREENHOUSE_COMPANIES = [
    "openai", "databricks", "stripe", "linkedin", "reddit", "asana", "robinhood", "coinbase", "twilio", "zoom",
    "snowflake", "palantir", "uber", "lyft", "doordash", "instacart", "flipkart", "paytm", "zomato", "phonepe",
    "postman", "myntra", "innovaccer", "policybazaar", "ola", "sigmoid", "inmobi", "metropolis", "udacity", "nice",
    "zoho", "freshworks", "razorpay", "cred", "sharechat", "meesho", "dunzo", "byjus", "unacademy", "upgrad"
]

async def greenhouse_fetch(client: httpx.AsyncClient, profile_keywords: Dict[str, List[str]]) -> List[Job]:
    jobs = []
    semaphore = asyncio.Semaphore(5)

    async def fetch_company(company: str):
        async with semaphore:
            url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
            r = await fetch_with_retry(client, url)
            if r:
                try:
                    data = r.json()
                    for j in data.get("jobs", [])[:MAX_RESULTS_PER_PORTAL]:
                        title = j.get("title", "").lower()
                        if not any(kw in title for kw_list in profile_keywords.values() for kw in kw_list):
                            continue
                        loc = (j.get("location") or {}).get("name", "").lower()
                        if "india" not in loc and not loc.startswith("remote"):
                            continue
                        jobs.append(Job(
                            id=f"gh:{company}:{j['id']}",
                            title=j.get("title", ""),
                            company=company.title(),
                            location=loc.title(),
                            remote="remote" in loc,
                            portals=["Greenhouse"],
                            apply_url=j.get("absolute_url"),
                            description_snippet=(j.get("content") or "")[:300],
                            date_posted=parser.parse(j.get("updated_at")) if j.get("updated_at") else None,
                            source="Greenhouse",
                            job_type=j.get("metadata", [{}])[0].get("value", "Full-time") if j.get("metadata") else "Full-time"
                        ))
                except Exception as e:
                    logger.error(f"Greenhouse parsing error for {company}: {e}")

    tasks = [fetch_company(c) for c in GREENHOUSE_COMPANIES]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs

LEVER_COMPANIES = [
    "goto", "gohighlevel", "aidash", "zeta", "eudia", "dnb", "paytm", "octopusenergy",
    "netflix", "upstart", "highlevel", "brex", "scale", "plaid", "ramp", "coda", "notion",
    "zerodha", "groww", "phonepe", "cred", "meesho", "sharechat", "dunzo", "byjus", "unacademy"
]

async def lever_fetch(client: httpx.AsyncClient, profile_keywords: Dict[str, List[str]]) -> List[Job]:
    jobs = []
    semaphore = asyncio.Semaphore(5)

    async def fetch_company(company: str):
        async with semaphore:
            url = f"https://jobs.lever.co/{company}"
            r = await fetch_with_retry(client, url)
            if r:
                soup = BeautifulSoup(r.text, "html.parser")
                postings = soup.find_all("div", class_="posting")[:MAX_RESULTS_PER_PORTAL]
                for p in postings:
                    title_el = p.find("h5")
                    if not title_el: continue
                    title = title_el.text.strip().lower()
                    if not any(kw in title for kw_list in profile_keywords.values() for kw in kw_list):
                        continue
                    loc_el = p.find("span", class_="sort-by-location")
                    loc = loc_el.text.strip().lower() if loc_el else ""
                    if "india" not in loc and "remote" not in loc: continue
                    apply_el = p.find("a", class_="posting-btn-submit")
                    apply_url = apply_el["href"] if apply_el else None
                    commit_el = p.find("span", class_="sort-by-commitment")
                    job_type = commit_el.text.strip() if commit_el else "Full-time"
                    jobs.append(Job(
                        id=f"lever:{company}:{hashlib.md5(title.encode()).hexdigest()[:8]}",
                        title=title.title(),
                        company=company.title(),
                        location=loc.title(),
                        remote="remote" in loc,
                        portals=["Lever"],
                        apply_url=apply_url,
                        source="Lever",
                        job_type=job_type
                    ))

    tasks = [fetch_company(c) for c in LEVER_COMPANIES]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs

async def remoteok_fetch(client: httpx.AsyncClient, profile_keywords: Dict[str, List[str]]) -> List[Job]:
    jobs = []
    r = await fetch_with_retry(client, "https://remoteok.com/api?tag=data")
    if r:
        try:
            data = r.json()
            for j in data[1:MAX_RESULTS_PER_PORTAL+1]:
                title = j.get("position", "").lower()
                if not any(kw in title for kw_list in profile_keywords.values() for kw in kw_list):
                    continue
                loc = j.get("location", "").lower()
                if "india" not in loc and "anywhere" not in loc: continue
                jobs.append(Job(
                    id=f"remoteok:{j.get('id')}",
                    title=j.get("position", ""),
                    company=j.get("company", ""),
                    location=loc.title(),
                    remote=True,
                    portals=["RemoteOK"],
                    apply_url=j.get("url"),
                    description_snippet=j.get("description", "")[:300],
                    date_posted=parser.parse(j.get("date")) if j.get("date") else None,
                    source="RemoteOK",
                    job_type=j.get("type", "Full-time"),
                    salary=j.get("salary")
                ))
        except Exception as e:
            logger.error(f"RemoteOK parsing error: {e}")
    return jobs

async def startup_jobs_fetch(client: httpx.AsyncClient, profile_keywords: Dict[str, List[str]]) -> List[Job]:
    jobs = []
    r = await fetch_with_retry(client, "https://www.startup.jobs/api/v1/jobs?limit=100&tags=data")
    if r:
        try:
            data = r.json()
            for j in data.get("jobs", [])[:MAX_RESULTS_PER_PORTAL]:
                title = j.get("title", "").lower()
                if not any(kw in title for kw_list in profile_keywords.values() for kw in kw_list):
                    continue
                loc = j.get("location", "").lower()
                if "india" not in loc and "remote" not in loc: continue
                jobs.append(Job(
                    id=f"startupjobs:{j.get('id')}",
                    title=j.get("title", ""),
                    company=j.get("company", {}).get("name", ""),
                    location=loc.title(),
                    remote="remote" in loc or j.get("is_remote", False),
                    portals=["Startup.jobs"],
                    apply_url=j.get("apply_url"),
                    description_snippet=j.get("description", "")[:300],
                    date_posted=parser.parse(j.get("created_at")) if j.get("created_at") else None,
                    source="Startup.jobs"
                ))
        except Exception as e:
            logger.error(f"Startup.jobs parsing error: {e}")
    return jobs

async def wellfound_fetch(client: httpx.AsyncClient, profile_keywords: Dict[str, List[str]]) -> List[Job]:
    jobs = []
    r = await fetch_with_retry(client, "https://angel.co/jobs")
    if r:
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            script_tag = soup.find("script", {"id": "react-data"})
            if script_tag:
                data = json.loads(script_tag.string)
                listings = data.get("jobListings", {}).get("listings", [])[:MAX_RESULTS_PER_PORTAL]
                for j in listings:
                    title = j.get("title", "").lower()
                    if not any(kw in title for kw_list in profile_keywords.values() for kw in kw_list):
                        continue
                    loc = (j.get("location", {}) or {}).get("name", "").lower()
                    if "india" not in loc and not j.get("isRemote", False): continue
                    jobs.append(Job(
                        id=f"wellfound:{j.get('id')}",
                        title=j.get("title", ""),
                        company=j.get("company", {}).get("name", ""),
                        location=loc.title(),
                        remote=j.get("isRemote", False),
                        portals=["WellFound"],
                        apply_url=j.get("url"),
                        description_snippet=j.get("description", "")[:300],
                        date_posted=parser.parse(j.get("createdAt")) if j.get("createdAt") else None,
                        source="WellFound",
                    ))
        except Exception as e:
            logger.error(f"WellFound (Angel.co) parsing error: {e}")
    return jobs

# --- Indian Job Portals ---
async def naukri_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")
    semaphore = asyncio.Semaphore(3)

    async def fetch_page(page: int):
        async with semaphore:
            url = f"https://www.naukri.com/{keyword}-jobs-in-india-{page}"
            r = await fetch_with_retry(client, url)
            if r:
                soup = BeautifulSoup(r.text, "html.parser")
                for c in soup.select(".cust-job-tuple")[:20]:
                    title_el = c.select_one(".title")
                    company_el = c.select_one(".comp-name")
                    loc_el = c.select_one(".locWdth")
                    salary_el = c.select_one(".sal")
                    exp_el = c.select_one(".expwdth")
                    job_type_el = c.select_one(".jobType")
                    benefits_el = c.select_one(".benefits")
                    if not title_el: continue
                    jobs.append(Job(
                        id=f"naukri:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                        title=title_el.get_text(strip=True),
                        company=company_el.get_text(strip=True) if company_el else "",
                        location=loc_el.get_text(strip=True) if loc_el else "",
                        portals=["Naukri"],
                        apply_url=title_el.get("href"),
                        source="Naukri",
                        job_type=job_type_el.get_text(strip=True) if job_type_el else "Full-time",
                        salary=salary_el.get_text(strip=True) if salary_el else None,
                        experience=exp_el.get_text(strip=True) if exp_el else None,
                        benefits=benefits_el.get_text(strip=True) if benefits_el else None
                    ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def indeed_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data+scientist", "Data Analyst": "data+analyst", "Data Engineer": "data+engineer",
        "BI Developer": "bi+developer", "ML Engineer": "machine+learning+engineer", "Data Architect": "data+architect",
        "Big Data Engineer": "big+data+engineer", "AI Researcher": "ai+researcher", "Data Product Manager": "data+product+manager",
        "Data Science Manager": "data+science+manager", "Quantitative Analyst": "quantitative+analyst",
        "Business Intelligence Analyst": "business+intelligence+analyst", "Data Visualization Specialist": "data+visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://in.indeed.com/jobs?q={keyword}&l=India&start={page*10}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".job_seen_beacon")[:20]:
                title_el = c.select_one("h2 a")
                company_el = c.select_one(".companyName")
                loc_el = c.select_one(".companyLocation")
                salary_el = c.select_one(".salary-snippet")
                desc_el = c.select_one(".job-snippet")
                if not title_el: continue
                jobs.append(Job(
                    id=f"indeed:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Indeed"],
                    apply_url=f"https://in.indeed.com{title_el['href']}" if title_el.get("href") else None,
                    source="Indeed",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    description_snippet=desc_el.get_text(strip=True)[:300] if desc_el else None
                ))

    tasks = [fetch_page(page) for page in range(0, 2)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def glassdoor_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "ml-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    for page in range(1, 3):
        url = f"https://www.glassdoor.co.in/Job/india-{keyword}-jobs-SRCH_IL.0,5_IN115_KO6,{6+len(keyword)}_IP{page}.htm"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select("li[data-brandviews]")[:20]:
                title_el = c.select_one("[data-test='job-title']")
                company_el = c.select_one("[data-test='employer-name']")
                loc_el = c.select_one("[data-test='job-location']")
                salary_el = c.select_one("[data-test='detailSalary']")
                rating_el = c.select_one("[data-test='detailRating']")
                if not title_el: continue
                jobs.append(Job(
                    id=f"glassdoor:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Glassdoor"],
                    apply_url=f"https://www.glassdoor.co.in{title_el['href']}" if title_el.get("href") else None,
                    source="Glassdoor",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    company_rating=rating_el.get_text(strip=True) if rating_el else None
                ))
        await asyncio.sleep(1)

    return jobs[:MAX_RESULTS_PER_PORTAL]

async def linkedin_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data%20scientist", "Data Analyst": "data%20analyst", "Data Engineer": "data%20engineer",
        "BI Developer": "bi%20developer", "ML Engineer": "machine%20learning%20engineer", "Data Architect": "data%20architect",
        "Big Data Engineer": "big%20data%20engineer", "AI Researcher": "ai%20researcher", "Data Product Manager": "data%20product%20manager",
        "Data Science Manager": "data%20science%20manager", "Quantitative Analyst": "quantitative%20analyst",
        "Business Intelligence Analyst": "business%20intelligence%20analyst", "Data Visualization Specialist": "data%20visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.linkedin.com/jobs/search?keywords={keyword}&location=India&f_WT=2&start={page*25}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".base-card")[:20]:
                title_el = c.select_one(".base-search-card__title")
                company_el = c.select_one(".base-search-card__subtitle")
                loc_el = c.select_one(".job-search-card__location")
                salary_el = c.select_one(".job-search-card__salary-info")
                link_el = c.select_one("a")
                if not title_el: continue
                jobs.append(Job(
                    id=f"linkedin:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["LinkedIn"],
                    apply_url=link_el["href"] if link_el else None,
                    source="LinkedIn",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None
                ))

    tasks = [fetch_page(page) for page in range(0, 2)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def shine_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.shine.com/job-search/{keyword}-jobs-in-india?page={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".jobCard")[:20]:
                title_el = c.select_one(".jobCard_jobTitle__w8bju")
                company_el = c.select_one(".jobCard_jobCompany__Nmxzt")
                loc_el = c.select_one(".jobCard_location__z5zox")
                exp_el = c.select_one(".jobCard_exp__fL8uN")
                salary_el = c.select_one(".jobCard_salary__jYDvJ")
                link_el = c.select_one("a")
                if not title_el: continue
                jobs.append(Job(
                    id=f"shine:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Shine.com"],
                    apply_url=f"https://www.shine.com{link_el['href']}" if link_el else None,
                    source="Shine.com",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def foundit_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.foundit.in/srp/results?query={keyword}&location=india&pageNum={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".card-apply")[:20]:
                title_el = c.select_one(".title")
                company_el = c.select_one(".company")
                loc_el = c.select_one(".location")
                exp_el = c.select_one(".experience")
                salary_el = c.select_one(".salary")
                link_el = c.select_one("a")
                if not title_el: continue
                jobs.append(Job(
                    id=f"foundit:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Foundit"],
                    apply_url=f"https://www.foundit.in{link_el['href']}" if link_el else None,
                    source="Foundit",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def timesjobs_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&txtKeywords={keyword}&txtLocation=india&page={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".clearfix.job-bx")[:20]:
                title_el = c.select_one("h2 a")
                company_el = c.select_one(".joblist-comp-name")
                loc_el = c.select_one(".top-jd-dtl li span")
                exp_el = c.select_one(".top-jd-dtl li:nth-child(2) span")
                salary_el = c.select_one(".top-jd-dtl li:nth-child(3) span")
                desc_el = c.select_one(".list-job-dtl li")
                if not title_el: continue
                jobs.append(Job(
                    id=f"timesjobs:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True).replace('(More Jobs)', '').strip() if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["TimesJobs"],
                    apply_url=title_el.get("href"),
                    source="TimesJobs",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None,
                    description_snippet=desc_el.get_text(strip=True)[:300] if desc_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def hirist_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.hirist.com/jobs/{keyword}-jobs-{page}.html"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".job-card")[:20]:
                title_el = c.select_one(".job-title a")
                company_el = c.select_one(".company-name")
                loc_el = c.select_one(".location")
                exp_el = c.select_one(".exp")
                salary_el = c.select_one(".salary")
                skills_el = c.select(".skill-tag")
                if not title_el: continue
                jobs.append(Job(
                    id=f"hirist:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Hirist"],
                    apply_url=title_el.get("href"),
                    source="Hirist",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None,
                    skills=[skill.get_text(strip=True) for skill in skills_el] if skills_el else []
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def iimjobs_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.iimjobs.com/search/{keyword}-{page}.html"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".job-tuple")[:20]:
                title_el = c.select_one(".job-heading a")
                company_el = c.select_one(".company-name")
                loc_el = c.select_one(".location")
                exp_el = c.select_one(".exp")
                salary_el = c.select_one(".salary")
                if not title_el: continue
                jobs.append(Job(
                    id=f"iimjobs:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["IIMJobs"],
                    apply_url=title_el.get("href"),
                    source="IIMJobs",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def cutshort_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://cutshort.io/jobs?search={keyword}&location=india&page={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".card")[:20]:
                title_el = c.select_one(".card-title a")
                company_el = c.select_one(".company-name")
                loc_el = c.select_one(".location")
                skills_el = c.select(".skill-tag")
                if not title_el: continue
                jobs.append(Job(
                    id=f"cutshort:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["CutShort"],
                    apply_url=title_el.get("href"),
                    source="CutShort",
                    job_type="Full-time",
                    skills=[skill.get_text(strip=True) for skill in skills_el] if skills_el else []
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

# --- Additional Job Portals ---
async def simplyhired_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.simplyhired.co.in/search?q={keyword}&l=india&pn={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".SerpJob")[:20]:
                title_el = c.select_one(".jobposting-title a")
                company_el = c.select_one(".jobposting-company")
                loc_el = c.select_one(".jobposting-location")
                salary_el = c.select_one(".jobposting-salary")
                desc_el = c.select_one(".jobposting-snippet")
                if not title_el: continue
                jobs.append(Job(
                    id=f"simplyhired:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["SimplyHired"],
                    apply_url=f"https://www.simplyhired.co.in{title_el['href']}" if title_el.get("href") else None,
                    source="SimplyHired",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    description_snippet=desc_el.get_text(strip=True)[:300] if desc_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def dice_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.dice.com/jobs?q={keyword}&location=India&page={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".card")[:20]:
                title_el = c.select_one(".card-title-link")
                company_el = c.select_one(".card-company")
                loc_el = c.select_one(".card-location")
                desc_el = c.select_one(".card-description")
                if not title_el: continue
                jobs.append(Job(
                    id=f"dice:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Dice"],
                    apply_url=title_el.get("href"),
                    source="Dice",
                    job_type="Full-time",
                    description_snippet=desc_el.get_text(strip=True)[:300] if desc_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def stackoverflow_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://stackoverflow.com/jobs?q={keyword}&l=india&d=20&u=Km&pg={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".js-result")[:20]:
                title_el = c.select_one(".s-link")
                company_el = c.select_one(".fc-black-700")
                loc_el = c.select_one(".fc-black-500")
                salary_el = c.select_one(".salary")
                if not title_el: continue
                jobs.append(Job(
                    id=f"stackoverflow:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True).split('•')[0].strip() if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["StackOverflow"],
                    apply_url=f"https://stackoverflow.com{title_el['href']}" if title_el.get("href") else None,
                    source="StackOverflow",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def monster_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.monsterindia.com/srp/results?query={keyword}&locations=india&page={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".card-apply")[:20]:
                title_el = c.select_one(".title")
                company_el = c.select_one(".company")
                loc_el = c.select_one(".location")
                exp_el = c.select_one(".experience")
                salary_el = c.select_one(".salary")
                link_el = c.select_one("a")
                if not title_el: continue
                jobs.append(Job(
                    id=f"monster:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True) if company_el else "",
                    location=loc_el.get_text(strip=True) if loc_el else "",
                    portals=["Monster"],
                    apply_url=link_el.get("href") if link_el else None,
                    source="Monster",
                    job_type="Full-time",
                    salary=salary_el.get_text(strip=True) if salary_el else None,
                    experience=exp_el.get_text(strip=True) if exp_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

async def careerbuilder_fetch(client: httpx.AsyncClient, profile: str) -> List[Job]:
    jobs = []
    keyword_map = {
        "Data Scientist": "data-scientist", "Data Analyst": "data-analyst", "Data Engineer": "data-engineer",
        "BI Developer": "bi-developer", "ML Engineer": "machine-learning-engineer", "Data Architect": "data-architect",
        "Big Data Engineer": "big-data-engineer", "AI Researcher": "ai-researcher", "Data Product Manager": "data-product-manager",
        "Data Science Manager": "data-science-manager", "Quantitative Analyst": "quantitative-analyst",
        "Business Intelligence Analyst": "business-intelligence-analyst", "Data Visualization Specialist": "data-visualization",
        "All Data Roles": "data"
    }
    keyword = keyword_map.get(profile, "data")

    async def fetch_page(page: int):
        url = f"https://www.careerbuilder.com/jobs?keywords={keyword}&location=india&page_number={page}"
        r = await fetch_with_retry(client, url)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for c in soup.select(".data-results-content-parent")[:20]:
                title_el = c.select_one(".data-results-title")
                company_el = c.select_one(".data-details")
                loc_el = c.select_one(".data-details")
                desc_el = c.select_one(".data-snapshot")
                if not title_el: continue
                jobs.append(Job(
                    id=f"careerbuilder:{hashlib.md5((title_el.get_text(strip=True) + (company_el.get_text(strip=True) if company_el else '')).encode()).hexdigest()}",
                    title=title_el.get_text(strip=True),
                    company=company_el.get_text(strip=True).split('-')[0].strip() if company_el else "",
                    location=loc_el.get_text(strip=True).split('-')[-1].strip() if loc_el else "",
                    portals=["CareerBuilder"],
                    apply_url=title_el.get("href"),
                    source="CareerBuilder",
                    job_type="Full-time",
                    description_snippet=desc_el.get_text(strip=True)[:300] if desc_el else None
                ))

    tasks = [fetch_page(page) for page in range(1, 3)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jobs[:MAX_RESULTS_PER_PORTAL]

# ==================== JOB COLLECTION ENGINE ====================

# Define the fetcher map
FETCHER_MAP: Dict[str, Callable] = {
    "Greenhouse": lambda client, profile: greenhouse_fetch(client, PROFILE_KEYWORDS),
    "Lever": lambda client, profile: lever_fetch(client, PROFILE_KEYWORDS),
    "RemoteOK": lambda client, profile: remoteok_fetch(client, PROFILE_KEYWORDS),
    "Startup.jobs": lambda client, profile: startup_jobs_fetch(client, PROFILE_KEYWORDS),
    "WellFound": lambda client, profile: wellfound_fetch(client, PROFILE_KEYWORDS),
    "Naukri": lambda client, profile: naukri_fetch(client, profile),
    "Indeed": lambda client, profile: indeed_fetch(client, profile),
    "Glassdoor": lambda client, profile: glassdoor_fetch(client, profile),
    "LinkedIn": lambda client, profile: linkedin_fetch(client, profile),
    "Shine.com": lambda client, profile: shine_fetch(client, profile),
    "Foundit": lambda client, profile: foundit_fetch(client, profile),
    "TimesJobs": lambda client, profile: timesjobs_fetch(client, profile),
    "Hirist": lambda client, profile: hirist_fetch(client, profile),
    "IIMJobs": lambda client, profile: iimjobs_fetch(client, profile),
    "CutShort": lambda client, profile: cutshort_fetch(client, profile),
    "SimplyHired": lambda client, profile: simplyhired_fetch(client, profile),
    "Dice": lambda client, profile: dice_fetch(client, profile),
    "StackOverflow": lambda client, profile: stackoverflow_fetch(client, profile),
    "Monster": lambda client, profile: monster_fetch(client, profile),
    "CareerBuilder": lambda client, profile: careerbuilder_fetch(client, profile),
}

@st.cache_data(ttl=CACHE_TTL_JOB_DATA, show_spinner=False)
def collect_jobs_cached(source_filter: tuple, profile: str) -> List[Dict[str, Any]]:
    """Cached wrapper to collect jobs. Returns a list of dicts for Streamlit compatibility."""
    logger.info(f"Cache MISS: Fetching jobs for profile '{profile}' from sources {source_filter}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        jobs = loop.run_until_complete(async_collect_jobs(list(source_filter), profile))
        # Convert Job objects to dicts for caching
        return [job.model_dump() for job in jobs]
    finally:
        loop.close()

async def async_collect_jobs(source_filter: List[str], profile: str) -> List[Job]:
    """The main async function that orchestrates the job collection from selected sources."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    # Create a client with connection limits
    async with httpx.AsyncClient(
        headers=headers,
        timeout=20,
        limits=httpx.Limits(max_connections=MAX_CONCURRENT_REQUESTS, max_keepalive_connections=10),
        follow_redirects=True
    ) as client:

        # Create tasks for selected sources
        tasks = []
        for src in source_filter:
            if src in FETCHER_MAP:
                # For API-based sources, we pass the profile_keywords dict.
                # For others, we pass the profile string.
                if src in ["Greenhouse", "Lever", "RemoteOK", "Startup.jobs", "WellFound"]:
                    task = FETCHER_MAP[src](client, PROFILE_KEYWORDS)
                else:
                    task = FETCHER_MAP[src](client, profile)
                tasks.append(task)

        # Gather results with a semaphore if needed (already implemented in individual fetchers)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    jobs: List[Job] = []
    for idx, r in enumerate(results):
        src_name = source_filter[idx] if idx < len(source_filter) else "Unknown"
        if isinstance(r, list):
            jobs.extend(r)
            logger.info(f"{src_name}: Fetched {len(r)} jobs.")
        elif isinstance(r, Exception):
            logger.error(f"{src_name}: Fetch error: {r}")

    # Deduplicate by ID
    seen_ids = set()
    unique_jobs = []
    for job in jobs:
        if job.id not in seen_ids:
            seen_ids.add(job.id)
            unique_jobs.append(tag_job(job)) # Tag after deduplication
        else:
            logger.debug(f"Duplicate job removed: {job.id}")

    logger.info(f"Total unique jobs collected: {len(unique_jobs)}")
    return unique_jobs

# ==================== STREAMLIT UI ====================

def main():
    st.title("🚀 Advanced Data Jobs Aggregator")
    st.markdown("Find the best data science, analytics, and engineering jobs across India. Results are cached for 1 hour for speed.")

    with st.sidebar:
        st.header("🔍 Search & Filters")

        # Profile Selection
        profile = st.selectbox(
            "Job Profile",
            options=list(PROFILE_KEYWORDS.keys()),
            index=0,
            help="Select the type of data role you are looking for."
        )

        # Keyword Filter
        kw_filter = st.text_input(
            "Keyword in Title/Description",
            help="e.g., 'NLP', 'PySpark', 'Tableau', 'Senior'"
        )

        # Company Filter
        company_filter = st.text_input(
            "Company Name Contains",
            help="Filter jobs by company name."
        )

        # Location Filter
        loc_filter = st.text_input(
            "Location Contains",
            value="India" if st.checkbox("Default to India", value=True) else "",
            help="Filter by city or region. Leave blank for all."
        )

        # Remote Filter
        remote_only = st.checkbox("Remote Only", value=False)

        # Job Type Filter
        job_type_filter = st.selectbox(
            "Job Type",
            options=["All", "Full-time", "Part-time", "Contract", "Internship"],
            index=0
        )

        # Experience Filter
        experience_filter = st.text_input(
            "Experience (e.g., '2 years', '5+')",
            help="Keywords to filter the experience field."
        )

        # Salary Filter
        salary_filter = st.text_input(
            "Salary (e.g., '10 LPA', '15-20', 'USD')",
            help="Keywords to filter the salary field."
        )

        # Skills Filter
        skills_filter = st.multiselect(
            "Required Skills",
            options=sorted(SKILLS),
            default=[],
            help="Select one or more skills to filter jobs."
        )

        # Profile Filter (for tagged jobs)
        profile_filter = st.multiselect(
            "Filter by Tagged Profile",
            options=list(PROFILE_KEYWORDS.keys()),
            default=[profile] if profile != "All Data Roles" else [],
            help="Filter jobs based on their auto-tagged profile."
        )

        # Job Level Filter
        job_level_filter = st.multiselect(
            "Job Level",
            options=["All", "Entry Level", "Mid Level", "Senior Level", "Executive"],
            default=["All"],
            help="Filter by job level/experience required."
        )

        # Source Selection
        all_sources = list(FETCHER_MAP.keys())
        default_sources = ["Naukri", "Indeed", "LinkedIn", "Glassdoor", "Hirist", "IIMJobs"]
        source_filter = st.multiselect(
            "Select Job Portals",
            options=all_sources,
            default=default_sources,
            help="Choose which job portals to scrape. More sources = longer fetch time."
        )

        # Sorting
        sort_by = st.selectbox(
            "Sort Results By",
            options=["Relevance", "Date Posted (Newest)", "Date Posted (Oldest)", "Title A-Z", "Title Z-A", "Company A-Z", "Company Z-A"],
            index=0
        )

        # Performance Options
        st.header("⚡ Performance")
        use_caching = st.checkbox("Use Caching (Recommended)", value=True, help="Speeds up repeated searches. Cache refreshes every hour.")
        force_refresh = st.checkbox("Force Refresh (Ignore Cache)", value=False, help="Check this to fetch fresh data, ignoring the cache.")
        st.caption(f"Max {MAX_RESULTS_PER_PORTAL} results per portal. Concurrent requests: {MAX_CONCURRENT_REQUESTS}")

    # Search Button
    if st.button("🔎 Search Jobs", type="primary") or 'jobs_data' in st.session_state:
        if not source_filter:
            st.warning("Please select at least one job portal.")
            return

        # Generate a unique key for caching based on filters
        filter_key = f"{profile}_{','.join(sorted(source_filter))}"

        with st.spinner("Fetching and processing jobs... This may take a minute."):
            start_time = time.time()

            # Fetch raw job data (cached)
            if use_caching and not force_refresh:
                raw_jobs_data = collect_jobs_cached(tuple(source_filter), profile)
            else:
                # Bypass cache
                st.cache_data.clear()
                raw_jobs_data = collect_jobs_cached(tuple(source_filter), profile)

            # Convert back to Job objects for filtering
            jobs = [Job(**job_dict) for job_dict in raw_jobs_data]

            fetch_time = time.time() - start_time
            st.success(f"✅ Fetched {len(jobs)} unique jobs in {fetch_time:.2f} seconds.")

            # Apply Filters (this is fast, no need to cache)
            filtered_jobs = apply_filters(
                jobs,
                kw_filter=kw_filter,
                company_filter=company_filter,
                loc_filter=loc_filter,
                remote_only=remote_only,
                job_type_filter=job_type_filter,
                skills_filter=skills_filter,
                profile_filter=profile_filter,
                experience_filter=experience_filter,
                salary_filter=salary_filter,
                job_level_filter=job_level_filter
            )

            # Apply Sorting
            sorted_jobs = apply_sorting(filtered_jobs, sort_by)

            # Pagination
            total_jobs = len(sorted_jobs)
            jobs_per_page = 50
            total_pages = max(1, (total_jobs + jobs_per_page - 1) // jobs_per_page)

            if total_pages > 1:
                page = st.selectbox("Page", options=range(1, total_pages + 1), index=0)
                start_idx = (page - 1) * jobs_per_page
                end_idx = start_idx + jobs_per_page
                display_jobs = sorted_jobs[start_idx:end_idx]
            else:
                display_jobs = sorted_jobs
                page = 1

            # Display Statistics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Fetched", len(jobs))
            col2.metric("After Filtering", len(filtered_jobs))
            col3.metric("Displaying", len(display_jobs))
            col4.metric("Page", f"{page} of {total_pages}")

            # Display Jobs
            if display_jobs:
                df = create_display_dataframe(display_jobs)

                # Make Apply URL a clickable link
                st.dataframe(
                    df,
                    use_container_width=True,
                    height=600,
                    column_config={
                        "apply_url": st.column_config.LinkColumn(
                            "Apply Link",
                            help="Click to apply for the job",
                            validate="^https?://.*",
                            display_text="Apply Now"
                        ),
                        "skills": st.column_config.ListColumn(
                            "Skills",
                            help="Key skills mentioned in the job"
                        ),
                        "remote": st.column_config.CheckboxColumn(
                            "Remote?",
                            help="Is this a remote position?"
                        ),
                    }
                )

                # Export Button
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name=f'data_jobs_{profile}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                    mime='text/csv',
                )

                # Show detailed view for a selected job
                with st.expander("📄 View Job Description Snippet"):
                    selected_title = st.selectbox("Select a job to see its snippet:", options=[job.title for job in display_jobs])
                    selected_job = next((job for job in display_jobs if job.title == selected_title), None)
                    if selected_job and selected_job.description_snippet:
                        st.markdown(f"**{selected_job.title}** at *{selected_job.company}*")
                        st.write(selected_job.description_snippet)
                    else:
                        st.info("No description snippet available for the selected job.")

                # Analytics Dashboard
                with st.expander("📊 Analytics Dashboard"):
                    st.subheader("Job Market Insights")
                    
                    # Profile distribution
                    profile_counts = df['profile'].value_counts()
                    st.bar_chart(profile_counts)
                    st.caption("Job Distribution by Profile")
                    
                    # Top companies
                    company_counts = df['company'].value_counts().head(10)
                    st.bar_chart(company_counts)
                    st.caption("Top Companies Hiring")
                    
                    # Skills frequency
                    all_skills = [skill for sublist in df['skills'].dropna() for skill in sublist]
                    skills_series = pd.Series(all_skills).value_counts().head(15)
                    st.bar_chart(skills_series)
                    st.caption("Most In-Demand Skills")
                    
                    # Location distribution
                    location_counts = df['location'].value_counts().head(10)
                    st.bar_chart(location_counts)
                    st.caption("Top Job Locations")

            else:
                st.info("No jobs found matching your criteria. Try adjusting your filters or selecting different portals.")

    # Tips Section
    with st.expander("💡 Pro Tips for Better Results"):
        st.markdown("""
        *   **Be Specific:** Use precise keywords like “TensorFlow,” “Apache Airflow,” or “Senior Data Engineer.”
        *   **Combine Filters:** Use “Remote Only” + “Experience: 5+” to find senior remote roles.
        *   **Leverage Skills Filter:** Find jobs that *require* your core skills.
        *   **Try “All Data Roles”:** Cast a wider net if you’re exploring.
        *   **Fewer Portals = Faster:** Start with 3-4 key portals (e.g., Naukri, LinkedIn, Indeed) for quicker results.
        *   **Check “Force Refresh”:** If you think the cache is stale, use this to get the latest listings.
        *   **Use Pagination:** Don’t load all 500+ jobs at once. Use the page selector.
        *   **Explore Analytics:** Check the Analytics Dashboard for market insights.
        """)

    # Footer
    st.markdown("---")
    st.caption("Built with Streamlit, Asyncio, and BeautifulSoup. Data is scraped from public job portals. Please use responsibly.")

def apply_filters(
    jobs: List[Job],
    kw_filter: str,
    company_filter: str,
    loc_filter: str,
    remote_only: bool,
    job_type_filter: str,
    skills_filter: List[str],
    profile_filter: List[str],
    experience_filter: str,
    salary_filter: str,
    job_level_filter: List[str]
) -> List[Job]:
    """Applies all user-defined filters to the list of jobs."""
    filtered = jobs

    if kw_filter:
        kw = kw_filter.lower()
        filtered = [j for j in filtered if kw in j.title.lower() or (j.description_snippet and kw in j.description_snippet.lower())]

    if company_filter:
        cf = company_filter.lower()
        filtered = [j for j in filtered if cf in j.company.lower()]

    if loc_filter:
        lf = loc_filter.lower()
        filtered = [j for j in filtered if j.location and lf in j.location.lower()]

    if remote_only:
        filtered = [j for j in filtered if j.remote]

    if job_type_filter != "All":
        filtered = [j for j in filtered if j.job_type and job_type_filter.lower() in j.job_type.lower()]

    if skills_filter:
        filtered = [j for j in filtered if any(skill in j.skills for skill in skills_filter)]

    if profile_filter:
        filtered = [j for j in filtered if j.profile in profile_filter]

    if experience_filter:
        ef = experience_filter.lower()
        filtered = [j for j in filtered if j.experience and ef in j.experience.lower()]

    if salary_filter:
        sf = salary_filter.lower()
        filtered = [j for j in filtered if j.salary and sf in j.salary.lower()]

    if "All" not in job_level_filter:
        filtered = [j for j in filtered if j.job_level in job_level_filter]

    return filtered

def apply_sorting(jobs: List[Job], sort_by: str) -> List[Job]:
    """Sorts the list of jobs based on the selected criterion."""
    if sort_by == "Date Posted (Newest)":
        return sorted(jobs, key=lambda x: x.date_posted or datetime.min, reverse=True)
    elif sort_by == "Date Posted (Oldest)":
        return sorted(jobs, key=lambda x: x.date_posted or datetime.min, reverse=False)
    elif sort_by == "Title A-Z":
        return sorted(jobs, key=lambda x: x.title.lower())
    elif sort_by == "Title Z-A":
        return sorted(jobs, key=lambda x: x.title.lower(), reverse=True)
    elif sort_by == "Company A-Z":
        return sorted(jobs, key=lambda x: x.company.lower())
    elif sort_by == "Company Z-A":
        return sorted(jobs, key=lambda x: x.company.lower(), reverse=True)
    else: # Relevance (default, could be improved with scoring)
        return jobs

def create_display_dataframe(jobs: List[Job]) -> pd.DataFrame:
    """Creates a pandas DataFrame for display from a list of Job objects."""
    df = pd.DataFrame([job.model_dump() for job in jobs])

    # Select and reorder columns for display
    display_columns = [
        "title", "company", "location", "remote", "job_type", "experience", "salary",
        "skills", "profile", "job_level", "source", "date_posted", "apply_url"
    ]

    # Ensure all columns exist
    for col in display_columns:
        if col not in df.columns:
            df[col] = None

    df = df[display_columns]

    # Format date
    if "date_posted" in df.columns:
        df["date_posted"] = df["date_posted"].dt.strftime("%Y-%m-%d %H:%M") if not df["date_posted"].isnull().all() else "N/A"

    return df.fillna("N/A")

if __name__ == "__main__":
    main()
