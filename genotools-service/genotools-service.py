import json
import logging
import os
import time
import threading
from flask import Flask, jsonify, request
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
import subprocess
import psutil
import shutil
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import gridfs
from datetime import datetime, timedelta
import base64
import requests
import random

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

executor = ThreadPoolExecutor(max_workers=int(os.getenv('MAX_CONCURRENT_JOBS', '2')))

#MongoDB Connection
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_DB = os.getenv('MONGO_DB', 'genotools')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'job_results')

try: 
    mongo_client = MongoClient(f'mongodb://{MONGO_HOST}:{MONGO_PORT}', serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB]
    job_results_collection = mongo_db[MONGO_COLLECTION]

    #Create TTL index for automatic deletion
    job_results_collection.create_index(
        "created_at",
        expireAfterSeconds=600
    )

    #Create index on job_id
    job_results_collection.create_index("job_id", unique=True)

    logging.info(f"Connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
except Exception as e:
    logging.error(f"Failed to MongoDB: {e}")
    mongo_client = None
    job_results_collection = None

# === COMPREHENSIVE PROMETHEUS METRICS ===

# Job Processing Metrics
analysis_counter = Counter('genotools_analyses_total', 'Total analyses processed', ['type', 'status'])
analysis_duration = Histogram('genotools_analysis_duration_seconds', 'Analysis processing time', ['type'])
active_analyses = Gauge('genotools_active_analyses', 'Currently running analyses')
queued_analyses = Gauge('genotools_queued_analyses', 'Jobs waiting to be processed')

# Resource Metrics
pod_cpu_usage = Gauge('genotools_pod_cpu_percent', 'Current pod CPU usage')
pod_memory_usage = Gauge('genotools_pod_memory_percent', 'Current pod memory usage')
pod_memory_bytes = Gauge('genotools_pod_memory_bytes', 'Current pod memory usage in bytes')
pod_disk_usage = Gauge('genotools_pod_disk_usage_bytes', 'Current pod disk usage in bytes')

# Scaling Metrics
kubernetes_pod_count = Gauge('genotools_kubernetes_pod_count', 'Current number of GenoTools pods')
kubernetes_desired_replicas = Gauge('genotools_kubernetes_desired_replicas', 'HPA desired replicas')
kubernetes_hpa_target_utilization = Gauge('genotools_kubernetes_hpa_target', 'HPA target queue length')
kubernetes_hpa_current_utilization = Gauge('genotools_kubernetes_hpa_current', 'HPA current queue length')

# Performance Metrics
job_throughput = Gauge('genotools_job_throughput_per_minute', 'Jobs completed per minute')
average_job_duration = Gauge('genotools_average_job_duration_seconds', 'Average job processing time')
pod_efficiency = Gauge('genotools_pod_efficiency_percent', 'Pod utilization efficiency')

# Service Info
service_info = Info('genotools_service_info', 'GenoTools service information')
service_uptime = Gauge('genotools_service_uptime_seconds', 'Service uptime in seconds')

# Error Metrics
error_rate = Gauge('genotools_error_rate_percent', 'Job error rate percentage')
timeout_count = Counter('genotools_timeouts_total', 'Total job timeouts')
restart_count = Counter('genotools_restarts_total', 'Pod restart count')

# Dynamic concurrency control
MAX_CONCURRENT_JOBS = int(os.getenv('MAX_CONCURRENT_JOBS', '2'))
CPU_THRESHOLD = float(os.getenv('CPU_THRESHOLD', '75.0'))
MEMORY_THRESHOLD = float(os.getenv('MEMORY_THRESHOLD', '75.0'))

# Job management
active_jobs = {}
completed_jobs = {}
job_lock = threading.Lock()
start_time = time.time()

# Metrics collection
recent_completions = [] 
total_job_time = 0
total_completed_jobs = 0

def store_job_results(job_id, job_data):
    """Store job results in MongoDB using GridFS for large files"""
    if job_results_collection is None:
        logging.warning("MongoDB not available, skipping result storage")
        return False
    
    try:
        # Initialize GridFS
        fs = gridfs.GridFS(mongo_db)
        
        stored_files = {}
        results_folder = f"/shared/results/{job_id}"
        
        # File size threshold for GridFS (15MB to stay under 16MB document limit)
        GRIDFS_THRESHOLD = 15 * 1024 * 1024  # 15MB
        
        logging.info(f"*** STORING JOB {job_id} RESULTS ***")
        
        if os.path.exists(results_folder):
            for filename in os.listdir(results_folder):
                file_path = os.path.join(results_folder, filename)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    
                    logging.info(f"   Processing file: {filename} ({file_size:,} bytes)")
                    
                    if file_size > GRIDFS_THRESHOLD:
                        # Store large files in GridFS
                        logging.info(f"Storing large file {filename} in GridFS")
                        
                        with open(file_path, 'rb') as f:
                            file_id = fs.put(
                                f,
                                filename=filename,
                                job_id=job_id,
                                upload_date=datetime.utcnow(),
                                content_type='application/octet-stream'
                            )
                        
                        stored_files[filename] = {
                            'storage_type': 'gridfs',
                            'file_id': str(file_id),
                            'size': file_size,
                            'filename': filename,
                            'content_type': 'application/octet-stream'
                        }
                        
                        logging.info(f"Stored in GridFS with ID: {file_id}")
                        
                    else:
                        # Store small files in document
                        logging.info(f"Storing small file {filename} in document")
                        
                        with open(file_path, 'rb') as f:
                            file_content = f.read()
                        
                        # Try to decode as text first
                        if filename.endswith(('.txt', '.log', '.summary', '.csv', '.tsv')):
                            try:
                                stored_files[filename] = {
                                    'storage_type': 'document',
                                    'content': file_content.decode('utf-8'),
                                    'content_type': 'text',
                                    'size': file_size,
                                    'filename': filename
                                }
                                logging.info(f"Stored as text document")
                            except UnicodeDecodeError:
                                # If text decode fails, store as binary
                                stored_files[filename] = {
                                    'storage_type': 'document',
                                    'content': base64.b64encode(file_content).decode('utf-8'),
                                    'content_type': 'binary',
                                    'size': file_size,
                                    'filename': filename
                                }
                                logging.info(f"Stored as binary document (base64)")
                        else:
                            # Store binary files as base64
                            stored_files[filename] = {
                                'storage_type': 'document',
                                'content': base64.b64encode(file_content).decode('utf-8'),
                                'content_type': 'binary',
                                'size': file_size,
                                'filename': filename
                            }
                            logging.info(f"Stored as binary document (base64)")
        
        # Calculate total storage size
        total_document_size = sum(
            len(f.get('content', '')) for f in stored_files.values() 
            if f.get('storage_type') == 'document'
        )
        total_gridfs_size = sum(
            f.get('size', 0) for f in stored_files.values() 
            if f.get('storage_type') == 'gridfs'
        )
        
        logging.info(f"Storage breakdown:")
        logging.info(f"Document storage: {total_document_size:,} bytes")
        logging.info(f"GridFS storage: {total_gridfs_size:,} bytes")
        logging.info(f"Total files: {len(stored_files)}")
        
        # Create MongoDB document (metadata only, not file contents for large files)
        mongo_doc = {
            'job_id': job_id,
            'status': job_data.get('status', 'unknown'),
            'success': job_data.get('success', False),
            'analysis_type': job_data.get('analysis_type', 'qc'),
            'duration': job_data.get('duration', 0),
            'completion_time': job_data.get('completion_time', time.time()),
            'output': job_data.get('output', ''),
            'error': job_data.get('error', ''),
            'command': job_data.get('command', ''),
            'uploaded_files': job_data.get('uploaded_files', []),
            'output_files': job_data.get('output_files', []),
            'stored_files': stored_files,  # This contains metadata + small file contents
            'storage_summary': {
                'total_files': len(stored_files),
                'document_files': len([f for f in stored_files.values() if f.get('storage_type') == 'document']),
                'gridfs_files': len([f for f in stored_files.values() if f.get('storage_type') == 'gridfs']),
                'total_document_bytes': total_document_size,
                'total_gridfs_bytes': total_gridfs_size
            },
            'created_at': datetime.utcnow(),
            'expires_at': datetime.utcnow() + timedelta(minutes=10)
        }
        
        # Check document size before storing
        import json
        doc_size_estimate = len(json.dumps(mongo_doc, default=str))
        
        if doc_size_estimate > 15 * 1024 * 1024:  # 15MB safety margin
            logging.error(f"Document still too large: {doc_size_estimate:,} bytes")
            # Move more files to GridFS if needed
            return False
        
        logging.info(f"Document size: {doc_size_estimate:,} bytes (safe for MongoDB)")
        
        # Store in MongoDB
        job_results_collection.replace_one(
            {'job_id': job_id},
            mongo_doc,
            upsert=True
        )
        
        logging.info(f"   SUCCESSFULLY STORED JOB {job_id}")
        logging.info(f"   MongoDB document: {doc_size_estimate:,} bytes")
        logging.info(f"   Small files in document: {len([f for f in stored_files.values() if f.get('storage_type') == 'document'])}")
        logging.info(f"   Large files in GridFS: {len([f for f in stored_files.values() if f.get('storage_type') == 'gridfs'])}")
        logging.info(f"   Total storage: {(total_document_size + total_gridfs_size):,} bytes")
        
        return True
    
    except gridfs.errors.GridFSError as e:
        logging.error(f"GridFS error storing job {job_id}: {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to store job {job_id} in MongoDB: {e}")
        logging.error(f"   Error type: {type(e).__name__}")
        return False



def update_service_info():
    """Update service information metrics"""
    service_info.info({
        'version': '1.0',
        'max_concurrent_jobs': str(MAX_CONCURRENT_JOBS),
        'cpu_threshold': str(CPU_THRESHOLD),
        'memory_threshold': str(MEMORY_THRESHOLD),
        'pod_name': os.getenv('HOSTNAME', 'unknown'),
        'namespace': os.getenv('POD_NAMESPACE', 'default')
    })

def get_resource_usage():
    """Get current pod resource usage"""
    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_info = psutil.virtual_memory()
        memory_percent = memory_info.percent
        memory_bytes = memory_info.used
        
        # Disk usage
        disk_usage = psutil.disk_usage('/tmp').used
        
        # Update metrics
        pod_cpu_usage.set(cpu_percent)
        pod_memory_usage.set(memory_percent)
        pod_memory_bytes.set(memory_bytes)
        pod_disk_usage.set(disk_usage)
        
        return cpu_percent, memory_percent
    except Exception as e:
        logging.error(f"Error getting resource usage: {e}")
        return 0, 0

def get_kubernetes_metrics():
    """Get Kubernetes scaling metrics"""
    try:
        import requests

        with job_lock:
            current_active = len(active_jobs)
        
        kubernetes_pod_count.set(1)  
        
    except Exception as e:
        logging.warning(f"Error getting Kubernetes metrics: {e}")

def calculate_performance_metrics():
    """Calculate performance and efficiency metrics"""
    global recent_completions, total_job_time, total_completed_jobs
    
    try:
        # Service uptime
        uptime = time.time() - start_time
        service_uptime.set(uptime)
        
        # Clean old completions (keep last 60 seconds for throughput)
        now = time.time()
        recent_completions = [t for t in recent_completions if now - t < 60]
        job_throughput.set(len(recent_completions))
        
        # Average job duration
        if total_completed_jobs > 0:
            avg_duration = total_job_time / total_completed_jobs
            average_job_duration.set(avg_duration)
        
        # Pod efficiency (active jobs / max possible)
        with job_lock:
            current_active = len(active_jobs)
        
        efficiency = (current_active / MAX_CONCURRENT_JOBS) * 100
        pod_efficiency.set(efficiency)
        
        # Error rate
        total_jobs = len(completed_jobs)
        if total_jobs > 0:
            failed_jobs = len([j for j in completed_jobs.values() if not j.get('success', False)])
            error_rate.set((failed_jobs / total_jobs) * 100)
        
    except Exception as e:
        logging.error(f"Error calculating performance metrics: {e}")

def update_all_metrics():
    """Update all metrics"""
    get_resource_usage()
    get_kubernetes_metrics()
    calculate_performance_metrics()
    update_service_info()

@app.route('/process-job', methods=['POST'])
def process_job():
    """Process a single job (called by controller)"""
    try:
        job_data = request.json
        job_id = job_data['job_id']
        
        logging.info(f"Received job {job_id} via HTTP")
        
        # Submit to thread pool
        future = executor.submit(run_genotools_analysis, job_data)
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "message": "Job accepted for processing"
        }), 200
        
    except Exception as e:
        logging.error(f"Error accepting job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/job-callback', methods=['POST'])
def job_callback():
    """Handle job completion notifications from workers"""
    try:
        data = request.json
        job_id = data.get('job_id')
        result = data.get('result', {})
        
        logging.info(f"Received callback for job {job_id}")
        
        if job_id:
            with job_lock:
                if job_id in active_jobs: 
                    # Update job status from worker result
                    active_jobs[job_id]['status'] = result.get('status', 'completed')
                    active_jobs[job_id]['success'] = result.get('success', False)
                    active_jobs[job_id]['end_time'] = time.time()
                    active_jobs[job_id]['duration'] = result.get('duration', 0)
                    active_jobs[job_id]['completion_time'] = result.get('completion_time', time.time())
                    active_jobs[job_id]['result'] = result
                    
                    # Update metrics
                    status = 'completed' if result.get('success') else 'failed'
                    
                    logging.info(f"Updated job {job_id} status to: {active_jobs[job_id]['status']}")
                    
                    # Schedule cleanup after 5 minutes
                    def cleanup_job():
                        time.sleep(300) 
                        with job_lock:
                            if job_id in active_jobs:
                                del active_jobs[job_id]
                                logging.info(f"Cleaned up tracking for job {job_id}")
                    
                    threading.Thread(target=cleanup_job, daemon=True).start()
                else:
                    logging.warning(f"Received callback for unknown job {job_id}")
            
            return jsonify({"success": True, "message": f"Job {job_id} status updated"})
        else:
            return jsonify({"error": "Missing job_id"}), 400
            
    except Exception as e:
        logging.error(f"Error in job callback: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health():
    """Enhanced health check with metrics"""
    update_all_metrics()
    
    cpu_percent, memory_percent = get_resource_usage()
    with job_lock:
        active_count = len(active_jobs)
        completed_count = len(completed_jobs)
    
    return {
        "status": "healthy", 
        "active_jobs": active_count,
        "completed_jobs": completed_count,
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "max_concurrent": MAX_CONCURRENT_JOBS,
        "uptime_seconds": time.time() - start_time,
        "throughput_per_minute": len(recent_completions)
    }, 200

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint"""
    update_all_metrics()
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@app.route('/status')
def get_status():
    """Status endpoint for controller capacity checking"""
    update_all_metrics()
    
    cpu_percent, memory_percent = get_resource_usage()
    
    with job_lock:
        active_count = len(active_jobs)
        completed_count = len(completed_jobs)
        
        # Get details of active jobs
        active_job_details = []
        for job_id, job_info in active_jobs.items():
            active_job_details.append({
                'job_id': job_id,
                'status': job_info.get('status', 'unknown'),
                'analysis_type': job_info.get('analysis_type', 'qc'),
                'start_time': job_info.get('start_time', 0),
                'duration_so_far': time.time() - job_info.get('start_time', time.time())
            })
    
    return jsonify({
        "service": "genotools-service",
        "status": "running",
        "active_jobs": active_count,
        "max_concurrent_jobs": MAX_CONCURRENT_JOBS,
        "available_capacity": MAX_CONCURRENT_JOBS - active_count,
        "completed_jobs": completed_count,
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "uptime_seconds": time.time() - start_time,
        "throughput_per_minute": len(recent_completions),
        "active_job_details": active_job_details,
        "resource_thresholds": {
            "cpu_threshold": CPU_THRESHOLD,
            "memory_threshold": MEMORY_THRESHOLD
        }
    }), 200

@app.route('/job-status/<job_id>')
def get_job_status(job_id):
    """Get job status from this GenoTools service"""
    
    # Check active jobs
    with job_lock:
        if job_id in active_jobs:
            job_info = active_jobs[job_id].copy()
            return jsonify({
                "job_id": job_id,
                "status": job_info['status'],
                "tracked_by": "genotools-service",
                **job_info
            })
    
    # Check completed jobs
    if job_id in completed_jobs:
        job_info = completed_jobs[job_id].copy()
        return jsonify({
            "job_id": job_id,
            "status": job_info['status'],
            "success": job_info.get('success', False),
            "tracked_by": "genotools-service",
            **job_info
        })
    
    # Not found in this service
    return jsonify({"error": "Job not found"}), 404

def run_genotools_analysis(job_config):
    """Run GenoTools analysis with enhanced metrics and command logging"""
    global recent_completions, total_job_time, total_completed_jobs
    
    job_id = job_config['job_id']
    analysis_type = job_config.get('analysis_type', 'qc')
    
    files_data = job_config.get('files', {})
    genetic_file = files_data.get('genetic_file', '')
    uploaded_files = [genetic_file] if genetic_file else []

    start_time = time.time()
    
    try:
        # Mark job as active
        start_time_job = time.time()
        with job_lock:
            active_jobs[job_id] = {
                "status": "running",
                "start_time": start_time_job,
                "analysis_type": analysis_type,
                "progress": 0,
                "uploaded_files": uploaded_files,
                "genetic_file": genetic_file  
            }
        
        active_analyses.inc()
        analysis_counter.labels(type=analysis_type, status='started').inc()
        
        # Log resource usage
        cpu_before, mem_before = get_resource_usage()
        with job_lock:
            active_count = len(active_jobs)
        
        logging.info(f"Starting job {job_id} (CPU: {cpu_before}%, RAM: {mem_before}%, Active: {active_count})")
        logging.info(f"Analysis type: {analysis_type}")
        logging.info(f"Genetic file: {genetic_file}")
        logging.info(f"Uploaded files: {uploaded_files}")
        
        # Create output directory
        output_path = f'/tmp/analysis_{job_id}'
        results_folder = f"/shared/results/{job_id}"
        
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(results_folder, exist_ok=True)
        
        if genetic_file and os.path.exists(genetic_file):
            logging.info(f"Found uploaded genetic file: {genetic_file}")
            
            # For PLINK files, determine the base name
            if genetic_file.endswith(('.bed', '.bim', '.fam')):
                pfile_base = genetic_file.replace('.bed', '').replace('.bim', '').replace('.fam', '')
            elif genetic_file.endswith(('.pgen', '.pvar', '.psam')):
                pfile_base = genetic_file.replace('.pgen', '').replace('.pvar', '').replace('.psam', '')
            else:
                pfile_base = genetic_file  # For other formats
            
            logging.info(f"Using uploaded file base: {pfile_base}")
            
            # Verify associated files exist
            if genetic_file.endswith('.bed'):
                bim_file = genetic_file.replace('.bed', '.bim')
                fam_file = genetic_file.replace('.bed', '.fam')
                if not os.path.exists(bim_file):
                    logging.warning(f"Missing .bim file: {bim_file}")
                if not os.path.exists(fam_file):
                    logging.warning(f"Missing .fam file: {fam_file}")
            
        else:
            # Use default test files
            pfile_base = job_config.get('pfile', '/app/small_test')
            logging.info(f"Using default test data: {pfile_base}")
            logging.warning(f"Uploaded genetic file not found or not provided: {genetic_file}")
        
        # Build GenoTools command based on analysis type
        if analysis_type == 'ancestry':
            cmd = [
                'genotools',
                '--ancestry',
                '--bfile', pfile_base,
                '--out', f'{output_path}/results',
                '--ref_panel', job_config.get('ref_panel', '/app/ref_panel_gp2_prune_rm_underperform_pos_update'),
                '--ref_labels', job_config.get('ref_labels', 'ref_panel_ancestry_updated.txt')
            ]
        elif analysis_type == 'gwas':
            cmd = [
                'genotools',
                '--gwas',
                '--bfile', pfile_base,
                '--out', f'{output_path}/results',
                '--covars', job_config.get('covars', '/app/covariates.txt')
            ]
        else:  # QC
            cmd = [
                'genotools',
                '--bfile', pfile_base,
                '--out', f'{output_path}/results',
                '--geno', '0.05',
                '--callrate', '0.95',
                '--hwe', '1e-6',
                '--all_variant'
            ]
        
        # Log the exact command being executed
        command_str = ' '.join(cmd)
        logging.info("=" * 80)
        logging.info(f"EXECUTING COMMAND FOR JOB {job_id}:")
        logging.info(f"Working directory: /app")
        logging.info(f"Full command: {command_str}")
        logging.info(f"Input pfile base: {pfile_base}")
        logging.info(f"Genetic file: {genetic_file}")
        logging.info(f"Output path: {output_path}/results")
        if genetic_file:
            logging.info(f"Using UPLOADED genetic file: {genetic_file}")
        else:
            logging.info(f"Using DEFAULT test data")
        logging.info("=" * 80)
        
        # Run GenoTools
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minutes max
            cwd='/app'
        )
        
        duration = time.time() - start_time_job
        analysis_duration.labels(type=analysis_type).observe(duration)

        logging.info(f"Command completed in {duration:.2f} seconds")
        logging.info(f"Return code: {result.returncode}")
        
        if result.stdout:
            logging.info("STDOUT:")
            logging.info(result.stdout)
        
        if result.stderr:
            logging.info("STDERR:")
            logging.info(result.stderr)

        # Update performance tracking
        recent_completions.append(time.time())
        total_job_time += duration
        total_completed_jobs += 1
        
        # Log completion
        cpu_after, mem_after = get_resource_usage()
        logging.info(f"Completed job {job_id} in {duration:.1f}s (CPU: {cpu_after}%, RAM: {mem_after}%)")
        
        # Process results
        if result.returncode == 0:
            # Copy results to shared storage
            shared_results = f"/shared/results/{job_id}"
            os.makedirs(shared_results, exist_ok=True)
            
            output_files = []
            if os.path.exists(output_path):
                for file in os.listdir(output_path):
                    if file.endswith(('.txt', '.log', '.summary', '.pgen', '.pvar', '.psam')):
                        shutil.copy2(os.path.join(output_path, file), shared_results)
                        output_files.append(file)
            
            log_file = os.path.join(shared_results, f"{job_id}_analysis.log")
            with open(log_file, 'w') as f:
                f.write(f"GenoTools Analysis Log\n")
                f.write(f"Job ID: {job_id}\n")
                f.write(f"Analysis Type: {analysis_type}\n")
                f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Duration: {duration:.2f} seconds\n")
                f.write(f"Command: {command_str}\n")
                f.write(f"Input Files: {pfile_base}\n")
                f.write(f"Uploaded Files: {uploaded_files}\n")
                f.write(f"Output Files: {', '.join(output_files)}\n")
                f.write(f"\n=== STDOUT ===\n{result.stdout}\n")
                f.write(f"\n=== STDERR ===\n{result.stderr}\n")
            
            output_files.append(f"{job_id}_analysis.log")
            
            with job_lock:
                completed_jobs[job_id] = {
                    "status": "completed",
                    "success": True,
                    "duration": duration,
                    "completion_time": time.time(),
                    "output": result.stdout,
                    "error": result.stderr,
                    "output_files": output_files,
                    "analysis_type": analysis_type,
                    "command": command_str,  
                    "uploaded_files": uploaded_files
                }

            store_job_results(job_id, completed_jobs[job_id])
            
            analysis_counter.labels(type=analysis_type, status='completed').inc()
            logging.info(f"Job {job_id} completed successfully with {len(output_files)} output files")
            
        else:
            with job_lock:
                completed_jobs[job_id] = {
                    "status": "failed",
                    "success": False,
                    "duration": duration,
                    "completion_time": time.time(),
                    "error": result.stderr,
                    "output": result.stdout,
                    "analysis_type": analysis_type,
                    "command": command_str,  
                    "uploaded_files": uploaded_files
                }

            store_job_results(job_id, completed_jobs[job_id])
            
            analysis_counter.labels(type=analysis_type, status='failed').inc()
            logging.error(f"Job {job_id} failed with return code {result.returncode}")

    except subprocess.TimeoutExpired:
        logging.error(f"Job {job_id} timed out")
        timeout_count.inc()
        with job_lock:
            completed_jobs[job_id] = {
                "status": "failed",
                "success": False,
                "error": "Analysis timed out after 30 minutes",
                "analysis_type": analysis_type,
                "uploaded_files": uploaded_files
            }

        store_job_results(job_id, completed_jobs[job_id])
        analysis_counter.labels(type=analysis_type, status='timeout').inc()
     
    except Exception as e:
        logging.error(f"Error in job {job_id}: {e}")
        with job_lock:
            completed_jobs[job_id] = {
                "status": "failed",
                "success": False,
                "error": str(e),
                "analysis_type": analysis_type,
                "uploaded_files": uploaded_files
            }

        store_job_results(job_id, completed_jobs[job_id])
        analysis_counter.labels(type=analysis_type, status='error').inc()

    finally:
        # Clean up
        with job_lock:
            if job_id in active_jobs:
                del active_jobs[job_id]
        active_analyses.dec()
        
        # Clean up temp files
        try:
            shutil.rmtree(output_path, ignore_errors=True)
            logging.info(f"Cleaned up temp files for job {job_id}")
        except:
            pass

    try:
        completion_data = {
            "job_id": job_id,
            "result": completed_jobs[job_id]
        }
        requests.post(
            'http://genotools-controller:8080/job-callback',
            json=completion_data,
            timeout=5
        )
        logging.info(f"Notified controller of job {job_id} completion")
    except Exception as e:
        logging.warning(f"Failed to notify controller: {e}")

def metrics_updater():
    """Background thread to update metrics regularly"""
    while True:
        try:
            update_all_metrics()
            time.sleep(10)  # Update every 10 seconds
        except Exception as e:
            logging.error(f"Error in metrics updater: {e}")
            time.sleep(30)

def main():
    """Main service loop"""
    logging.info("Starting GenoTools Service with Comprehensive Metrics...")
    logging.info(f"Max concurrent jobs: {MAX_CONCURRENT_JOBS}")
    logging.info("Waiting for jobs via HTTP /process-job endpoint")
    
    # Initialize service info
    update_service_info()
    
    # Start metrics updater
    metrics_thread = threading.Thread(target=metrics_updater, daemon=True)
    metrics_thread.start()

    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)

if __name__ == "__main__":
    main()