from flask import Flask, render_template, request, jsonify, send_file
import pika  
import json
import uuid
import logging
import requests
import time
import os
import zipfile
from io import BytesIO
from werkzeug.utils import secure_filename
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter
from pymongo import MongoClient
from datetime import datetime, timedelta
import base64

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
logging.basicConfig(level=logging.INFO)

jobs_queued = Counter('frontend_jobs_queued_total', 'Total jobs queued by the frontend')

MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
mongo_client = MongoClient(f'mongodb://{MONGO_HOST}:27017', serverSelectionTimeoutMS=5000)
mongo_db = mongo_client['genotools']
job_results_collection = mongo_db['job_results']

try:
    job_results_collection.create_index("created_at", expireAfterSeconds=600)  # 10 minutes
    job_results_collection.create_index("job_id", unique=True)
    app.logger.info("Connected to MongoDB for job results")
except Exception as e:
    app.logger.error(f"MongoDB setup error: {e}")

UPLOAD_FOLDER = '/shared/uploads'
RESULTS_FOLDER = '/shared/results'

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

def create_rabbitmq_connection():
    """Create RabbitMQ connection with retry"""
    for attempt in range(3):
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    port=5672,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            )
            return connection
        except Exception as e:
            logging.error(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                time.sleep(2)
            else:
                raise

def get_job_from_mongodb(job_id):
    """Retrieve job results from MongoDB"""
    try:
        doc = job_results_collection.find_one({'job_id': job_id})
        if doc:
            doc.pop('_id', None)  # Remove MongoDB internal field
            app.logger.info(f"Retrieved job {job_id} from MongoDB")
            return doc
        return None
    except Exception as e:
        app.logger.error(f"Failed to retrieve job {job_id} from MongoDB: {e}")
        return None

def restore_files_from_mongodb(job_id, mongo_doc):
    """Restore job result files from MongoDB to filesystem for download"""
    try:
        results_folder = os.path.join(RESULTS_FOLDER, job_id)
        os.makedirs(results_folder, exist_ok=True)
        
        stored_files = mongo_doc.get('stored_files', {})
        restored_files = []
        
        for filename, file_data in stored_files.items():
            file_path = os.path.join(results_folder, filename)
            
            if file_data['type'] == 'text':
                # Restore text file
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(file_data['content'])
            else:
                # Decode base64 and restore binary file
                content = base64.b64decode(file_data['content'])
                with open(file_path, 'wb') as f:
                    f.write(content)
            
            restored_files.append(filename)
        
        app.logger.info(f"📦 Restored {len(restored_files)} files for job {job_id}")
        return restored_files
        
    except Exception as e:
        app.logger.error(f"❌ Failed to restore files for job {job_id}: {e}")
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    return {"status": "healthy", "service": "genotools-frontend"}, 200

@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@app.route('/upload-files', methods=['POST'])
def upload_files():
    """Upload user files and trigger analysis"""
    try:
        job_id = str(uuid.uuid4())[:8]
        job_folder = os.path.join('/shared/jobs', job_id)
        os.makedirs(job_folder, exist_ok=True)
        
        uploaded_files = {}
        
        # Handle PLINK file uploads (bed, bim, fam)
        plink_files = ['bed_file', 'bim_file', 'fam_file']
        for file_key in plink_files:
            if file_key in request.files:
                file_obj = request.files[file_key]
                if file_obj.filename != '':
                    filename = secure_filename(file_obj.filename)
                    file_path = os.path.join(job_folder, filename)
                    file_obj.save(file_path)
                    uploaded_files[file_key] = file_path
                    app.logger.info(f"Saved {file_key}: {filename}")
        
        if not uploaded_files:
            return jsonify({"error": "No files uploaded"}), 400

        if 'bed_file' in uploaded_files:
            uploaded_files['genetic_file'] = uploaded_files['bed_file']
        
        # Get analysis parameters
        analysis_type = request.form.get('analysis_type', 'qc')
        
        # Send job to controller
        job_request = {
            "job_id": job_id,
            "analysis_type": analysis_type,
            "files": uploaded_files,
            "output_folder": f"/shared/results/{job_id}"
        }
        
        # Create separate connection for publishing
        logging.info(f"📤 Publishing job {job_id} to queue...")
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue='genotools-jobs', durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key='genotools-jobs',
            body=json.dumps(job_request),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        # Close connection immediately after publishing
        connection.close()
        
        jobs_queued.inc()
        logging.info(f"✅ Job {job_id} queued for analysis: {analysis_type}")
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "message": "Job queued successfully"
        })
        
    except Exception as e:
        logging.error(f"Error queuing job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download-results/<job_id>')
def download_results(job_id):
    """Download analysis results as zip file"""
    try:
        results_folder = os.path.join(RESULTS_FOLDER, job_id)
        
        if not os.path.exists(results_folder):
            return jsonify({"error": "Results not found"}), 404
        
        # Create zip file of all results
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(results_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, results_folder)
                    zip_file.write(file_path, arcname)
        
        zip_buffer.seek(0)
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'genotools_results_{job_id}.zip'
        )
        
    except Exception as e:
        app.logger.error(f"Download error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/job-status/<job_id>')
def get_job_status(job_id):
    """Get job status - try live service first, then MongoDB"""
    
    # Try live GenoTools service first
    try:
        response = requests.get(f'http://genotools-controller:8080/job-status/{job_id}', timeout=5)
        if response.status_code == 200:
            job_data = response.json()
            app.logger.info(f"📡 Got live status for job {job_id}: {job_data.get('status')}")
            return jsonify(job_data)
        
        # Try the service directly
        response = requests.get(f'http://genotools-service:8080/job-status/{job_id}', timeout=5)
        if response.status_code == 200:
            job_data = response.json()
            app.logger.info(f"📡 Got live status for job {job_id}: {job_data.get('status')}")
            return jsonify(job_data)
            
    except Exception as e:
        app.logger.info(f"📡 Live services unavailable for job {job_id}: {e}")

    #Try MongoDB 
    mongo_result = get_job_from_mongodb(job_id)
    if mongo_result:
        app.logger.info(f"📦 Found job {job_id} in MongoDB")
        
        # Restore files to filesystem for download
        restored_files = []
        if mongo_result.get('stored_files'):
            restored_files = restore_files_from_mongodb(job_id, mongo_result)
        
        # Return the job status with restored flag
        return jsonify({
            'job_id': job_id,
            'status': mongo_result['status'],
            'success': mongo_result['success'],
            'analysis_type': mongo_result.get('analysis_type', 'qc'),
            'duration': mongo_result.get('duration', 0),
            'completion_time': mongo_result.get('completion_time'),
            'output': mongo_result.get('output', ''),
            'error': mongo_result.get('error', ''),
            'output_files': mongo_result.get('output_files', []),
            'restored_files': restored_files,
            'restored_from_database': True,
            'expires_at': mongo_result.get('expires_at')
        })

    app.logger.info(f"Job {job_id} not found in live services or MongoDB")
    return jsonify({"error": "Job not found or has expired"}), 404

@app.route('/retrieve-job', methods=['POST'])
def retrieve_job():
    """Retrieve a job by ID from database"""
    try:
        data = request.get_json()
        job_id = data.get('job_id', '').strip()
        
        if not job_id:
            return jsonify({"error": "Job ID is required"}), 400
        
        # Check if job exists (this will also restore files if found in MongoDB)
        status_response = get_job_status(job_id)
        
        if status_response[1] == 404:  # Not found
            return jsonify({
                "success": False,
                "error": "Job not found or has expired"
            }), 404
        
        job_data = status_response[0].get_json()
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "job_data": job_data,
            "message": f"Job {job_id} retrieved successfully"
        })
        
    except Exception as e:
        app.logger.error(f"Retrieve job error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)