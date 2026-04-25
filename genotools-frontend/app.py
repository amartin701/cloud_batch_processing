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
import gridfs
from bson import ObjectId
from datetime import datetime, timedelta
import base64

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024 * 1024  # 1GB max file size
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
    """Restore job result files from MongoDB (both document and GridFS) to filesystem for download"""
    try:
        import gridfs
        
        results_folder = os.path.join(RESULTS_FOLDER, job_id)
        os.makedirs(results_folder, exist_ok=True)
        
        # Initialize GridFS
        fs = gridfs.GridFS(mongo_db)
        
        stored_files = mongo_doc.get('stored_files', {})
        restored_files = []
        
        app.logger.info(f"*** RESTORING {len(stored_files)} FILES FOR JOB {job_id} ***")
        
        for filename, file_data in stored_files.items():
            file_path = os.path.join(results_folder, filename)
            storage_type = file_data.get('storage_type', 'document')  # Default to old format
            
            try:
                if storage_type == 'gridfs':
                    # Restore from GridFS
                    app.logger.info(f"Restoring large file {filename} from GridFS")
                    
                    file_id = gridfs.ObjectId(file_data['file_id'])
                    
                    # Get file from GridFS and write to filesystem
                    with open(file_path, 'wb') as f:
                        gridfs_file = fs.get(file_id)
                        f.write(gridfs_file.read())
                    
                    file_size = os.path.getsize(file_path)
                    app.logger.info(f"Restored from GridFS: {filename} ({file_size:,} bytes)")
                    
                elif storage_type == 'document':
                    # Restore from document storage
                    app.logger.info(f"Restoring small file {filename} from document")
                    
                    content_type = file_data.get('content_type', 'binary')
                    content = file_data.get('content', '')
                    
                    if content_type == 'text':
                        # Text file
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                    else:
                        # Binary file (base64 encoded)
                        decoded_content = base64.b64decode(content)
                        with open(file_path, 'wb') as f:
                            f.write(decoded_content)
                    
                    file_size = os.path.getsize(file_path)
                    app.logger.info(f"Restored from document: {filename} ({file_size:,} bytes)")
                    
                else:
                    # Legacy format (backwards compatibility)
                    app.logger.info(f"Restoring legacy file {filename}")
                    
                    if file_data.get('type') == 'text':
                        # Legacy text file
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(file_data['content'])
                    else:
                        # Legacy binary file
                        content = base64.b64decode(file_data['content'])
                        with open(file_path, 'wb') as f:
                            f.write(content)
                    
                    file_size = os.path.getsize(file_path)
                    app.logger.info(f"Restored legacy file: {filename} ({file_size:,} bytes)")
                
                restored_files.append(filename)
                
            except gridfs.errors.NoFile:
                app.logger.error(f"GridFS file not found for {filename} (ID: {file_data.get('file_id')})")
            except gridfs.errors.GridFSError as e:
                app.logger.error(f"GridFS error restoring {filename}: {e}")
            except Exception as e:
                app.logger.error(f"Failed to restore file {filename}: {e}")
        
        app.logger.info(f"*** RESTORED {len(restored_files)} FILES FOR JOB {job_id} ***")
        app.logger.info(f"   Files: {', '.join(restored_files)}")
        app.logger.info(f"   Location: {results_folder}")
        
        return restored_files
        
    except Exception as e:
        app.logger.error(f"Failed to restore files for job {job_id}: {e}")
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
        start_time = datetime.now()
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
        logging.info(f"Publishing job {job_id} to queue...")
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
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        jobs_queued.inc()
        logging.info(f"Job {job_id} queued for analysis: {analysis_type}")
        logging.info(f"Time taken: {total_duration:.3f} seconds")

        return jsonify({
            "success": True,
            "job_id": job_id,
            "message": "Job queued successfully",
        })


        
    except Exception as e:
        logging.error(f"Error queuing job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download-results/<job_id>')
def download_results(job_id):
    """Download analysis results as zip file"""
    try:
        app.logger.info(f"*** DOWNLOAD REQUEST FOR JOB {job_id} ***")
        
        results_folder = os.path.join(RESULTS_FOLDER, job_id)
        
        # Check if results folder exists
        if not os.path.exists(results_folder):
            app.logger.info(f"Results folder not found, attempting to restore from MongoDB...")
            
            # Try to restore from MongoDB first
            mongo_result = get_job_from_mongodb(job_id)
            if mongo_result and mongo_result.get('stored_files'):
                restored_files = restore_files_from_mongodb(job_id, mongo_result)
                if not restored_files:
                    app.logger.error(f"Failed to restore files for job {job_id}")
                    return jsonify({"error": "Results not found and could not be restored"}), 404
                app.logger.info(f"Restored {len(restored_files)} files from MongoDB")
            else:
                app.logger.error(f"Job {job_id} not found in MongoDB either")
                return jsonify({"error": "Results not found"}), 404
        
        # Check if folder has files
        all_files = []
        for root, dirs, files in os.walk(results_folder):
            all_files.extend(files)
        
        if not all_files:
            app.logger.warning(f"Results folder exists but is empty for job {job_id}")
            return jsonify({"error": "No result files found"}), 404
        
        app.logger.info(f"Creating zip file with {len(all_files)} files")
        
        # Create zip file of all results
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            total_size = 0
            for root, dirs, files in os.walk(results_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, results_folder)
                    
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        zip_file.write(file_path, arcname)
                        total_size += file_size
                        app.logger.info(f"Added {arcname} ({file_size:,} bytes) to zip")
                    else:
                        app.logger.warning(f"File missing during zip creation: {file_path}")
        
        zip_buffer.seek(0)
        zip_size = len(zip_buffer.getvalue())
        
        app.logger.info(f"   *** DOWNLOAD READY FOR JOB {job_id} ***")
        app.logger.info(f"   Files in zip: {len(all_files)}")
        app.logger.info(f"   Total file size: {total_size:,} bytes")
        app.logger.info(f"   Zip file size: {zip_size:,} bytes")
        app.logger.info(f"   Compression ratio: {((total_size - zip_size) / total_size * 100):.1f}%" if total_size > 0 else "0.0%")
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'genotools_results_{job_id}.zip'
        )
        
    except Exception as e:
        app.logger.error(f"Download error for job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/job-status/<job_id>')
def get_job_status(job_id):
    """Get job status - try live service first, then MongoDB"""
    
    # Try live GenoTools service first
    try:
        response = requests.get(f'http://genotools-controller:8080/job-status/{job_id}', timeout=5)
        if response.status_code == 200:
            job_data = response.json()
            app.logger.info(f"Got live status for job {job_id}: {job_data.get('status')}")
            return jsonify(job_data)
        
        # Try the service directly
        response = requests.get(f'http://genotools-service:8080/job-status/{job_id}', timeout=5)
        if response.status_code == 200:
            job_data = response.json()
            app.logger.info(f"Got live status for job {job_id}: {job_data.get('status')}")
            return jsonify(job_data)
            
    except Exception as e:
        app.logger.info(f"Live services unavailable for job {job_id}: {e}")

    #Try MongoDB 
    mongo_result = get_job_from_mongodb(job_id)
    if mongo_result:
        app.logger.info(f"Found job {job_id} in MongoDB")
        
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