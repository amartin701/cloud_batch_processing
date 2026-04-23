import json
import logging
import pika
import requests
import uuid
import threading
import time
from flask import Flask, jsonify, request
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Prometheus metrics
jobs_queued = Counter('genotools_jobs_queued_total', 'Total jobs queued')
jobs_dispatched = Counter('genotools_jobs_dispatched_total', 'Total jobs dispatched to workers')
jobs_completed = Counter('genotools_jobs_completed_total', 'Total jobs completed', ['status'])
queue_depth = Gauge('genotools_queue_depth', 'Number of jobs in RabbitMQ queue')
active_jobs = Gauge('genotools_active_jobs', 'Number of jobs currently being processed')

# Track active jobs
current_jobs = {}
job_lock = threading.Lock()
consumer_running = False
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60

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

def classify_failure(error_message, status_code):
    """
    Classify failure type to determine if job should be retried
    Returns: ('pod_failure', True) or ('user_error', False)
    """
    error_lower = str(error_message).lower()
    
    # Pod/Infrastructure failures (SHOULD RETRY)
    pod_failures = [
        'connection refused',
        'timeout', 'timed out',
        'service unavailable', '503',
        'internal server error', '500',
        'bad gateway', '502',
        'gateway timeout', '504',
        'network', 'dns',
        'pod not ready',
        'container', 'kubernetes',
        'out of memory', 'memory',
        'disk space', 'no space left'
    ]
    
    # User input failures (SHOULD NOT RETRY)  
    user_failures = [
        'file not found', '404',
        'invalid file format',
        'bad request', '400',
        'unauthorized', '401', '403',
        'malformed', 'syntax error',
        'invalid parameter',
        'missing required field',
        'file too large',
        'unsupported format',
        'validation error'
    ]
    
    # Check for pod failures first
    for failure_pattern in pod_failures:
        if failure_pattern in error_lower:
            return 'pod_failure', True
    
    # Check for user failures
    for failure_pattern in user_failures:
        if failure_pattern in error_lower:
            return 'user_error', False
    
    # HTTP status codes
    if status_code:
        if status_code >= 500:  # Server errors - retry
            return 'pod_failure', True
        elif status_code >= 400 and status_code < 500:  # Client errors - don't retry
            return 'user_error', False
    
    # Default: assume pod failure for unknown errors (safer to retry)
    return 'unknown_failure', True

def requeue_job_with_retry(job_data, retry_count, failure_reason):
    """Requeue job with incremented retry count"""
    try:
        job_data['retry_count'] = retry_count + 1
        job_data['last_failure'] = failure_reason
        job_data['retry_timestamp'] = time.time()
        
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue='genotools-jobs', durable=True)
        
        # Add delay by using a delayed exchange or simple sleep
        time.sleep(RETRY_DELAY_SECONDS)
        
        channel.basic_publish(
            exchange='',
            routing_key='genotools-jobs',
            body=json.dumps(job_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers={'retry_count': retry_count + 1}
            )
        )
        
        connection.close()
        logging.info(f"Job {job_data.get('job_id')} requeued for retry {retry_count + 1}/{MAX_RETRIES}")
        
    except Exception as e:
        logging.error(f"Failed to requeue job: {e}")

def send_to_failed_jobs_queue(job_data, failure_type, failure_reason):
    """Send permanently failed job to a separate queue for review"""
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        
        # Create failed jobs queue
        channel.queue_declare(queue='genotools-jobs-failed', durable=True)
        
        failed_job = {
            'original_job': job_data,
            'failure_type': failure_type,
            'failure_reason': failure_reason,
            'failed_at': time.time(),
            'retry_count': job_data.get('retry_count', 0)
        }
        
        channel.basic_publish(
            exchange='',
            routing_key='genotools-jobs-failed',
            body=json.dumps(failed_job),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        connection.close()
        logging.error(f"Job {job_data.get('job_id')} sent to failed queue: {failure_type} - {failure_reason}")
        
    except Exception as e:
        logging.error(f"Failed to send job to failed queue: {e}")

def can_service_accept_job():
    """Check if any GenoTools service instance can accept a job"""
    try:
        response = requests.get('http://genotools-service:8080/status', timeout=5)
        if response.status_code == 200:
            status = response.json()
            active_count = status.get('active_jobs', 0)
            max_jobs = status.get('max_concurrent_jobs', 2)
            available_capacity = max_jobs - active_count
            
            logging.info(f"Service capacity check: {active_count}/{max_jobs} jobs active, capacity: {available_capacity}")
            return available_capacity > 0
        else:
            logging.warning(f"Service status check failed: {response.status_code}")
            return False
    except Exception as e:
        logging.warning(f"Cannot reach service for capacity check: {e}")
        return False

def consume_queue():
    """Consumer thread that processes the queue with capacity checking"""
    global consumer_running
    consumer_running = True
    
    logging.info("Starting queue consumer thread...")
    
    while consumer_running:
        connection = None
        channel = None
        
        try:
            logging.info("Connecting to RabbitMQ for consumption...")
            connection = create_rabbitmq_connection()
            channel = connection.channel()
            
            # Declare queue
            result = channel.queue_declare(queue='genotools-jobs', durable=True)
            message_count = result.method.message_count
            logging.info(f"Queue declared. Messages waiting: {message_count}")
            
            # Set QoS to 1 (process one message at a time)
            channel.basic_qos(prefetch_count=1)
            def callback(ch, method, properties, body):
                try:
                job_data = json.loads(body)
                job_id = job_data.get('job_id', 'unknown')
                retry_count = job_data.get('retry_count', 0)
        
                if retry_count > 0:
                    logging.info(f"Processing job {job_id} (retry {retry_count})")
                else:
                    logging.info(f"*** CHECKING JOB {job_id} FROM QUEUE ***")
        
                # Check if service can accept the job
                if can_service_accept_job():
                    logging.info(f"Service has capacity, dispatching job {job_id}")
            
                    # Dispatch to GenoTools service
                    dispatch_result = dispatch_job_to_worker(job_data)
            
                    if dispatch_result is True:
                        # Success case
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logging.info(f"Job {job_id} dispatched and removed from queue")
                
                    elif isinstance(dispatch_result, tuple):
                        # Failure case with details
                        success, failure_type, error_msg = dispatch_result
                
                        # Determine if should retry
                        failure_classification, should_retry = classify_failure(error_msg, None)
                
                        if should_retry and retry_count < MAX_RETRIES:
                            # Retry for pod failures
                            logging.warning(f"Job {job_id} failed ({failure_type}), retrying in {RETRY_DELAY_SECONDS}s (attempt {retry_count + 1}/{MAX_RETRIES})")
                    
                            ch.basic_ack(delivery_tag=method.delivery_tag)  # Remove from queue
                            requeue_job_with_retry(job_data, retry_count, error_msg)  # Add back with retry count
                    
                        elif not should_retry:
                            # Don't retry user errors
                            logging.error(f"Job {job_id} failed due to user error ({failure_type}), not retrying: {error_msg}")
                            ch.basic_ack(delivery_tag=method.delivery_tag)  # Remove from queue
                            send_to_failed_jobs_queue(job_data, failure_type, error_msg)
                    
                        else:
                            # Max retries exceeded
                            logging.error(f"Job {job_id} failed after {MAX_RETRIES} attempts, giving up: {error_msg}")
                            ch.basic_ack(delivery_tag=method.delivery_tag)  # Remove from queue  
                            send_to_failed_jobs_queue(job_data, failure_type, f"Max retries exceeded: {error_msg}")
            
                else:
                    logging.info(f"⏸Service at capacity, leaving job {job_id} in queue for KEDA scaling")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    time.sleep(15)
                    logging.info(f"Waiting 15 seconds before checking next job")
        
            except Exception as e:
                logging.error(f"Error processing queued job: {e}", exc_info=True)
        
                # Try to get job data for classification
                try:
                    job_data = json.loads(body)
                    job_id = job_data.get('job_id', 'unknown')
                    retry_count = job_data.get('retry_count', 0)
            
                    # Processing errors are usually pod failures
                    if retry_count < MAX_RETRIES:
                        logging.warning(f"Processing error for job {job_id}, retrying (attempt {retry_count + 1})")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        requeue_job_with_retry(job_data, retry_count, f"Processing error: {e}")
                    else:
                        logging.error(f"Job {job_id} processing failed after max retries")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        send_to_failed_jobs_queue(job_data, 'processing_error', f"Max retries exceeded: {e}")
                except:
                    # Can't parse job data - just discard
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set up consumer
            consumer_tag = channel.basic_consume(
                queue='genotools-jobs', 
                on_message_callback=callback
            )
            
            logging.info(f"*** CONSUMER READY - Capacity-aware consumer tag: {consumer_tag} ***")
            logging.info("*** STARTING CAPACITY-AWARE CONSUMPTION ***")
            
            # Use start_consuming (blocking call)
            channel.start_consuming()
            
        except KeyboardInterrupt:
            logging.info("Consumer interrupted")
            break
        except Exception as e:
            logging.error(f"Queue consumer error: {e}", exc_info=True)
        finally:
            # Clean up
            try:
                if channel and not channel.is_closed:
                    logging.info("Stopping consumption...")
                    channel.stop_consuming()
                    channel.close()
                if connection and not connection.is_closed:
                    logging.info("Closing connection...")
                    connection.close()
            except Exception as cleanup_error:
                logging.error(f"Error during cleanup: {cleanup_error}")
            
            if consumer_running:
                logging.info("Retrying consumer in 5 seconds...")
                time.sleep(5)
    
    logging.info("Queue consumer thread ended")

def dispatch_job_to_worker(job_data):
    """Send job to GenoTools service via HTTP - returns success status"""
    try:
        job_id = job_data['job_id']
        retry_count = job_data.get('retry_count', 0)
        
        # Log retry attempts
        if retry_count > 0:
            logging.info(f"Dispatching job {job_id} (retry {retry_count}/{MAX_RETRIES})")
        
        with job_lock:
            current_jobs[job_id] = {
                'status': 'dispatching',
                'start_time': time.time(),
                'analysis_type': job_data.get('analysis_type', 'qc'),
                'retry_count': retry_count
            }
            active_jobs.set(len(current_jobs))
        
        logging.info(f"Dispatching job {job_id} to GenoTools service...")
        
        # Send job to GenoTools service
        response = requests.post(
            'http://genotools-service:8080/process-job',
            json=job_data,
            timeout=30
        )
        
        if response.status_code == 200:
            jobs_dispatched.inc()
            logging.info(f"Job {job_id} successfully dispatched to worker")
            
            with job_lock:
                if job_id in current_jobs:
                    current_jobs[job_id]['status'] = 'processing'
                    current_jobs[job_id]['dispatched_at'] = time.time()
            
            return True  # SUCCESS
            
        else:
            # Classify the failure
            failure_type, should_retry = classify_failure(response.text, response.status_code)
            error_msg = f"HTTP {response.status_code}: {response.text}"
            
            logging.error(f"Job {job_id} dispatch failed: {error_msg} (Type: {failure_type})")
            
            with job_lock:
                if job_id in current_jobs:
                    current_jobs[job_id]['status'] = 'failed'
                    current_jobs[job_id]['error'] = error_msg
                    current_jobs[job_id]['failure_type'] = failure_type
            
            return False, failure_type, error_msg  # FAILURE with details
            
    except requests.exceptions.Timeout as e:
        # Timeout is always a pod issue - should retry
        error_msg = f"Request timeout: {e}"
        logging.error(f"Job {job_id} timeout: {error_msg}")
        
        with job_lock:
            if job_id in current_jobs:
                current_jobs[job_id]['status'] = 'failed'
                current_jobs[job_id]['error'] = error_msg
        
        return False, 'pod_failure', error_msg
        
    except Exception as e:
        # Classify other exceptions
        failure_type, should_retry = classify_failure(str(e), None)
        error_msg = f"Exception: {e}"
        
        logging.error(f"Error dispatching job {job_id}: {error_msg} (Type: {failure_type})")
        
        job_id = job_data.get('job_id')
        if job_id:
            with job_lock:
                current_jobs[job_id] = {
                    'status': 'failed',
                    'error': error_msg,
                    'failure_type': failure_type,
                    'start_time': time.time()
                }
        
        return False, failure_type, error_msg

@app.route('/debug/consume-one', methods=['POST'])
def debug_consume_one():
    """Debug endpoint to manually consume one message"""
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        
        # Check queue
        method = channel.queue_declare(queue='genotools-jobs', durable=True, passive=True)
        message_count = method.method.message_count
        
        if message_count == 0:
            connection.close()
            return jsonify({"message": "No jobs in queue", "queue_count": 0})
        
        # Get one message
        method, properties, body = channel.basic_get(queue='genotools-jobs', auto_ack=False)
        
        if method:
            job_data = json.loads(body)
            job_id = job_data.get('job_id', 'unknown')
            
            logging.info(f"Manual consume: Got job {job_id}")
            
            # Process it
            dispatch_job_to_worker(job_data)
            
            # Acknowledge
            channel.basic_ack(method.delivery_tag)
            
            connection.close()
            
            return jsonify({
                "success": True,
                "job_id": job_id,
                "message": "Manually consumed and dispatched job"
            })
        else:
            connection.close()
            return jsonify({"message": "No message available"})
            
    except Exception as e:
        logging.error(f"Manual consume error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug/failed-jobs')
def debug_failed_jobs():
    """Check failed jobs queue"""
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        
        # Check both queues
        try:
            method = channel.queue_declare(queue='genotools-jobs-failed', durable=True, passive=True)
            failed_count = method.method.message_count
        except:
            failed_count = 0
        
        connection.close()
        
        return jsonify({
            "failed_jobs_queue": failed_count,
            "description": "Jobs that failed due to user errors or exceeded max retries"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/trigger-job', methods=['POST'])
def trigger_job():
    """Queue a job for processing"""
    try:
        job_id = str(uuid.uuid4())[:8]
        
        job_data = request.json or {}
        analysis_type = job_data.get('analysis_type', 'qc')
        
        job_request = {
            "job_id": job_id,
            "analysis_type": analysis_type,
            "files": job_data.get('files', {}),
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
        
        jobs_queued.inc()
        logging.info(f"Job {job_id} queued for analysis: {analysis_type}")
        
        return jsonify({
            "success": True,
            "job_id": job_id,
            "message": "Job queued successfully"
        })
        
    except Exception as e:
        logging.error(f"Error queuing job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/job-status/<job_id>')
def get_job_status(job_id):
    """Get job status - check controller tracking first, then worker service"""
    
    # Check if job is being tracked by controller
    with job_lock:
        if job_id in current_jobs:
            job_info = current_jobs[job_id].copy()
            logging.info(f"Job {job_id} status: {job_info['status']} (tracked by controller)")
            return jsonify({
                "job_id": job_id,
                "status": job_info['status'],
                "tracked_by": "controller",
                **job_info
            })
    
    # Check GenoTools service for completed jobs
    try:
        response = requests.get(f'http://genotools-service:8080/job-status/{job_id}', timeout=5)
        if response.status_code == 200:
            logging.info(f"Job {job_id} found in GenoTools service")
            return response.json(), 200
        else:
            logging.debug(f"Job {job_id} not found in GenoTools service")
    except Exception as e:
        logging.warning(f"Error checking GenoTools service: {e}")
    
    logging.info(f"Job {job_id} not found anywhere")
    return jsonify({"error": "Job not found"}), 404

@app.route('/debug/queue')
def debug_queue():
    """Debug endpoint to check queue status"""
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        method = channel.queue_declare(queue='genotools-jobs', durable=True, passive=True)
        message_count = method.method.message_count
        connection.close()
        
        with job_lock:
            active_count = len(current_jobs)
        
        return jsonify({
            "queue_name": "genotools-jobs",
            "message_count": message_count,
            "active_jobs": active_count,
            "consumer_running": consumer_running,
            "queue_exists": True
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "consumer_running": consumer_running,
            "queue_exists": False
        }), 500

@app.route('/health')
def health():
    with job_lock:
        active_count = len(current_jobs)
    
    return {
        "status": "healthy", 
        "active_jobs": active_count,
        "consumer_running": consumer_running,
        "component": "controller"
    }, 200

@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': 'text/plain; charset=utf-8'}

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
                if job_id in current_jobs:
                    # Update job status from worker result
                    current_jobs[job_id]['status'] = result.get('status', 'completed')
                    current_jobs[job_id]['success'] = result.get('success', False)
                    current_jobs[job_id]['end_time'] = time.time()
                    current_jobs[job_id]['duration'] = result.get('duration', 0)
                    current_jobs[job_id]['completion_time'] = result.get('completion_time', time.time())
                    current_jobs[job_id]['result'] = result
                    
                    # Update metrics
                    status = 'completed' if result.get('success') else 'failed'
                    jobs_completed.labels(status=status).inc()
                    
                    logging.info(f"Updated job {job_id} status to: {current_jobs[job_id]['status']}")
                    
                    # Schedule cleanup after 5 minutes
                    def cleanup_job():
                        time.sleep(300) 
                        with job_lock:
                            if job_id in current_jobs:
                                del current_jobs[job_id]
                                active_jobs.set(len(current_jobs))
                                logging.info(f"🗑️ Cleaned up tracking for job {job_id}")
                    
                    threading.Thread(target=cleanup_job, daemon=True).start()
                else:
                    logging.warning(f"Received callback for unknown job {job_id}")
            
            active_jobs.set(len(current_jobs))
            return jsonify({"success": True, "message": f"Job {job_id} status updated"})
        else:
            return jsonify({"error": "Missing job_id"}), 400
            
    except Exception as e:
        logging.error(f"Error in job callback: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

def main():
    logging.info("Starting GenoTools Controller (Queue Consumer + Job Dispatcher)...")
    logging.info("Frontend API: port 8080")
    logging.info("Queue consumer: pulling from 'genotools-jobs'")
    logging.info("Job dispatcher: pushing to GenoTools service")
    
    # Start the queue consumer thread
    try:
        consumer_thread = threading.Thread(target=consume_queue, daemon=True, name="QueueConsumer")
        consumer_thread.start()
        logging.info(f"🧵 Consumer thread started: {consumer_thread.name}")
        
        # Wait a moment for consumer to initialize
        time.sleep(3)
        
        if consumer_thread.is_alive():
            logging.info("Consumer thread is running")
        else:
            logging.error("Consumer thread failed to start")
            
    except Exception as e:
        logging.error(f"Failed to start consumer thread: {e}")
    
    # Start Flask app
    logging.info("Starting Flask app...")
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)

if __name__ == "__main__":
    main()