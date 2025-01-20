import os
import subprocess

from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt

import logging
view_logging = logging.getLogger("django_view")
CONFIG_DIR = '../config/'
LOGS_DIR = '../logs/'



def list_configs():
    """Lists all config files in the config directory without file extensions."""
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)  # Ensure the directory exists
    return [os.path.splitext(f)[0] for f in os.listdir(CONFIG_DIR) if os.path.isfile(os.path.join(CONFIG_DIR, f))]

# views.py
from django.shortcuts import render


def job_page(request):
    """View to display the job page with the list of configurations."""
    configs = list_configs()
    return render(request, 'job_page.html', {'configs': configs})


@csrf_exempt
def upload_config(request):
    """Upload a configuration file to the config directory."""
    if request.method == 'POST' and request.FILES.get('config_file'):
        config_file = request.FILES['config_file']
        save_path = os.path.join(CONFIG_DIR, config_file.name)

        with open(save_path, 'wb+') as destination:
            for chunk in config_file.chunks():
                destination.write(chunk)

        return JsonResponse({'success': True, 'message': 'Configuration uploaded successfully!'})

    return JsonResponse({'success': False, 'message': 'Invalid request!'})



def run_config(request, config_name):
    if request.method == 'POST':
        view_logging.info(f"Starting task with config: {config_name}")
        # Define log file paths
        log_file_path = os.path.join(LOGS_DIR, f'{config_name}_log.txt')
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        try:
            # Example: Execute the script with the configuration name
            command = ["python","finalSlowInvest.py",f"{config_name}"]
            with open(log_file_path, 'w') as log_file:
                process = subprocess.Popen(command,cwd="../", shell=True, stdout=log_file, stderr=log_file)
                process.wait()  # Waits for the process to complete
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
            return JsonResponse({
                'success': process.returncode == 0,
                'output': log_content
            })
        except Exception as e:
            view_logging.error(f"Error running task with {config_name}", exc_info=True)
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method'})



def log_viewer(request):
    log_dir = os.path.join(LOGS_DIR)
    logs = []
    if os.path.exists(log_dir):
        for log_file in os.listdir(log_dir):
            file_path = os.path.join(log_dir, log_file)
            if os.path.isfile(file_path):
                logs.append({
                    'name': log_file,
                    'modified': os.path.getmtime(file_path),
                })
    return render(request, 'log_viewer.html', {'logs': logs})
def view_log(request, log_name):
    log_path = os.path.join(LOGS_DIR, log_name)
    if os.path.exists(log_path):
        with open(log_path, 'r') as file:
            content = file.read()
        return JsonResponse({'content': content})
    return JsonResponse({'error': 'Log not found'}, status=404)
def delete_log(request, log_name):
    if request.method == 'DELETE':
        log_path = os.path.join(LOGS_DIR, log_name)
        if os.path.exists(log_path):
            os.remove(log_path)
            return JsonResponse({'success': True})
        return JsonResponse({'success': False, 'error': 'Log not found'}, status=404)
    return JsonResponse({'success': False, 'error': 'Invalid request method'}, status=405)
