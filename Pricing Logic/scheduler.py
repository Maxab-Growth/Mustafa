"""
Pricing Logic Scheduler
=======================
Runs pricing notebooks based on scheduled times (Cairo timezone).

Schedule:
- 8 AM:  Data Extraction + Module 2 (Initial Price Push)
- 12 PM, 3 PM, 6 PM, 9 PM, 11 PM: Module 3 (Periodic Actions)
- 5 AM, 1 PM, 2 PM, 4 PM, 5 PM, 7 PM, 8 PM, 10 PM: Module 4 (Hourly Updates)

Usage:
    python scheduler.py              # Run continuously (checks every minute)
    python scheduler.py --once       # Run once for current hour and exit
    python scheduler.py --test       # Show what would run without executing
"""

import os
import sys
import time
import subprocess
from datetime import datetime
import pytz

# =============================================================================
# CONFIGURATION
# =============================================================================
CAIRO_TZ = pytz.timezone('Africa/Cairo')

# Base directory (where this script is located)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULES_DIR = os.path.join(BASE_DIR, 'modules')

# Notebook paths
NOTEBOOKS = {
    'data_extraction': os.path.join(BASE_DIR, 'data_extraction.ipynb'),
    'module_2': os.path.join(MODULES_DIR, 'module_2_initial_price_push.ipynb'),
    'module_3': os.path.join(MODULES_DIR, 'module_3_periodic_actions.ipynb'),
    'module_4': os.path.join(MODULES_DIR, 'module_4_hourly_updates.ipynb'),
}

# Schedule configuration (hours in 24h format)
SCHEDULE = {
    5:  ['module_4'],                          # 5 AM
    8:  ['data_extraction', 'module_2'],       # 8 AM
    12: ['module_3'],                          # 12 PM
    13: ['module_4'],                          # 1 PM
    14: ['module_4'],                          # 2 PM
    15: ['module_3'],                          # 3 PM
    16: ['module_4'],                          # 4 PM
    17: ['module_4'],                          # 5 PM
    18: ['module_3'],                          # 6 PM
    19: ['module_4'],                          # 7 PM
    20: ['module_4'],                          # 8 PM
    21: ['module_3'],                          # 9 PM
    22: ['module_4'],                          # 10 PM
    23: ['module_3'],                          # 11 PM
}

# Log file
LOG_FILE = os.path.join(BASE_DIR, 'scheduler.log')

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def get_cairo_time():
    """Get current time in Cairo timezone."""
    return datetime.now(CAIRO_TZ)

def log_message(message, also_print=True):
    """Log a message to file and optionally print to console."""
    timestamp = get_cairo_time().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {message}"
    
    if also_print:
        print(log_line)
    
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_line + '\n')
    except Exception as e:
        print(f"Warning: Could not write to log file: {e}")

def run_notebook(notebook_name, test_mode=False):
    """
    Run a Jupyter notebook using nbconvert.
    Returns True if successful, False otherwise.
    """
    notebook_path = NOTEBOOKS.get(notebook_name)
    
    if not notebook_path:
        log_message(f"ERROR: Unknown notebook: {notebook_name}")
        return False
    
    if not os.path.exists(notebook_path):
        log_message(f"ERROR: Notebook not found: {notebook_path}")
        return False
    
    if test_mode:
        log_message(f"TEST MODE: Would run {notebook_name} ({notebook_path})")
        return True
    
    log_message(f"Starting: {notebook_name}")
    
    try:
        # Run notebook using jupyter nbconvert --execute
        # This executes the notebook in place
        cmd = [
            'jupyter', 'nbconvert',
            '--to', 'notebook',
            '--execute',
            '--inplace',
            '--ExecutePreprocessor.timeout=3600',  # 1 hour timeout
            notebook_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(notebook_path)
        )
        
        if result.returncode == 0:
            log_message(f"SUCCESS: {notebook_name} completed")
            return True
        else:
            log_message(f"ERROR: {notebook_name} failed with code {result.returncode}")
            log_message(f"STDERR: {result.stderr[:500] if result.stderr else 'None'}")
            return False
            
    except Exception as e:
        log_message(f"ERROR: Exception running {notebook_name}: {str(e)}")
        return False

def get_scheduled_notebooks(hour):
    """Get list of notebooks scheduled for a given hour."""
    return SCHEDULE.get(hour, [])

def run_scheduled_tasks(hour, test_mode=False):
    """Run all notebooks scheduled for a given hour."""
    notebooks = get_scheduled_notebooks(hour)
    
    if not notebooks:
        log_message(f"No notebooks scheduled for hour {hour}")
        return
    
    log_message(f"{'='*60}")
    log_message(f"Running scheduled tasks for hour {hour}:00")
    log_message(f"Notebooks: {', '.join(notebooks)}")
    log_message(f"{'='*60}")
    
    results = {}
    for notebook in notebooks:
        success = run_notebook(notebook, test_mode=test_mode)
        results[notebook] = success
        
        # Small delay between notebooks
        if not test_mode and len(notebooks) > 1:
            time.sleep(5)
    
    # Summary
    log_message(f"\n{'='*60}")
    log_message(f"SUMMARY for hour {hour}:00")
    for notebook, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        log_message(f"  {notebook}: {status}")
    log_message(f"{'='*60}\n")

def print_schedule():
    """Print the full schedule."""
    print("\n" + "="*60)
    print("PRICING LOGIC SCHEDULER - SCHEDULE")
    print("="*60)
    print(f"{'Hour':<10} {'Notebooks':<50}")
    print("-"*60)
    
    for hour in sorted(SCHEDULE.keys()):
        notebooks = SCHEDULE[hour]
        hour_str = f"{hour:02d}:00"
        notebooks_str = ', '.join(notebooks)
        print(f"{hour_str:<10} {notebooks_str:<50}")
    
    print("="*60)
    print(f"\nAll times are in Cairo timezone ({CAIRO_TZ})")
    print(f"Current Cairo time: {get_cairo_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

# =============================================================================
# MAIN SCHEDULER LOOP
# =============================================================================
def run_continuous():
    """Run the scheduler continuously, checking every minute."""
    log_message("Scheduler started in continuous mode")
    print_schedule()
    
    last_run_hour = None
    
    while True:
        try:
            now = get_cairo_time()
            current_hour = now.hour
            current_minute = now.minute
            
            # Run at the beginning of each scheduled hour (minute 0-2)
            # and only if we haven't run for this hour yet
            if current_minute <= 2 and current_hour != last_run_hour:
                if current_hour in SCHEDULE:
                    run_scheduled_tasks(current_hour)
                    last_run_hour = current_hour
            
            # Reset last_run_hour when minute passes 2
            # This allows re-running if the script restarts
            if current_minute > 2 and last_run_hour == current_hour:
                pass  # Keep last_run_hour to prevent re-running same hour
            
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            
        except KeyboardInterrupt:
            log_message("Scheduler stopped by user (Ctrl+C)")
            break
        except Exception as e:
            log_message(f"ERROR in main loop: {str(e)}")
            time.sleep(60)  # Wait a minute before retrying

def run_once():
    """Run once for the current hour and exit."""
    now = get_cairo_time()
    current_hour = now.hour
    
    log_message(f"Running once for current hour ({current_hour}:00)")
    
    if current_hour in SCHEDULE:
        run_scheduled_tasks(current_hour)
    else:
        log_message(f"No notebooks scheduled for hour {current_hour}")

def run_test():
    """Test mode - show what would run without executing."""
    print_schedule()
    
    now = get_cairo_time()
    current_hour = now.hour
    
    print(f"\nTEST MODE - Current hour: {current_hour}:00")
    print("-"*40)
    
    if current_hour in SCHEDULE:
        run_scheduled_tasks(current_hour, test_mode=True)
    else:
        print(f"No notebooks scheduled for hour {current_hour}")

# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == '__main__':
    # Parse command line arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg == '--once':
            run_once()
        elif arg == '--test':
            run_test()
        elif arg == '--schedule':
            print_schedule()
        elif arg == '--help' or arg == '-h':
            print(__doc__)
        else:
            print(f"Unknown argument: {arg}")
            print("Use --help for usage information")
    else:
        # Default: run continuously
        run_continuous()

