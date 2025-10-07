#!/usr/bin/env python3
"""
Flask web application for trading data visualization - CSV-only version.
Provides real-time charts and metrics without database dependency.
"""

from flask import Flask, render_template, jsonify, request, send_file, make_response
from flask_cors import CORS
from flask_compress import Compress
import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime, timedelta, timezone
import threading
import time
from pathlib import Path
# Removed GCS sync import - portfolio version
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import statistics
# Removed OKX import - portfolio version

app = Flask(__name__)
CORS(app)
Compress(app)

# Configure logging with rotation
from logging.handlers import RotatingFileHandler

# Create rotating file handler for web app
web_rotating_handler = RotatingFileHandler(
    'web_app.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,  # Keep 5 old versions
    encoding='utf-8'
)
web_rotating_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        web_rotating_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OKX integration disabled for now

# Global data cache
DATA_CACHE = {}
LAST_MODIFIED = {}
CACHE_LOCK = threading.Lock()
# Per-cache-key locks for finer-grained locking
CACHE_KEY_LOCKS = {}

# Thread pool for parallel operations
MAX_WORKERS = min(multiprocessing.cpu_count() * 2, 8)
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

def get_cache_key_lock(cache_key):
    """Get or create a lock for a specific cache key."""
    if cache_key not in CACHE_KEY_LOCKS:
        with CACHE_LOCK:  # Brief global lock to create per-key lock
            if cache_key not in CACHE_KEY_LOCKS:
                CACHE_KEY_LOCKS[cache_key] = threading.Lock()
    return CACHE_KEY_LOCKS[cache_key]

# Configuration
LOCAL_ROOT = "./buckets"

# Portfolio demo - no sync needed
data_version = 1  # Static version for portfolio
last_data_update = datetime.now()  # Portfolio demo timestamp


def get_symbol_from_json(bucket_path, ts_id):
    """Get hedge_symbol from TS-{ID}.json file."""
    metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                
                # Check in models array first (nested structure)
                models = metadata.get('models', [])
                hedge_symbol = None
                
                if models and len(models) > 0:
                    model_args = models[0].get('args', {})
                    hedge_symbol = model_args.get('hedge_symbol', '')
                
                # If not found in models, check top level
                if not hedge_symbol:
                    hedge_symbol = metadata.get('hedge_symbol', '')
                
                return hedge_symbol
        except Exception as e:
            logger.warning(f"Error reading metadata file {metadata_file}: {e}")
    return None




def load_csv_data_from_path(csv_path):
    """Load CSV data from a specific file path."""
    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found: {csv_path}")
        return pd.DataFrame()
    
    try:
        # Read CSV with specific columns to avoid duplicates
        # First, get all columns
        df_temp = pd.read_csv(csv_path, nrows=1)
        all_columns = df_temp.columns.tolist()
        
        # Select only the columns we need
        required_columns = []
        
        # Map columns carefully to avoid duplicates
        if 'Close' in all_columns:
            required_columns.append('Close')
        elif 'close' in all_columns and 'Close' not in all_columns:
            required_columns.append('close')
            
        if 'Close time' in all_columns:
            required_columns.append('Close time')
        elif 'datetime' in all_columns:
            required_columns.append('datetime')
        elif 'timestamp' in all_columns:
            required_columns.append('timestamp')
            
        if 'Position' in all_columns:
            required_columns.append('Position')
        elif 'position' in all_columns:
            required_columns.append('position')
            
        # Always include calculated columns if they exist
        for col in ['cumulative_return', 'returns', 'position_lag', 'strategy_returns']:
            if col in all_columns:
                required_columns.append(col)
        
        # Read only required columns
        df = pd.read_csv(csv_path, usecols=required_columns)
        
        # Rename columns for consistency
        column_mapping = {
            'Close time': 'timestamp',
            'datetime': 'timestamp',
            'Close': 'close',
            'Position': 'position'
        }
        
        # Only rename columns that exist
        rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
        df = df.rename(columns=rename_dict)
        
        # Convert timestamp to datetime with format mixed
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Calculate cumulative returns if not already present and position column exists
        if 'cumulative_return' not in df.columns and 'position' in df.columns and 'close' in df.columns:
            # Calculate returns
            df['returns'] = df['close'].pct_change().fillna(0)
            
            # Shift position to align with returns (position at time t affects return from t to t+1)
            df['position_lag'] = df['position'].shift(1).fillna(0)
            
            # Calculate strategy returns (returns when in position)
            df['strategy_returns'] = df['returns'] * df['position_lag']
            
            # Replace any NaN or inf values with 0
            df['strategy_returns'] = df['strategy_returns'].replace([np.inf, -np.inf], 0).fillna(0)
            
            # Calculate cumulative return
            df['cumulative_return'] = (1 + df['strategy_returns']).cumprod() - 1
            # Hard cap cumulative returns at ±100%
            df['cumulative_return'] = df['cumulative_return'].clip(lower=-0.99, upper=1.0)
            
            # Clean up any remaining NaN values
            df['cumulative_return'] = df['cumulative_return'].fillna(0)
            
            # Calculate fee-adjusted returns
            # Load transaction fee from config
            try:
                with open('config.json', 'r') as f:
                    config = json.load(f)
                    fee_percent = config.get('transaction_fee_percent', 0.05)
            except:
                fee_percent = 0.05  # Default to 0.05%
            
            # Identify position changes
            df['position_change'] = df['position'].diff()
            df['is_trade'] = df['position_change'] != 0
            df['position_change_abs'] = df['position_change'].abs()
            
            # Calculate transaction costs as a fraction of returns
            df['fee_multiplier'] = 0.0
            df.loc[df['is_trade'], 'fee_multiplier'] = (fee_percent / 100) * df.loc[df['is_trade'], 'position_change_abs']
            
            # Apply transaction costs
            df['strategy_returns_after_fees'] = df['strategy_returns'] - df['fee_multiplier']
            
            # Calculate cumulative return after fees
            df['cumulative_return_after_fees'] = (1 + df['strategy_returns_after_fees']).cumprod() - 1
            # Hard cap cumulative returns at ±100%
            df['cumulative_return_after_fees'] = df['cumulative_return_after_fees'].clip(lower=-0.99, upper=1.0)
            df['cumulative_return_after_fees'] = df['cumulative_return_after_fees'].fillna(0)
            
            # Add transaction cost column for display
            df['transaction_cost'] = df['fee_multiplier']
            
            logger.info(f"Calculated cumulative returns with fees for {os.path.basename(csv_path)}")
        
        logger.info(f"Loaded {len(df)} rows from {os.path.basename(csv_path)}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading CSV from {csv_path}: {e}")
        return pd.DataFrame()


def refresh_data_cache():
    """Refresh the data cache by reloading CSV files with per-key locking."""
    global DATA_CACHE, LAST_MODIFIED, data_version, last_data_update
    
    # Get list of cache keys to process (snapshot to avoid modification during iteration)
    with CACHE_LOCK:
        cache_keys_to_process = list(DATA_CACHE.keys())
    
    # Process all cache keys with per-key locking
    for cache_key in cache_keys_to_process:
        if '/' in cache_key and not cache_key.startswith('gs://'):  # bucket/TS-ID key
            cache_key_lock = get_cache_key_lock(cache_key)
            
            with cache_key_lock:  # Lock only this specific cache entry
                bucket, ts_id_part = cache_key.split('/', 1)
                bucket_path = os.path.join(LOCAL_ROOT, bucket)
                
                # Extract TS-ID from cache key (format: "bucket/TS-X")
                if ts_id_part.startswith('TS-'):
                    ts_id = ts_id_part[3:]  # Remove "TS-" prefix
                    
                    # Find the CSV file for this TS-ID directly
                    pattern = re.compile(rf'STGC2OGTrim2Model_TS-{ts_id}_T-(\d+)_.*\.csv')
                    
                    if os.path.exists(bucket_path):
                        for filename in os.listdir(bucket_path):
                            if filename.endswith('.csv') and not filename.endswith('_PH.csv'):
                                match = pattern.match(filename)
                                if match:
                                    csv_path = os.path.join(bucket_path, filename)
                                    try:
                                        current_mtime = os.path.getmtime(csv_path)
                                        if LAST_MODIFIED.get(cache_key, 0) < current_mtime:
                                            df = load_csv_data_from_path(csv_path)
                                            if not df.empty:
                                                DATA_CACHE[cache_key] = df
                                                LAST_MODIFIED[cache_key] = current_mtime
                                                logger.info(f"Updated cache for {cache_key}, {len(df)} rows")
                                                data_version += 1
                                                last_data_update = datetime.utcnow()
                                    except Exception as e:
                                        logger.error(f"Error refreshing cache for {cache_key}: {e}")
                                    break


def calculate_metrics(df):
    """Calculate metrics from dataframe."""
    metrics = {}
    
    if len(df) > 0:
        # Last values
        metrics['last_price'] = float(df['close'].iloc[-1]) if 'close' in df.columns else float(df['Close'].iloc[-1])
        metrics['last_position'] = float(df['position'].iloc[-1]) if 'position' in df.columns else float(df['Position'].iloc[-1]) if 'Position' in df.columns else 0
        
        # Cumulative return
        if 'cumulative_return' in df.columns:
            metrics['cumulative_return'] = float(df['cumulative_return'].iloc[-1] * 100)  # Convert to percentage
            metrics['max_return'] = float(df['cumulative_return'].max() * 100)
        else:
            metrics['cumulative_return'] = 0.0
            metrics['max_return'] = 0.0
            
        # Cumulative return after fees
        if 'cumulative_return_after_fees' in df.columns:
            metrics['cumulative_return_after_fees'] = float(df['cumulative_return_after_fees'].iloc[-1] * 100)
            metrics['max_return_after_fees'] = float(df['cumulative_return_after_fees'].max() * 100)
            # Calculate total fees paid
            metrics['total_fees'] = metrics['cumulative_return'] - metrics['cumulative_return_after_fees']
        else:
            metrics['cumulative_return_after_fees'] = metrics.get('cumulative_return', 0.0)
            metrics['max_return_after_fees'] = metrics.get('max_return', 0.0)
            metrics['total_fees'] = 0.0
        
        # Data points info
        metrics['total_points'] = len(df)
        metrics['displayed_points'] = min(len(df), 5000)  # We display up to 5000 points
        
        # Last timestamp - handle both 'timestamp' and 'datetime' columns
        timestamp_col = 'timestamp' if 'timestamp' in df.columns else 'datetime'
        metrics['last_timestamp'] = df[timestamp_col].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
    
    return metrics


def get_resampled_data(df, resample_period=None):
    """
    Resample data based on size or specified period.
    Returns resampled dataframe suitable for charting.
    """
    if df.empty:
        return df
    
    total_rows = len(df)
    
    # Auto-determine resampling period if not specified
    if resample_period is None:
        if total_rows > 50000:  # > 1 month of minute data
            resample_period = '1h'  # Hourly
        elif total_rows > 20000:  # > 2 weeks
            resample_period = '30min'  # 30 minutes
        elif total_rows > 10000:  # > 1 week
            resample_period = '15min'  # 15 minutes
        elif total_rows > 5000:  # > 3 days
            resample_period = '5min'  # 5 minutes
        else:
            return df  # No resampling needed
    
    # Set timestamp as index for resampling - handle both 'timestamp' and 'datetime' columns
    df_copy = df.copy()
    timestamp_col = 'timestamp' if 'timestamp' in df_copy.columns else 'datetime'
    df_copy.set_index(timestamp_col, inplace=True)
    
    # Resample with appropriate aggregation
    agg_dict = {}
    
    # Handle both uppercase and lowercase column names
    if 'close' in df_copy.columns:
        agg_dict['close'] = 'last'
    elif 'Close' in df_copy.columns:
        agg_dict['Close'] = 'last'
        
    if 'position' in df_copy.columns:
        agg_dict['position'] = 'last'
    elif 'Position' in df_copy.columns:
        agg_dict['Position'] = 'last'
        
    if 'cumulative_return' in df_copy.columns:
        agg_dict['cumulative_return'] = 'last'
    
    # Add fee-adjusted returns if they exist
    if 'cumulative_return_after_fees' in df_copy.columns:
        agg_dict['cumulative_return_after_fees'] = 'last'
        
    resampled = df_copy.resample(resample_period).agg(agg_dict).dropna()
    
    # Reset index to get timestamp back as column
    resampled.reset_index(inplace=True)
    
    logger.info(f"Resampled {total_rows} rows to {len(resampled)} rows with {resample_period} period")
    return resampled


# Portfolio demo - sync functions removed


@app.route('/')
def landing():
    """Main dashboard - Modern UI with advanced visual effects"""
    return render_template('modern.html')

@app.route('/symbol/<path:symbol_id>')
def symbol_detail(symbol_id):
    """Main symbol detail view - Modern UI with enhanced visual effects"""
    return render_template('modern_detail.html')

@app.route('/modern/symbol/<path:symbol_id>')
def symbol_detail_modern(symbol_id):
    """Modern detail view with charts for a specific symbol"""
    global DATA_CACHE
    
    # Parse the symbol_id to extract bucket and ts_id
    parts = symbol_id.split('/')
    if len(parts) >= 2:
        bucket = parts[0]
        ts_id = parts[1].replace('TS-', '')
        
        # Try to find the symbol name from the cached data
        symbol_name = 'Unknown'
        cache_key = f"{bucket}/TS-{ts_id}"
        
        # Check if we have data for this symbol in cache
        if cache_key in DATA_CACHE:
            cached_data = DATA_CACHE[cache_key]
            if 'symbol' in cached_data:
                symbol_name = cached_data['symbol']
        else:
            # Try to discover the symbol from the bucket
            bucket_path = os.path.join(LOCAL_ROOT, bucket)
            if os.path.exists(bucket_path):
                # Look for the TS-ID.json file
                metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
                if os.path.exists(metadata_file):
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                            models = metadata.get('models', [])
                            if models and len(models) > 0:
                                model_args = models[0].get('args', {})
                                hedge_symbol = model_args.get('hedge_symbol', '')
                                if hedge_symbol and '-' in hedge_symbol:
                                    symbol_name = hedge_symbol.split('-')[0]
                    except:
                        pass
        
        return render_template('modern_detail.html',
                             bucket=bucket,
                             ts_id=ts_id,
                             symbol=symbol_name)
    
    # Fallback to modern detail template with basic info
    return render_template('modern_detail.html',
                         bucket='',
                         ts_id='',
                         symbol='Unknown')


@app.route('/api/symbols')
def get_symbols():
    """Get all available symbols across all buckets."""
    all_symbols = []
    
    # Load config to get buckets
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            buckets = config.get('buckets', [])
    except:
        buckets = []
    
    # Process buckets in parallel
    def process_bucket(bucket):
        bucket_name = bucket.replace('gs://', '').replace('/', '_').rstrip('_')
        bucket_path = os.path.join(LOCAL_ROOT, bucket_name)
        
        if os.path.exists(bucket_path):
            symbols = discover_symbols_in_bucket(bucket_path)
            for sym in symbols:
                sym['bucket'] = bucket_name
            return symbols
        return []
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_bucket, bucket) for bucket in buckets]
        
        for future in as_completed(futures):
            symbols = future.result()
            all_symbols.extend(symbols)
    
    # Group by symbol name for compatibility
    symbol_dict = {}
    for sym in all_symbols:
        name = sym['symbol']
        if name not in symbol_dict:
            symbol_dict[name] = []
        symbol_dict[name].append(sym)
    
    return jsonify({
        'symbols': list(symbol_dict.keys()),
        'detailed': symbol_dict
    })






@app.route('/api/sync/status/buckets')
def get_sync_status_buckets():
    """Get sync status with bucket-level freshness information."""
    bucket_status = {}
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            buckets = config.get('buckets', [])
    except:
        buckets = []
    
    # Process buckets in parallel
    def process_bucket_status(bucket):
        bucket_name = bucket.replace('gs://', '').replace('/', '_').rstrip('_')
        bucket_path = os.path.join(LOCAL_ROOT, bucket_name)
        
        fresh = 0
        stale = 0
        very_stale = 0
        
        if os.path.exists(bucket_path):
            symbols = discover_symbols_in_bucket(bucket_path)
            
            for sym in symbols:
                if sym['freshness'] == 'fresh':
                    fresh += 1
                elif sym['freshness'] == 'stale':
                    stale += 1
                else:
                    very_stale += 1
        
        return bucket_name, {
            'fresh': fresh,
            'stale': stale,
            'very_stale': very_stale,
            'total': fresh + stale + very_stale,
            'fresh_count': fresh,
            'total_symbols': fresh + stale + very_stale,
            'display_name': bucket_name.replace('_', ' ').title(),
            'overall_status': 'healthy' if fresh == (fresh + stale + very_stale) and fresh > 0 else 'warning' if stale > 0 else 'critical' if very_stale > 0 else 'unknown'
        }
    
    # Execute in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_bucket_status, bucket) for bucket in buckets]
        
        for future in as_completed(futures):
            bucket_name, status = future.result()
            bucket_status[bucket_name] = status
    
    # Calculate totals
    total_fresh = sum(b['fresh'] for b in bucket_status.values())
    total_stale = sum(b['stale'] for b in bucket_status.values())
    total_very_stale = sum(b['very_stale'] for b in bucket_status.values())
    
    # Calculate next sync time
    next_sync_time = None
    if last_sync_time:
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                interval = config.get('interval', 300)
                next_sync_time = (last_sync_time + timedelta(seconds=interval)).isoformat()
        except:
            pass
    
    total_symbols = total_fresh + total_stale + total_very_stale
    percentage_fresh = round((total_fresh / total_symbols * 100) if total_symbols > 0 else 0, 1)
    
    return jsonify({
        'sync_in_progress': sync_in_progress,
        'thread_running': sync_running,  # Add this for frontend compatibility
        'currently_syncing_bucket': currently_syncing_bucket,
        'last_sync': last_sync_time.isoformat() if last_sync_time else None,
        'next_sync': next_sync_time,
        'buckets': bucket_status,
        'totals': {
            'fresh': total_fresh,
            'stale': total_stale,
            'very_stale': total_very_stale,
            'total': total_symbols
        },
        'summary': {
            'total_buckets': len(bucket_status),
            'total_symbols': total_symbols,
            'total_fresh': total_fresh,
            'percentage_fresh': percentage_fresh
        }
    })


@app.route('/api/data/version')
def get_data_version():
    """Get current data version to check for updates."""
    return jsonify({
        'version': data_version,
        'last_update': last_data_update.isoformat() if last_data_update else None
    })


@app.route('/api/sync/status')
def get_sync_status_portfolio():
    """Return static sync status for portfolio demo."""
    return jsonify({
        'sync_enabled': True,
        'sync_healthy': True,
        'sync_in_progress': False,
        'last_sync': datetime.now(timezone.utc).isoformat(),
        'data_version': 1,
        'message': 'Portfolio demo - static data'
    })




@app.route('/api/buckets')
def get_buckets():
    """Return all available bucket directories."""
    buckets = []
    
    if os.path.exists(LOCAL_ROOT):
        for item in os.listdir(LOCAL_ROOT):
            item_path = os.path.join(LOCAL_ROOT, item)
            # Skip hidden files and non-directories
            if os.path.isdir(item_path) and not item.startswith('.'):
                buckets.append({
                    'name': item,
                    'path': item,
                    'display_name': item.replace('_', ' ').title()
                })
    
    return jsonify(buckets)


def discover_symbols_in_bucket(bucket_path):
    """Discover all symbols in a bucket directory and return metadata."""
    symbols = []
    pattern = re.compile(r'STGC2OGTrim2Model_TS-(\d+)_T-(\d+)_.*\.csv')
    
    # Function to get symbol from JSON metadata
    def get_symbol_from_json(bucket_path, ts_id):
        """Get hedge_symbol from TS-{ID}.json file."""
        metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                    # Check in models array first (nested structure)
                    models = metadata.get('models', [])
                    hedge_symbol = None
                    
                    if models and len(models) > 0:
                        model_args = models[0].get('args', {})
                        hedge_symbol = model_args.get('hedge_symbol', '')
                    
                    # If not found in models, check top level
                    if not hedge_symbol:
                        hedge_symbol = metadata.get('hedge_symbol', '')
                    
                    return hedge_symbol
            except Exception as e:
                logger.warning(f"Error reading metadata file {metadata_file}: {e}")
        return None
    
    for filename in os.listdir(bucket_path):
        if filename.endswith('.csv') and not filename.endswith('_PH.csv'):
            match = pattern.match(filename)
            if match:
                ts_id = match.group(1)
                t_id = match.group(2)
                
                # Get symbol name from JSON metadata
                hedge_symbol = get_symbol_from_json(bucket_path, ts_id)
                symbol_name = None
                if hedge_symbol:
                    # Extract symbol from hedge_symbol format like "BTC-USD-SWAP"
                    parts = hedge_symbol.split('-')
                    if parts:
                        symbol_name = parts[0]
                        logger.info(f"From JSON: TS-{ts_id} -> {symbol_name} (hedge_symbol: {hedge_symbol})")
                
                # Fallback to config mapping if JSON doesn't provide symbol
                if not symbol_name:
                    try:
                        with open('config.json', 'r') as f:
                            config = json.load(f)
                            ts_id_mapping = config.get('ts_id_mapping', {})
                            symbol_name = ts_id_mapping.get(ts_id, f'TS-{ts_id}')
                    except:
                        symbol_name = f'TS-{ts_id}'
                
                # Get freshness info
                file_path = os.path.join(bucket_path, filename)
                file_stat = os.stat(file_path)
                last_modified = datetime.fromtimestamp(file_stat.st_mtime)
                now = datetime.now()
                minutes_ago = int((now - last_modified).total_seconds() / 60)
                hours_ago = minutes_ago / 60
                
                # Portfolio demo: always show as fresh
                freshness = 'fresh'
                
                # Extract pair information from hedge_symbol
                pair = 'USD'  # default
                if hedge_symbol:
                    parts = hedge_symbol.split('-')
                    if len(parts) >= 2:
                        pair = parts[1]  # USD, USDT, etc.
                
                symbols.append({
                    'symbol': symbol_name,
                    'pair': pair,
                    'ts_id': ts_id,
                    't_id': t_id,
                    'filename': filename,
                    'freshness': freshness,
                    'last_update': last_modified.strftime('%H:%M:%S'),
                    'minutes_ago': minutes_ago
                })
    
    return symbols


@app.route('/api/buckets/<bucket>/symbols')
def get_bucket_symbols(bucket):
    """Get all symbols in a specific bucket with metadata. Includes retry logic for sync race conditions."""
    bucket_path = os.path.join(LOCAL_ROOT, bucket)
    
    if not os.path.exists(bucket_path):
        return jsonify({'error': 'Bucket not found'}), 404
    
    symbols = []
    pattern = re.compile(r'STGC2OGTrim2Model_TS-(\d+)_T-(\d+)_.*\.csv')
    
    # Function to get symbol from JSON metadata
    def get_symbol_from_json(bucket_path, ts_id):
        """Get hedge_symbol from TS-{ID}.json file."""
        metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                    # Check in models array first (nested structure)
                    models = metadata.get('models', [])
                    hedge_symbol = None
                    
                    if models and len(models) > 0:
                        model_args = models[0].get('args', {})
                        hedge_symbol = model_args.get('hedge_symbol', '')
                    
                    # If not found in models, check top level
                    if not hedge_symbol:
                        hedge_symbol = metadata.get('hedge_symbol', '')
                    
                    return hedge_symbol
            except Exception as e:
                logger.warning(f"Could not read metadata for TS-{ts_id}: {e}")
        return None
    
    # Load sync state for freshness info
    sync_state = {}
    try:
        sync_state_path = os.path.join(LOCAL_ROOT, '.sync_state.json')
        if os.path.exists(sync_state_path):
            with open(sync_state_path, 'r') as f:
                sync_data = json.load(f)
                sync_state = sync_data.get('files', {})
    except:
        pass
    
    # Scan bucket directory with retry logic for race conditions during sync
    max_retries = 3
    for attempt in range(max_retries):
        try:
            filenames = os.listdir(bucket_path)
            break
        except (OSError, FileNotFoundError) as e:
            if attempt == max_retries - 1:
                logger.warning(f"Failed to list directory {bucket_path} after {max_retries} attempts: {e}")
                return jsonify([])  # Return empty list instead of error
            time.sleep(0.1)  # Brief pause before retry
    
    for filename in filenames:
        if filename.endswith('.csv') and not filename.endswith('_PH.csv'):  # Skip placeholder files
            try:
                match = pattern.match(filename)
                if match:
                    ts_id = match.group(1)
                    t_id = match.group(2)
                    
                    # Get symbol from JSON metadata with error handling
                    hedge_symbol = get_symbol_from_json(bucket_path, ts_id)
                    if hedge_symbol:
                        # Parse hedge_symbol format: "BTC-USD-SWAP" -> base="BTC", quote="USD"
                        parts = hedge_symbol.split('-')
                        if len(parts) >= 2:
                            symbol_name = parts[0]  # Base symbol (BTC, ETH, LTC, SOL, etc.)
                        else:
                            symbol_name = hedge_symbol  # Fallback to full string
                    else:
                        symbol_name = f'Unknown-{ts_id}'
                    
                    file_path = os.path.join(bucket_path, filename)
                    try:
                        file_stat = os.stat(file_path)
                        
                        # Calculate data freshness
                        last_modified = datetime.fromtimestamp(file_stat.st_mtime)
                        hours_old = (datetime.now() - last_modified).total_seconds() / 3600
                    except (OSError, FileNotFoundError):
                        # File might have been deleted/moved during sync, skip it
                        continue
                    
                    if hours_old <= 1.167:  # Up to 1 hour 10 minutes is fresh
                        status = 'fresh'
                    elif hours_old <= 2.167:  # Up to 2 hours 10 minutes is stale
                        status = 'stale'
                    else:
                        status = 'very_stale'
                    
                    # Extract pair information from hedge_symbol
                    pair = None
                    display_name = symbol_name
                    if hedge_symbol:
                        # Parse hedge_symbol format: "BTC-USD-SWAP" -> quote="USD"
                        parts = hedge_symbol.split('-')
                        if len(parts) >= 2:
                            pair = parts[1]  # USD, USDT, etc.
                            display_name = f"{symbol_name} ({pair})"
                            logger.info(f"Found pair {pair} for {symbol_name} from hedge_symbol: {hedge_symbol}")
                    
                    symbols.append({
                        'symbol': symbol_name,
                        'display_name': display_name,
                        'pair': pair,
                        'ts_id': ts_id,
                        't_id': t_id,
                        'filename': filename,
                        'size_mb': round(file_stat.st_size / (1024 * 1024), 2),
                        'last_modified': last_modified.isoformat(),
                        'hours_old': round(hours_old, 1),
                        'status': status
                    })
            except Exception as e:
                # Log but continue if there's an issue with a specific file
                logger.warning(f"Error processing file {filename} in bucket {bucket}: {e}")
                continue
    
    # Sort by symbol name
    symbols.sort(key=lambda x: x['symbol'])
    
    return jsonify(symbols)


@app.route('/api/data/<bucket>/<symbol>')
def get_bucket_data(bucket, symbol):
    """Get data for a specific symbol from a specific bucket.
    Symbol can be either a symbol name (e.g., 'BTC') or a TS-ID (e.g., 'TS-1').
    When multiple files have the same symbol (e.g., BTC-USD and BTC-USDT), use TS-ID.
    
    Query parameters:
    - resolution: 'minutely' or 'hourly' (default: 'hourly')
    """
    global data_version, last_data_update
    bucket_path = os.path.join(LOCAL_ROOT, bucket)
    
    if not os.path.exists(bucket_path):
        return jsonify({'error': 'Bucket not found'}), 404
    
    # Original CSV logic
    # Find the CSV file for this symbol in the bucket using JSON-based mapping
    csv_file = None
    pattern = re.compile(r'STGC2OGTrim2Model_TS-(\d+)_T-(\d+)_.*\.csv')
    
    # Check if symbol is a TS-ID (e.g., 'TS-1' or just '1')
    is_ts_id = symbol.startswith('TS-') or symbol.isdigit()
    if symbol.startswith('TS-'):
        target_ts_id = symbol[3:]
    elif symbol.isdigit():
        target_ts_id = symbol
    else:
        target_ts_id = None
    
    # Function to get symbol from JSON metadata (same as in get_bucket_symbols)
    def get_symbol_from_json(bucket_path, ts_id):
        """Get hedge_symbol from TS-{ID}.json file."""
        metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                    # Check in models array first (nested structure)
                    models = metadata.get('models', [])
                    hedge_symbol = None
                    
                    if models and len(models) > 0:
                        model_args = models[0].get('args', {})
                        hedge_symbol = model_args.get('hedge_symbol', '')
                    
                    # If not found in models, check top level
                    if not hedge_symbol:
                        hedge_symbol = metadata.get('hedge_symbol', '')
                    
                    return hedge_symbol
            except Exception as e:
                logger.warning(f"Could not read metadata for TS-{ts_id}: {e}")
        return None
    
    # Find the file for this symbol
    symbol_found = None  # Track the actual symbol name for cache key
    for filename in os.listdir(bucket_path):
        if filename.endswith('.csv') and not filename.endswith('_PH.csv'):  # Skip placeholder files
            match = pattern.match(filename)
            if match:
                ts_id = match.group(1)
                
                # If looking for a specific TS-ID, check that first
                if is_ts_id and ts_id == target_ts_id:
                    csv_file = filename
                    # Get the actual symbol name for the cache key
                    hedge_symbol = get_symbol_from_json(bucket_path, ts_id)
                    if hedge_symbol:
                        parts = hedge_symbol.split('-')
                        symbol_found = parts[0] if len(parts) >= 2 else hedge_symbol
                    else:
                        symbol_found = f'Unknown-{ts_id}'
                    break
                
                # Otherwise, match by symbol name
                if not is_ts_id:
                    # Get symbol from JSON metadata
                    hedge_symbol = get_symbol_from_json(bucket_path, ts_id)
                    if hedge_symbol:
                        # Parse hedge_symbol format: "BTC-USD-SWAP" -> base="BTC"
                        parts = hedge_symbol.split('-')
                        if len(parts) >= 2:
                            symbol_name = parts[0]  # Base symbol (BTC, ETH, LTC, SOL, etc.)
                        else:
                            symbol_name = hedge_symbol  # Fallback to full string
                    else:
                        symbol_name = f'Unknown-{ts_id}'
                    
                    if symbol_name == symbol:
                        csv_file = filename
                        symbol_found = symbol_name
                        break
    
    if not csv_file:
        return jsonify({'error': f'Symbol {symbol} not found in bucket {bucket}'}), 404
    
    # Create cache key with bucket and TS-ID to ensure uniqueness
    # This prevents cache collisions between different trading pairs of the same symbol
    if is_ts_id:
        # Normalize to always use TS-X format for cache key
        if symbol.isdigit():
            cache_key = f"{bucket}/TS-{symbol}"
        else:
            cache_key = f"{bucket}/{symbol}"  # symbol is already "TS-X" format
    else:
        # For legacy symbol names, we need to find the TS-ID
        # Use the TS-ID from the found file to create unique cache key
        found_ts_id = None
        if csv_file:
            match = pattern.match(csv_file)
            if match:
                found_ts_id = match.group(1)
        cache_key = f"{bucket}/TS-{found_ts_id}" if found_ts_id else f"{bucket}/{symbol}"
    
    # Check if we need to refresh this specific bucket/symbol
    csv_path = os.path.join(bucket_path, csv_file)
    
    cache_key_lock = get_cache_key_lock(cache_key)
    with cache_key_lock:  # Lock only this specific cache entry
        if cache_key not in DATA_CACHE or cache_key not in LAST_MODIFIED:
            # Load for the first time
            df = load_csv_data_from_path(csv_path)
            if df.empty:
                return jsonify({'error': 'Failed to load data'}), 500
            DATA_CACHE[cache_key] = df
            LAST_MODIFIED[cache_key] = os.path.getmtime(csv_path)
            data_version += 1
            last_data_update = datetime.utcnow()
        else:
            # Check if file has been modified
            current_mtime = os.path.getmtime(csv_path)
            if current_mtime > LAST_MODIFIED[cache_key]:
                df = load_csv_data_from_path(csv_path)
                if not df.empty:
                    DATA_CACHE[cache_key] = df
                    LAST_MODIFIED[cache_key] = current_mtime
                    data_version += 1
                    last_data_update = datetime.utcnow()
    
    # Get data from cache
    df = DATA_CACHE[cache_key]
    
    # Get resolution parameter
    resolution = request.args.get('resolution', 'hourly')
    
    # Calculate metrics
    metrics = calculate_metrics(df)
    
    # Get resampled data for display based on resolution
    if resolution == 'minutely':
        df_display = df  # No resampling, use original minutely data
    else:  # hourly (default)
        df_display = get_resampled_data(df, resample_period='1h')
    
    # Normalize column names for response (handle both upper and lowercase)
    timestamp_col = 'timestamp' if 'timestamp' in df_display.columns else 'datetime'
    close_col = 'close' if 'close' in df_display.columns else 'Close'
    position_col = 'position' if 'position' in df_display.columns else 'Position'
    
    # Prepare response
    response = {
        'timestamps': df_display[timestamp_col].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        'prices': df_display[close_col].tolist(),
        'positions': df_display[position_col].tolist() if position_col in df_display.columns else [],
        'cumulative_returns': (df_display['cumulative_return'] * 100).tolist() if 'cumulative_return' in df_display.columns else [],
        'cumulative_returns_after_fees': (df_display['cumulative_return_after_fees'] * 100).tolist() if 'cumulative_return_after_fees' in df_display.columns else [],
        'metrics': metrics,
        'bucket': bucket,
        'symbol': symbol,
        'data_source': 'csv_strategy',  # New field
        'is_real_time': False  # New field
    }
    
    return jsonify(response)


@app.route('/api/data/<bucket>/<symbol>/since/<last_timestamp>')
def get_bucket_data_since(bucket, symbol, last_timestamp):
    """Get incremental data for a specific symbol from a specific bucket."""
    # Use same cache key logic as get_bucket_data to ensure consistency
    pattern = re.compile(r'STGC2OGTrim2Model_TS-(\d+)_T-(\d+)_.*\.csv')
    is_ts_id = symbol.startswith('TS-') or symbol.isdigit()
    
    if is_ts_id:
        # Normalize to always use TS-X format for cache key
        if symbol.isdigit():
            cache_key = f"{bucket}/TS-{symbol}"
        else:
            cache_key = f"{bucket}/{symbol}"  # symbol is already "TS-X" format
    else:
        # For legacy symbol names, find the TS-ID to construct proper cache key
        bucket_path = os.path.join(LOCAL_ROOT, bucket)
        found_ts_id = None
        
        if os.path.exists(bucket_path):
            for filename in os.listdir(bucket_path):
                if filename.endswith('.csv') and not filename.endswith('_PH.csv'):
                    match = pattern.match(filename)
                    if match:
                        ts_id = match.group(1)
                        # Get symbol from JSON metadata to match
                        metadata_file = os.path.join(bucket_path, f'TS-{ts_id}.json')
                        if os.path.exists(metadata_file):
                            try:
                                with open(metadata_file, 'r') as f:
                                    metadata = json.load(f)
                                    models = metadata.get('models', [])
                                    hedge_symbol = None
                                    if models and len(models) > 0:
                                        model_args = models[0].get('args', {})
                                        hedge_symbol = model_args.get('hedge_symbol', '')
                                    if not hedge_symbol:
                                        hedge_symbol = metadata.get('hedge_symbol', '')
                                    if hedge_symbol:
                                        parts = hedge_symbol.split('-')
                                        symbol_name = parts[0] if len(parts) >= 2 else hedge_symbol
                                        if symbol_name == symbol:
                                            found_ts_id = ts_id
                                            break
                            except:
                                continue
        
        cache_key = f"{bucket}/TS-{found_ts_id}" if found_ts_id else f"{bucket}/{symbol}"
    
    if cache_key not in DATA_CACHE:
        return jsonify({'error': f'Symbol {symbol} not found in bucket {bucket}'}), 404
    
    df = DATA_CACHE[cache_key]
    
    # Filter data since last timestamp
    try:
        last_dt = pd.to_datetime(last_timestamp)
        new_data = df[df['timestamp'] > last_dt]
        
        if len(new_data) == 0:
            return jsonify({'new_data': False})
        
        # Return incremental data
        response = {
            'new_data': True,
            'timestamps': new_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            'prices': new_data['close'].tolist(),
            'positions': new_data['position'].tolist() if 'position' in new_data.columns else [],
            'cumulative_returns': (new_data['cumulative_return'] * 100).tolist() if 'cumulative_return' in new_data.columns else [],
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting incremental data: {e}")
        return jsonify({'error': 'Failed to get incremental data'}), 500


@app.route('/api/symbols/summary')
def get_symbols_summary():
    """Get comprehensive summary data for all symbols across buckets."""
    global data_version, last_data_update
    summary_data = []
    
    # Load config to get buckets
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            buckets = config.get('buckets', [])
    except:
        buckets = []
    
    # Process each bucket in parallel
    def process_bucket_summary(bucket):
        bucket_name = bucket.replace('gs://', '').replace('/', '_').rstrip('_')
        bucket_path = os.path.join(LOCAL_ROOT, bucket_name)
        bucket_summary = []
        
        if os.path.exists(bucket_path):
            symbols_info = discover_symbols_in_bucket(bucket_path)
            
            # Process each symbol in parallel within the bucket
            def process_symbol(sym_info):
                try:
                    symbol_name = sym_info['symbol']
                    ts_id = sym_info['ts_id']
                    cache_key = f"{bucket_name}/TS-{ts_id}"
                    
                    # Load data (from cache if available)
                    cache_key_lock = get_cache_key_lock(cache_key)
                    with cache_key_lock:
                        if cache_key in DATA_CACHE:
                            df = DATA_CACHE[cache_key]
                        else:
                            csv_path = os.path.join(bucket_path, sym_info['filename'])
                            df = load_csv_data_from_path(csv_path)
                            if not df.empty:
                                DATA_CACHE[cache_key] = df
                                LAST_MODIFIED[cache_key] = os.path.getmtime(csv_path)
                    
                    if not df.empty and 'close' in df.columns:
                        # Calculate metrics
                        last_price = df['close'].iloc[-1]
                        
                        # 24h change
                        if len(df) > 1440:  # 24h of 1min data
                            price_24h_ago = df['close'].iloc[-1440]
                            change_24h = ((last_price - price_24h_ago) / price_24h_ago) * 100
                        else:
                            change_24h = 0
                        
                        # 7-day change
                        if len(df) > 10080:  # 7 days of 1min data (1440 * 7)
                            price_7d_ago = df['close'].iloc[-10080]
                            change_7d = ((last_price - price_7d_ago) / price_7d_ago) * 100
                        else:
                            # If not enough data, use the earliest available
                            price_7d_ago = df['close'].iloc[0]
                            change_7d = ((last_price - price_7d_ago) / price_7d_ago) * 100
                        
                        # Position
                        current_position = df['position'].iloc[-1] if 'position' in df.columns else 0
                        position_text = 'LONG' if current_position > 0 else 'SHORT' if current_position < 0 else 'FLAT'
                        
                        # Cumulative return (NET - after fees)
                        cumulative_return = df['cumulative_return_after_fees'].iloc[-1] * 100 if 'cumulative_return_after_fees' in df.columns else 0
                        max_return = df['cumulative_return_after_fees'].max() * 100 if 'cumulative_return_after_fees' in df.columns else 0
                        
                        # Calculate consecutive positive days
                        consecutive_days = 0
                        if 'close' in df.columns and 'timestamp' in df.columns and len(df) > 0:
                            # Group by date and calculate daily returns
                            df['date'] = df['timestamp'].dt.date
                            daily_returns = df.groupby('date')['close'].agg(['first', 'last'])
                            daily_returns['daily_return'] = (daily_returns['last'] - daily_returns['first']) / daily_returns['first']
                            
                            # Count consecutive positive days from most recent
                            for i in range(len(daily_returns) - 1, -1, -1):
                                if daily_returns.iloc[i]['daily_return'] > 0:
                                    consecutive_days += 1
                                else:
                                    break
                        
                        # Default to CSV strategy
                        data_source = 'csv_strategy'
                        
                        return {
                            'bucket': bucket_name.replace('_', ' ').title(),
                            'bucket_raw': bucket_name,
                            'symbol': symbol_name,
                            'symbol_pair': f"{symbol_name} ({sym_info['pair']})",
                            'trading_pair': sym_info['pair'],
                            'pair': sym_info['pair'],
                            'ts_id': ts_id,
                            'freshness': sym_info['freshness'],
                            'last_update': sym_info['last_update'],
                            'minutes_ago': sym_info['minutes_ago'],
                            'last_price': round(last_price, 2),
                            'position': position_text,
                            'position_value': int(current_position),
                            'cumulative_return': round(cumulative_return, 2),
                            'max_return': round(max_return, 2),
                            'change_24h': round(change_24h, 2),
                            'change_7d': round(change_7d, 2),
                            'consecutive_positive_days': consecutive_days,
                            'data_source': data_source
                        }
                except Exception as e:
                    logger.error(f"Error processing symbol {sym_info.get('symbol', 'unknown')}: {e}")
                    return None
            
            # Process symbols within bucket in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as symbol_pool:
                futures = [symbol_pool.submit(process_symbol, sym_info) for sym_info in symbols_info]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        bucket_summary.append(result)
        
        return bucket_summary
    
    # Process all buckets in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_bucket_summary, bucket) for bucket in buckets]
        
        for future in as_completed(futures):
            bucket_results = future.result()
            summary_data.extend(bucket_results)
    
    # Calculate aggregate statistics
    if summary_data:
        returns = [s['cumulative_return'] for s in summary_data]
        avg_return = sum(returns) / len(returns)
        min_return = min(returns)
        max_return = max(returns)
        fresh_count = sum(1 for s in summary_data if s['freshness'] == 'fresh')
        total_count = len(summary_data)
        
        # Calculate median
        median_return = statistics.median(returns)
        
        # Count positive and negative returns (cumulative)
        positive_cumulative = sum(1 for r in returns if r > 0)
        negative_cumulative = sum(1 for r in returns if r < 0)
        
        # Count positive and negative 24h changes
        changes_24h = [s['change_24h'] for s in summary_data]
        positive_24h = sum(1 for c in changes_24h if c > 0)
        negative_24h = sum(1 for c in changes_24h if c < 0)
    else:
        avg_return = 0
        min_return = 0
        max_return = 0
        fresh_count = 0
        total_count = 0
        median_return = 0
        positive_cumulative = 0
        negative_cumulative = 0
        positive_24h = 0
        negative_24h = 0
    
    # Calculate freshness percentage
    freshness_percent = round((fresh_count / total_count * 100) if total_count > 0 else 0)
    
    # Calculate per-coin statistics
    coin_stats = {}
    for coin in ['BTC', 'ETH', 'SOL', 'LTC']:
        coin_data = [s for s in summary_data if s['symbol'] == coin]
        if coin_data:
            coin_returns = [s['cumulative_return'] for s in coin_data]
            coin_changes_24h = [s['change_24h'] for s in coin_data]
            
            coin_stats[coin] = {
                'total_symbols': len(coin_data),
                'fresh_symbols': sum(1 for s in coin_data if s['freshness'] == 'fresh'),
                'freshness_percent': round((sum(1 for s in coin_data if s['freshness'] == 'fresh') / len(coin_data) * 100)),
                'avg_return': round(sum(coin_returns) / len(coin_returns), 2),
                'min_return': round(min(coin_returns), 2),
                'max_return': round(max(coin_returns), 2),
                'median_return': round(statistics.median(coin_returns), 2),
                'positive_cumulative': sum(1 for r in coin_returns if r > 0),
                'negative_cumulative': sum(1 for r in coin_returns if r < 0),
                'positive_24h': sum(1 for c in coin_changes_24h if c > 0),
                'negative_24h': sum(1 for c in coin_changes_24h if c < 0),
                'active_positions': sum(1 for s in coin_data if s['position'] != 'FLAT')
            }
    
    return jsonify({
        'symbols': summary_data,
        'stats': {
            'total_symbols': total_count,
            'fresh_symbols': fresh_count,
            'freshness_percent': freshness_percent,
            'avg_return': round(avg_return, 2),
            'min_return': round(min_return, 2),
            'max_return': round(max_return, 2),
            'median_return': round(median_return, 2),
            'positive_cumulative': positive_cumulative,
            'negative_cumulative': negative_cumulative,
            'positive_24h': positive_24h,
            'negative_24h': negative_24h,
            'total_buckets': len(set(s['bucket_raw'] for s in summary_data)) if summary_data else 0
        },
        'coin_stats': coin_stats
    })


@app.route('/api/cumulative_returns/all')
def get_all_cumulative_returns():
    """Get cumulative returns data for all symbols for aggregate chart."""
    try:
        all_returns_data = []
        
        # Load config to get buckets
        with open('config.json', 'r') as f:
            config = json.load(f)
            buckets = config.get('buckets', [])
        
        # Color mapping for different coins
        color_map = {
            'BTC': '#f7931a',  # Bitcoin orange
            'ETH': '#627eea',  # Ethereum blue
            'SOL': '#00d18c',  # Solana green
            'LTC': '#bebebe',  # Litecoin grey
            'XRP': '#23292f',  # XRP dark
            'ADA': '#0033ad',  # Cardano blue
            'DOT': '#e6007a',  # Polkadot pink
            'AVAX': '#e84142', # Avalanche red
            'MATIC': '#8247e5', # Polygon purple
            'LINK': '#2a5ada',  # Chainlink blue
            'UNI': '#ff007a',   # Uniswap pink
            'DEFAULT': '#999999' # Default grey
        }
        
        # Function to process each symbol
        def process_symbol_returns(bucket_name, sym_info):
            try:
                cache_key = f"{bucket_name}_TS-{sym_info['ts_id']}"
                
                # Get or create lock for this cache key
                lock = get_cache_key_lock(cache_key)
                
                # Try to get from cache first without loading
                with lock:
                    if cache_key in DATA_CACHE:
                        df = DATA_CACHE[cache_key]
                        # Don't copy the entire dataframe, just work with references
                    else:
                        csv_path = os.path.join(LOCAL_ROOT, bucket_name, sym_info['filename'])
                        df = load_csv_data_from_path(csv_path)
                        if df.empty:
                            return None
                        # Cache the data for future use
                        DATA_CACHE[cache_key] = df
                
                # Get cumulative returns data (NET - after fees)
                if 'cumulative_return_after_fees' not in df.columns:
                    return None
                
                # Find timestamp column
                timestamp_col = None
                for col in ['datetime', 'timestamp', 'Close time', 'time', 'date']:
                    if col in df.columns:
                        timestamp_col = col
                        break
                
                if not timestamp_col:
                    return None
                
                # Prepare data - sample if too many points
                max_points = 500  # Limit points per symbol for performance
                if len(df) > max_points:
                    # Sample evenly across the dataset
                    step = len(df) // max_points
                    df_sampled = df.iloc[::step].copy()
                else:
                    df_sampled = df.copy()
                
                # Convert timestamps
                df_sampled[timestamp_col] = pd.to_datetime(df_sampled[timestamp_col])
                
                # Get symbol color
                symbol_name = sym_info['symbol']
                color = color_map.get(symbol_name, color_map['DEFAULT'])
                
                # Clean up NaN values and convert to list (using NET returns)
                y_values = (df_sampled['cumulative_return_after_fees'] * 100).round(2)
                # Replace NaN with 0 or previous valid value
                y_values = y_values.fillna(0).tolist()
                
                return {
                    'symbol': symbol_name,
                    'pair': sym_info['pair'],
                    'bucket': bucket_name,
                    'ts_id': sym_info['ts_id'],
                    'color': color,
                    'data': {
                        'x': df_sampled[timestamp_col].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                        'y': y_values
                    }
                }
                
            except Exception as e:
                logger.error(f"Error processing returns for {sym_info.get('symbol', 'unknown')}: {e}")
                return None
        
        # Process all symbols in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for bucket in buckets:
                bucket_name = bucket.replace('gs://', '').replace('/', '_').rstrip('_')
                bucket_path = os.path.join(LOCAL_ROOT, bucket_name)
                
                if os.path.exists(bucket_path):
                    symbols_info = discover_symbols_in_bucket(bucket_path)
                    
                    for sym_info in symbols_info:
                        future = executor.submit(process_symbol_returns, bucket_name, sym_info)
                        futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_returns_data.append(result)
        
        # Sort by symbol name for consistent ordering
        all_returns_data.sort(key=lambda x: (x['symbol'], x['pair']))
        
        return jsonify({
            'symbols': all_returns_data,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting all cumulative returns: {e}")
        return jsonify({'error': 'Failed to get cumulative returns data'}), 500






# Portfolio demo - OKX and REB routes removed


def initialize_app():
    """Initialize the application."""
    logger.info("Initializing portfolio demo application...")

    # Initial data load
    refresh_data_cache()
    logger.info("Portfolio demo ready - using static data")


if __name__ == '__main__':
    initialize_app()
    app.run(debug=False, port=5001, host='0.0.0.0')