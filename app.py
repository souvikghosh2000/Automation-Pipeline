import streamlit as st
import snowflake.connector
import pandas as pd
import dataikuapi
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

# ======================
# Page Config
# ======================
st.set_page_config(
    page_title="Client Information & Tables",
    page_icon=":bar_chart:",
    layout="wide"
)

# ======================
# Helper Functions for Timing
# ======================
def format_duration(seconds):
    """Format duration in seconds to readable format"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"

def display_timing_summary(timing_data):
    """Display timing summary in a nice format"""
    st.subheader("⏱️ Processing Time Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Total Time", 
            format_duration(timing_data.get('total_time', 0)),
            help="Total time for all operations"
        )
    
    with col2:
        st.metric(
            "Connection Time", 
            format_duration(timing_data.get('connection_time', 0)),
            help="Time to connect to Snowflake"
        )
    
    with col3:
        st.metric(
            "API Processing Time", 
            format_duration(timing_data.get('api_time', 0)),
            help="Time for API calls"
        )
    
    if timing_data.get('table_times'):
        st.write("**Table Processing Times:**")
        for table_name, table_time in timing_data['table_times'].items():
            st.write(f"• {table_name}: {format_duration(table_time)}")

# ======================
# Async Processing Functions
# ======================
def process_equipment_async(df, table_name):
    """Process equipment table asynchronously"""
    eq_client = dataikuapi.APINodeClient(
        "https://api-81259c59-85f60f3c-dku.us-east-1.app.dataiku.io",
        "LLM_Endpoint",
        api_key="7mHQ9XU5jHZiUoTqSiCg2yXNCAilCOeA"
    )
    
    results = []
    for i, row in df.iterrows():
        try:
            result = eq_client.run_function(
                "v1",
                query={
                    "EQUIPMENT_NAME": str(row["EQUIPMENT_NAME"]),
                    "EQUIPMENT_DESC_PRO": str(row["EQUIPMENT_DESC_PRO"])
                }
            )
            results.append(result.get("response"))
        except Exception as e:
            results.append(f"Error: {e}")
    
    df["api_result"] = results
    return df, table_name, "equipment"

def process_workorder_async(df, table_name):
    """Process work order table asynchronously"""
    api_client = dataikuapi.APINodeClient(
        "https://api-81259c59-85f60f3c-dku.us-east-1.app.dataiku.io", 
        "Transformers_Endpoint"
    )
    
    results = []
    for i, row in df.iterrows():
        try:
            result = api_client.run_function(
                "Transformers",
                workorder_description=str(row["WORKORDER_DESC"]),
                completion_note=str(row["COMPLETION_NOTES"])
            )
            results.append(result.get("response"))
        except Exception as e:
            results.append(f"Error: {e}")
            
    df["api_result"] = results
    return df, table_name, "workorder"

# ======================
# Data Processing Functions
# ======================
def find_workorder_and_equipment_tables(tables_data):
    """Find workorder and equipment tables from the loaded data"""
    workorder_table = None
    equipment_table = None
    workorder_name = None
    equipment_name = None
    
    for table_name, df in tables_data.items():
        if "work" in table_name.lower():
            workorder_table = df
            workorder_name = table_name
        elif "equipment" in table_name.lower():
            equipment_table = df
            equipment_name = table_name
    
    return workorder_table, equipment_table, workorder_name, equipment_name

def perform_data_join(workorder_df, equipment_df, workorder_name, equipment_name):
    """Perform left join on PROPERTY_SKEY and keep specified columns"""
    
    # Check if PROPERTY_SKEY exists in both datasets
    if 'PROPERTY_SKEY' not in workorder_df.columns:
        return None, "PROPERTY_SKEY column not found in workorder dataset"
    
    if 'PROPERTY_SKEY' not in equipment_df.columns:
        return None, "PROPERTY_SKEY column not found in equipment dataset"
    
    # Check if required columns exist
    workorder_required = ['WORKORDER_DESC', 'COMPLETION_NOTES']
    equipment_required = ['api_result']
    
    missing_workorder = [col for col in workorder_required if col not in workorder_df.columns]
    missing_equipment = [col for col in equipment_required if col not in equipment_df.columns]
    
    if missing_workorder:
        return None, f"Missing columns in workorder dataset: {missing_workorder}"
    
    if missing_equipment:
        return None, f"Missing columns in equipment dataset: {missing_equipment}"
    
    # Prepare datasets for join
    workorder_subset = workorder_df[['PROPERTY_SKEY', 'WORKORDER_DESC', 'COMPLETION_NOTES', 'api_result']].copy()
    equipment_subset = equipment_df[['PROPERTY_SKEY', 'api_result']].copy()
    
    # Rename api_result columns to avoid conflicts
    workorder_subset = workorder_subset.rename(columns={'api_result': 'workorder_api_result'})
    equipment_subset = equipment_subset.rename(columns={'api_result': 'equipment_api_result'})
    
    # Perform left join
    joined_df = pd.merge(
        workorder_subset,
        equipment_subset,
        on='PROPERTY_SKEY',
        how='left'
    )
    
    return joined_df, None

def perform_grouping_and_aggregation(joined_df):
    """
    Group by WORKORDER_DESC, take first workorder_api_result, and concatenate equipment_api_result
    Uses tuples to remove duplicates and checks if workorder values exist in equipment results
    """
    if joined_df is None or len(joined_df) == 0:
        return None, "No data to group"
    
    # Check if required columns exist
    required_columns = ['WORKORDER_DESC', 'workorder_api_result', 'equipment_api_result']
    missing_columns = [col for col in required_columns if col not in joined_df.columns]
    
    if missing_columns:
        return None, f"Missing required columns for grouping: {missing_columns}"
    
    # Perform grouping and aggregation
    try:
        import json
        import ast
        
        def process_equipment_results(x):
            """Process equipment results using tuple to remove duplicates and return as tuple string"""
            # Get non-null values
            valid_results = x.dropna().astype(str)
            # Convert to tuple to remove duplicates
            unique_results = tuple(valid_results)
            # Return tuple as string representation
            return str(unique_results)
        
        def extract_values_from_json_string(json_str):
            """Extract values from JSON string, handling both dict and string formats"""
            if pd.isna(json_str) or json_str == '' or json_str == 'nan':
                return []
            
            try:
                # Try to parse as JSON
                if isinstance(json_str, str):
                    # Handle different JSON string formats
                    if json_str.startswith('{') or json_str.startswith('['):
                        data = json.loads(json_str)
                    else:
                        # Try ast.literal_eval for Python-like strings
                        try:
                            data = ast.literal_eval(json_str)
                        except:
                            # If all else fails, treat as plain string
                            return [json_str.strip()]
                else:
                    data = json_str
                
                # Extract values from the parsed data
                values = []
                if isinstance(data, dict):
                    values = list(data.values())
                elif isinstance(data, list):
                    values = data
                else:
                    values = [str(data)]
                
                # Convert all values to strings and clean them
                return [str(v).strip() for v in values if v is not None and str(v).strip() != '']
                
            except Exception as e:
                # If parsing fails, treat as plain string
                return [str(json_str).strip()] if str(json_str).strip() != '' else []
        
        # Perform basic grouping first
        grouped_df = joined_df.groupby('WORKORDER_DESC').agg({
            'workorder_api_result': 'first',  # Take the first workorder_api_result
            'equipment_api_result': process_equipment_results  # Use tuple to remove duplicates
        }).reset_index()
        
        # Rename concatenated column for clarity
        grouped_df = grouped_df.rename(columns={'equipment_api_result': 'concatenated_equipment_results'})
        
        # Process workorder_api_result values and check existence in equipment results
        validation_results = []
        
        for idx, row in grouped_df.iterrows():
            # Extract values from workorder_api_result
            workorder_values = extract_values_from_json_string(row['workorder_api_result'])
            
            # Check if any workorder values exist in equipment results
            equipment_results_str = str(row['concatenated_equipment_results']).lower()
            
            # Check each workorder value against equipment results
            found_any = False
            for value in workorder_values:
                if str(value).lower() in equipment_results_str:
                    found_any = True
                    break
            
            # Set validation result
            validation_results.append("Matched" if found_any else "Not Matched")
        
        # Add validation column
        grouped_df['Validation'] = validation_results
        
        return grouped_df, None
        
    except Exception as e:
        return None, f"Error during grouping: {str(e)}"

# ======================
# Sidebar
# ======================
st.sidebar.header(":mag: Client Selector")
client_name = st.sidebar.text_input("Enter Client Name")
st.sidebar.markdown("---")
st.sidebar.info(":bulb: Tip: Enter schema name exactly as in Snowflake.")

# Snowflake connection info
sf_conn_info = {
    "user": "SFTRIAL",
    "password": "Sf_trial123456",
    "account": "ODBFDTH-AB84413",
    "warehouse": "COMPUTE_WH",
    "database": "POC",
    "role": "ACCOUNTADMIN"
}

# ======================
# Main Header
# ======================
st.title(":bar_chart: Client Information & Table Processing")
st.caption("This tool connects to Snowflake, loads tables into memory, and processes them using Dataiku APIs with concurrent processing.")

# Initialize timing data
if 'timing_data' not in st.session_state:
    st.session_state.timing_data = {
        'total_time': 0,
        'connection_time': 0,
        'api_time': 0,
        'table_times': {},
        'start_time': None
    }

if client_name:
    # Start overall timer
    overall_start_time = time.time()
    st.session_state.timing_data['start_time'] = overall_start_time
    
    st.markdown(f"### Client: **`{client_name}`**")
    
    # Real-time timer display
    timer_placeholder = st.empty()
    
    # Step 1: Fetch tables
    if 'tables_data' not in st.session_state:
        st.session_state.tables_data = {}
    
    with st.spinner(f":arrows_counterclockwise: Fetching tables for `{client_name}`..."):
        connection_start_time = time.time()
        
        try:
            conn = snowflake.connector.connect(**sf_conn_info)
            cur = conn.cursor()
            cur.execute(f'USE SCHEMA {sf_conn_info["database"]}."{client_name}"')
            cur.execute(f'SHOW TABLES IN SCHEMA {sf_conn_info["database"]}."{client_name}"')
            tables = cur.fetchall()
            
            connection_end_time = time.time()
            st.session_state.timing_data['connection_time'] = connection_end_time - connection_start_time
            
        except Exception as e:
            st.error(f":x: Error connecting to Snowflake: {str(e)}")
            tables = None
        finally:
            if 'conn' in locals():
                conn.close()
    
    if tables:
        st.success(f":white_check_mark: Found **{len(tables)}** tables in `{client_name}` schema.")
        st.session_state.tables_data[client_name] = {}
        
        # Separate tables by type
        equipment_tables = []
        workorder_tables = []
        other_tables = []
        
        for table in tables:
            table_name = table[1]
            if "equipment" in table_name.lower():
                equipment_tables.append(table)
            elif "work" in table_name.lower():
                workorder_tables.append(table)
            else:
                other_tables.append(table)
        
        # Load all table data first
        st.markdown("### 📊 **Loading Table Data**")
        loading_progress = st.progress(0)
        loading_status = st.empty()
        
        all_dataframes = {}
        for idx, table in enumerate(tables):
            table_name = table[1]
            loading_status.text(f"Loading {table_name}... ({idx + 1}/{len(tables)})")
            
            conn = snowflake.connector.connect(**sf_conn_info)
            df = pd.read_sql(
                f'SELECT * FROM {sf_conn_info["database"]}."{client_name}"."{table_name}"',
                conn
            )
            conn.close()
            
            all_dataframes[table_name] = df
            st.session_state.tables_data[client_name][table_name] = df
            loading_progress.progress((idx + 1) / len(tables))
        
        loading_status.text("✅ All tables loaded successfully!")
        
        # Create progress containers for concurrent processing
        if equipment_tables or workorder_tables:
            st.markdown("### 🚀 **Concurrent API Processing**")            
            st.markdown("---")
            
            # Progress tracking containers
            progress_cols = st.columns(2)
            
            eq_progress_bars = {}
            eq_status_texts = {}
            wo_progress_bars = {}
            wo_status_texts = {}
            
            if equipment_tables:
                with progress_cols[0]:
                    st.write("⚙️ **Equipment Processing Status**")
                    for table in equipment_tables:
                        table_name = table[1]
                        st.write(f"**{table_name}**")
                        eq_progress_bars[table_name] = st.progress(0)
                        eq_status_texts[table_name] = st.empty()
            
            if workorder_tables:
                with progress_cols[1]:
                    st.write("🛠️ **Work Order Processing Status**")
                    for table in workorder_tables:
                        table_name = table[1]
                        st.write(f"**{table_name}**")
                        wo_progress_bars[table_name] = st.progress(0)
                        wo_status_texts[table_name] = st.empty()
        
        # Start concurrent processing
        futures = []
        processing_start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit equipment processing tasks
            for table in equipment_tables:
                table_name = table[1]
                df = all_dataframes[table_name]
                
                if {"EQUIPMENT_NAME", "EQUIPMENT_DESC_PRO"}.issubset(df.columns):
                    future = executor.submit(process_equipment_async, df.copy(), table_name)
                    futures.append(future)
                    eq_status_texts[table_name].text(f"⏳ Starting processing ({len(df)} rows)...")
                else:
                    eq_status_texts[table_name].text("⚠️ Missing required columns: EQUIPMENT_NAME, EQUIPMENT_DESC_PRO")
            
            # Submit work order processing tasks
            for table in workorder_tables:
                table_name = table[1]
                df = all_dataframes[table_name]
                
                if {"WORKORDER_DESC", "COMPLETION_NOTES"}.issubset(df.columns):
                    future = executor.submit(process_workorder_async, df.copy(), table_name)
                    futures.append(future)
                    wo_status_texts[table_name].text(f"⏳ Starting processing ({len(df)} rows)...")
                else:
                    wo_status_texts[table_name].text("⚠️ Missing required columns: WORKORDER_DESC, COMPLETION_NOTES")
            
            # Monitor progress and collect results
            if futures:
                completed_count = 0
                total_futures = len(futures)
                
                # Show overall progress
                st.markdown("#### 📊 **Overall Progress**")
                overall_progress = st.progress(0)
                overall_status = st.empty()
                overall_status.text(f"Processing {total_futures} tables concurrently...")
                
                # Process completed futures as they finish
                for future in as_completed(futures):
                    try:
                        processed_df, table_name, table_type = future.result()
                        
                        # Update session state with processed data
                        st.session_state.tables_data[client_name][table_name] = processed_df
                        
                        # Update progress
                        completed_count += 1
                        overall_progress.progress(completed_count / total_futures)
                        
                        # Update specific progress bars
                        if table_type == "equipment":
                            eq_progress_bars[table_name].progress(1.0)
                            eq_status_texts[table_name].text(f"✅ Completed! ({len(processed_df)} rows processed)")
                        elif table_type == "workorder":
                            wo_progress_bars[table_name].progress(1.0)
                            wo_status_texts[table_name].text(f"✅ Completed! ({len(processed_df)} rows processed)")
                        
                        # Update overall status
                        elapsed = time.time() - processing_start_time
                        overall_status.text(f"Completed {completed_count}/{total_futures} tables in {format_duration(elapsed)}")
                        
                    except Exception as e:
                        st.error(f"❌ Error processing table: {str(e)}")
                        completed_count += 1
                        overall_progress.progress(completed_count / total_futures)
        
        # Calculate total processing time
        processing_end_time = time.time()
        total_api_time = processing_end_time - processing_start_time
        
        if futures:
            st.success(f"🎉 **All concurrent processing completed in {format_duration(total_api_time)}!**")
        
        # Create tab names - individual tables plus joined data tab plus grouped data tab
        table_names = [table[1] for table in tables]
        tab_names = table_names + ["🔗 Joined Data", "📊 Grouped Analysis"]
        tabs = st.tabs(tab_names)
        
        # Display processed data in tabs
        for idx, table in enumerate(tables):
            table_name = table[1]
            
            with tabs[idx]:
                # Update timer in real-time
                current_time = time.time()
                elapsed_time = current_time - overall_start_time
                timer_placeholder.info(f"⏱️ **Elapsed Time:** {format_duration(elapsed_time)}")
                
                df = st.session_state.tables_data[client_name][table_name]
                
                if "equipment" in table_name.lower():
                    st.info(f"Asset category is classified using the LLM Endpoint on the Equipment_data ")
                    if 'api_result' in df.columns:
                        st.success("✅ API processing completed!")
                        # Show sample API results
                        non_empty_results = df[df['api_result'].notna() & (df['api_result'] != '')]['api_result']
                        
                    else:
                        st.warning("⚠️ API processing was not completed")
                    
                elif "work" in table_name.lower():
                    st.info(f"🛠 Asset category is predicted along with the Confidence Scores using the Transformer Endpoint on the Work_Order_data")
                    if 'api_result' in df.columns:
                        st.success("✅ API processing completed!")
                        # Show sample API results
                        non_empty_results = df[df['api_result'].notna() & (df['api_result'] != '')]['api_result']
                       
                        st.warning("⚠️ API processing was not completed")
                
                else:
                    st.info(f"📋 **Table**: `{table_name}` ({len(df)} rows)")
                
                st.dataframe(df, use_container_width=True)
        
        # Joined Data Tab
        joined_df = None
        with tabs[-2]:  # Second to last tab is the joined data tab
            st.info("🔗 **Joined Dataset**: Workorder ⟵ Equipment (Left Join on PROPERTY_SKEY)")
            
            if client_name in st.session_state.tables_data:
                workorder_df, equipment_df, workorder_name, equipment_name = find_workorder_and_equipment_tables(
                    st.session_state.tables_data[client_name]
                )
                
                if workorder_df is not None and equipment_df is not None:
                    st.write(f"📊 **Source Tables:**")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"• **Workorder Table**: `{workorder_name}` ({len(workorder_df)} rows)")
                    with col2:
                        st.write(f"• **Equipment Table**: `{equipment_name}` ({len(equipment_df)} rows)")
                    
                    st.markdown("---")
                    
                    joined_df, error = perform_data_join(workorder_df, equipment_df, workorder_name, equipment_name)
                    
                    if error:
                        st.error(f"❌ **Join Error**: {error}")
                    else:
                        st.success(f"✅ **Join Successful**")
                        
                        # Store joined data in session state for the grouped analysis
                        st.session_state['joined_data'] = joined_df
                        
                    
                        
                        st.markdown("---")
                        
                        # Display the joined dataframe
                        st.dataframe(joined_df, use_container_width=True)
                        
                        # # Download option
                        # csv = joined_df.to_csv(index=False)
                        # st.download_button(
                        #     label="📥 Download Joined Data as CSV",
                        #     data=csv,
                        #     file_name=f"{client_name}_joined_data.csv",
                        #     mime="text/csv"
                        # )
                        
                elif workorder_df is None and equipment_df is None:
                    st.warning("⚠️ **No workorder or equipment tables found**")
                    st.write("Available tables:")
                    for table_name in st.session_state.tables_data[client_name].keys():
                        st.write(f"• {table_name}")
                elif workorder_df is None:
                    st.warning("⚠️ **No workorder table found** (looking for tables with 'work' in the name)")
                else:
                    st.warning("⚠️ **No equipment table found** (looking for tables with 'equipment' in the name)")
            else:
                st.info("ℹ️ Please process the individual tables first to enable data joining.")
        
        # Grouped Analysis Tab
        with tabs[-1]:  # Last tab is the grouped analysis tab
            st.info("📊 **Grouped Analysis**: Group by WORKORDER_DESC with aggregated results")
            
            if 'joined_data' in st.session_state and st.session_state['joined_data'] is not None:
                joined_data = st.session_state['joined_data']
                
                # Perform grouping and aggregation
                grouped_df, error = perform_grouping_and_aggregation(joined_data)
                
                if error:
                    st.error(f"❌ **Grouping Error**: {error}")
                else:
                    st.success(f"✅ **Successful**")
                    
                  
                    
                    st.markdown("---")
                    
                    # Display the grouped dataframe
                    st.dataframe(grouped_df, use_container_width=True)
                    
                    
                    
            else:
                st.info("ℹ️ Please complete the data joining process first to enable grouped analysis.")
                
        # Update timing data
        overall_end_time = time.time()
        st.session_state.timing_data['total_time'] = overall_end_time - overall_start_time
        st.session_state.timing_data['api_time'] = total_api_time
        
        # Display final timing summary
        st.markdown("---")
        # display_timing_summary(st.session_state.timing_data)
        
    else:
        st.warning(f"⚠ No tables found in schema `{client_name}`.")
        
else:
    st.info("ℹ️ Please enter a client name in the sidebar to start.")
    st.markdown("""
    
    ### 📋 **Instructions:**
    1. Enter your client name (schema name) in the sidebar
    2. The app will automatically detect and process Equipment and Work Order tables
    3. View results in individual tabs or combined analysis tabs
    4. Download processed data as needed
    """)